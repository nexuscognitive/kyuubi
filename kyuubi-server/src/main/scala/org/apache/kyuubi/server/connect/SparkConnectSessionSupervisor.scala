/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.server.connect

import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.{ApplicationState, KubernetesDriverPostMortem}
import org.apache.kyuubi.engine.ApplicationState.isTerminated
import org.apache.kyuubi.server.metadata.api.{SparkConnectDriverPostMortem, SparkConnectSessionInfo}
import org.apache.kyuubi.util.ThreadUtils

/**
 * Keeps a Spark Connect session's reported state honest about its driver, and brings the driver
 * back when it dies.
 *
 * Two things Kyuubi's own session record cannot tell anyone on its own. The first is whether the
 * session works: the record says `RUNNING` from the moment the engine reported in and goes on
 * saying it after the driver pod has been evicted, OOM-killed or deleted, because nothing about
 * the pod's death travels back to it. The second is what to do about that. Both are answered from
 * the driver itself, through the pod informer
 * [[org.apache.kyuubi.engine.KubernetesApplicationOperation]] already runs -- no second watch and
 * no second client.
 *
 * ==What recovery is and is not==
 *
 * A relaunched driver is a new JVM running a new `SparkSession`. Temporary views, cached frames,
 * registered artifacts and every session-level Spark conf set over the wire are gone with the
 * process that held them, and a client still holding the old Spark Connect session id is answered
 * `INVALID_HANDLE.SESSION_NOT_FOUND` by the new engine. Recovery restores a usable endpoint. It
 * does not, and cannot, restore state, and nothing here pretends otherwise: every path that
 * replaces an engine bumps [[SparkConnectSessionInfo.generation]], which is what the REST session
 * view, the relay's response header and the web UI all read to say that the session was replaced
 * and when.
 *
 * Kubernetes cannot restart the driver in place either, so a Kyuubi-level relaunch is the only
 * option there is: Spark's `BasicDriverFeatureStep` applies `restartPolicy: Never` to the driver
 * pod through `editOrNewSpec()`, which overwrites whatever a pod template asked for.
 *
 * ==Why lazy by default==
 *
 * Eager recovery -- relaunching the moment a death is observed -- keeps the endpoint warm for a
 * user who is waiting, and burns a driver for a user who went home at six. A driver is the most
 * expensive thing this gateway allocates, and a Spark Connect session is per user and long-lived,
 * so the second case is the common one. Lazy recovery relaunches on the next thing that proves
 * somebody wants the session: a `POST` to create one, or a Spark Connect call. The cost is that
 * the endpoint stays dead until touched, which the session view says plainly rather than hiding.
 * [[KyuubiConf.FRONTEND_SPARK_CONNECT_RECOVERY_EAGER_ENABLED]] switches to eager for deployments
 * that would rather pay for the driver.
 */
class SparkConnectSessionSupervisor(
    conf: KyuubiConf,
    registry: SparkConnectSessionRegistry,
    engineLocator: SparkConnectEngineLocator,
    driverObserver: SparkConnectDriverObserver,
    provisionEngine: SparkConnectEngineRequest => String)
  extends Logging {

  import SparkConnectSessionSupervisor._

  private val recoveryEnabled = conf.get(FRONTEND_SPARK_CONNECT_RECOVERY_ENABLED)
  private val eagerRecoveryEnabled = conf.get(FRONTEND_SPARK_CONNECT_RECOVERY_EAGER_ENABLED)
  private val eagerRecoveryInterval = conf.get(FRONTEND_SPARK_CONNECT_RECOVERY_EAGER_INTERVAL)
  private val maxRecoveryAttempts = conf.get(FRONTEND_SPARK_CONNECT_RECOVERY_MAX_ATTEMPTS)
  private val initialBackoff = conf.get(FRONTEND_SPARK_CONNECT_RECOVERY_BACKOFF_INITIAL)
  private val maxBackoff = conf.get(FRONTEND_SPARK_CONNECT_RECOVERY_BACKOFF_MAX)
  private val postMortemRetain = conf.get(FRONTEND_SPARK_CONNECT_POST_MORTEM_RETAIN)
  private val postMortemMaxEvents = conf.get(FRONTEND_SPARK_CONNECT_POST_MORTEM_MAX_EVENTS)

  /**
   * Users whose engine this instance is currently relaunching.
   *
   * The persisted `RECOVERING` state is what stops a *peer* starting a second driver; this stops
   * two threads of this instance doing it, which the store cannot, because a Spark Connect client
   * issues several calls in flight at once and they would all read the same pre-recovery row.
   */
  private val recoveriesInFlight = ConcurrentHashMap.newKeySet[String]()

  private var recoveryExecutor: ScheduledExecutorService = _

  private val started = new AtomicBoolean(false)

  /**
   * Begin watching for driver deaths.
   *
   * Idempotent, because both the REST frontend and the Spark Connect frontend may be enabled and
   * neither owns this.
   */
  def start(): Unit = {
    if (!started.compareAndSet(false, true)) {
      return
    }
    recoveryExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "spark-connect-recovery-thread")
    driverObserver.onDriverTerminated(recordDriverDeath)
    if (recoveryEnabled && eagerRecoveryEnabled) {
      info("Spark Connect engine recovery is eager: dead drivers are relaunched on detection")
      ThreadUtils.scheduleTolerableRunnableWithFixedDelay(
        recoveryExecutor,
        () => reconcileAll(),
        eagerRecoveryInterval,
        eagerRecoveryInterval,
        TimeUnit.MILLISECONDS)
    }
  }

  def stop(): Unit = {
    if (recoveryExecutor != null) {
      ThreadUtils.shutdown(recoveryExecutor)
      recoveryExecutor = null
    }
  }

  /**
   * Store the post-mortem of a driver that has just died, against the session that owned it.
   *
   * Called from the pod informer for every Spark engine driver, batch and Thrift included, so the
   * overwhelming majority of calls resolve to no Spark Connect binding and do nothing.
   */
  private[connect] def recordDriverDeath(postMortem: KubernetesDriverPostMortem): Unit = {
    try {
      val stored = SparkConnectDriverPostMortem.fromDriverPod(postMortem, postMortemMaxEvents)
      registry.recordDriverPostMortem(stored, postMortemRetain).foreach { binding =>
        warn(s"The Spark Connect driver of ${binding.userName} died: ${stored.summary}" +
          s" (pod ${stored.driverName} in ${stored.location}, state ${stored.finalState})")
      }
    } catch {
      case NonFatal(e) =>
        error(s"Failed to record the post-mortem of driver ${postMortem.podName}", e)
    }
  }

  /**
   * The state to report for a session, reconciled against its driver.
   *
   * `recordState` is what Kyuubi's own session record says -- the answer this used to give on its
   * own, and still the answer where there is no cluster to check against.
   */
  def sessionStatus(userName: String, recordState: String): SparkConnectSessionStatus = {
    val binding = registry.lookup(userName)
    SparkConnectSessionStatus(
      state = reconcileState(recordState, binding),
      binding = binding)
  }

  /**
   * Reconcile Kyuubi's own answer against the driver.
   *
   * The session record wins where it reports something the driver cannot contradict: a session
   * the user closed is closed, and a launch that threw failed, whatever any pod says. Everything
   * else -- above all, the claim that the session is `RUNNING` -- is the driver's to answer.
   */
  private[connect] def reconcileState(
      recordState: String,
      binding: Option[SparkConnectSessionInfo]): String = {
    if (recordState == STATE_CLOSED || recordState == STATE_FAILED) {
      return recordState
    }
    binding match {
      case Some(record) if record.isRecoveryAbandoned => STATE_FAILED
      case Some(record) if record.isRecovering => STATE_RECOVERING
      case Some(record) => driverDerivedState(recordState, record)
      // Nothing binds this user to an engine, which is what the record says of a session whose
      // engine has been cleaned up entirely. There is no driver to ask about.
      case None => recordState
    }
  }

  /**
   * What the driver says about a session Kyuubi believes is starting or running.
   *
   * The distinction that matters is between a session whose driver has not appeared yet and one
   * whose driver has been and gone: the first is waited out and the second is acted on, and an
   * operator handed one word for both has to go and look at the cluster to tell which they have.
   * Kyuubi can tell them apart because it knows whether it ever saw the engine serve -- from the
   * informer's own record of the application, from the session record's opened time, and from
   * whether a post-mortem was ever captured for the engine.
   */
  private def driverDerivedState(
      recordState: String,
      binding: SparkConnectSessionInfo): String = {
    if (!driverObserver.isAvailable) {
      // No Kubernetes client on this instance: there is no driver pod for it to inspect, and
      // inventing a state from the absence of one would be a false report either way.
      return recordState
    }
    val applicationState = driverObserver.applicationState(binding.engineTag)
    val driverPod = driverObserver.driverPod(binding.engineTag)
    val everServed = binding.wasRestarted ||
      binding.driverPostMortems.nonEmpty ||
      recordState == STATE_RUNNING ||
      applicationState.contains(ApplicationState.RUNNING)
    driverPod match {
      case Some(pod) if pod.phase == POD_PHASE_RUNNING =>
        // The pod being up is necessary but not sufficient: an engine is only usable once the
        // locator can route to it, which is the same question the relay asks per call.
        if (engineLocator.locate(binding.engineTag).isDefined) STATE_RUNNING else STATE_PENDING
      case Some(pod) if pod.phase == POD_PHASE_PENDING => STATE_PENDING
      // Succeeded, Failed, or a phase Kubernetes could not determine: either way this driver is
      // not coming back on its own, because Spark pins restartPolicy: Never on it.
      case Some(_) => STATE_DEAD
      case None if applicationState.exists(isTerminated) => STATE_DEAD
      case None if everServed => STATE_DEAD
      // No pod, and no evidence there ever was one. The engine is still being submitted; the
      // launch operation itself fails the session if it never arrives.
      case None => STATE_PENDING
    }
  }

  /**
   * Bring the user's engine back if it is dead and recovery has anything left to try.
   *
   * Returns what the caller should tell the client. The relaunch itself is asynchronous: it takes
   * a minute or two and the caller is usually a gRPC call the client is waiting on, which is
   * better answered `UNAVAILABLE` and retried than held open.
   */
  def recoverIfDead(userName: String, recordState: String): SparkConnectRecoveryOutcome = {
    val binding = registry.lookup(userName)
    binding match {
      case None => SparkConnectRecoveryOutcome.NoSession
      case Some(record) if record.isRecoveryAbandoned =>
        SparkConnectRecoveryOutcome.Abandoned(
          record.recoveryMessage.getOrElse(RECOVERY_ABANDONED_WITHOUT_REASON))
      case Some(record) if record.isRecovering => SparkConnectRecoveryOutcome.Recovering
      case Some(record) if reconcileState(recordState, Some(record)) != STATE_DEAD =>
        SparkConnectRecoveryOutcome.Healthy
      case Some(record) if !recoveryEnabled =>
        SparkConnectRecoveryOutcome.Abandoned(RECOVERY_DISABLED_MESSAGE)
      case Some(record) if record.restartCount >= maxRecoveryAttempts =>
        val reason = exhaustedMessage(record)
        registry.abandonRecovery(userName, reason)
        SparkConnectRecoveryOutcome.Abandoned(reason)
      case Some(record) => scheduleRecovery(record)
    }
  }

  private def exhaustedMessage(binding: SparkConnectSessionInfo): String = {
    val cause = binding.latestPostMortem.map(_.summary).getOrElse(UNEXPLAINED_DRIVER_DEATH)
    s"The Spark driver for this session died ${binding.restartCount + 1} times and was" +
      s" relaunched ${binding.restartCount} times, which is the configured limit" +
      s" (${FRONTEND_SPARK_CONNECT_RECOVERY_MAX_ATTEMPTS.key} = $maxRecoveryAttempts)." +
      s" The last failure was: $cause. Kyuubi will not launch another driver for this session;" +
      " create a new session once the cause is addressed."
  }

  private def scheduleRecovery(binding: SparkConnectSessionInfo): SparkConnectRecoveryOutcome = {
    val userName = binding.userName
    if (!recoveriesInFlight.add(userName)) {
      return SparkConnectRecoveryOutcome.Recovering
    }
    // Persisted before the wait, so that a peer instance reading the row while this one sleeps
    // out the backoff sees RECOVERING and does not start a driver of its own.
    registry.beginRecovery(userName)
    val delay = backoffFor(binding.restartCount)
    info(s"Relaunching the Spark Connect engine of $userName in ${delay}ms," +
      s" attempt ${binding.restartCount + 1} of $maxRecoveryAttempts")
    try {
      recoveryExecutor.schedule(
        new Runnable {
          override def run(): Unit = {
            try {
              relaunch(userName)
            } finally {
              recoveriesInFlight.remove(userName)
            }
          }
        },
        delay,
        TimeUnit.MILLISECONDS)
      SparkConnectRecoveryOutcome.Recovering
    } catch {
      case NonFatal(e) =>
        recoveriesInFlight.remove(userName)
        val reason = s"Kyuubi could not schedule an engine relaunch: ${e.getMessage}"
        registry.abandonRecovery(userName, reason)
        SparkConnectRecoveryOutcome.Abandoned(reason)
    }
  }

  /**
   * How long to wait before the next relaunch: doubling from the initial delay, capped.
   *
   * A driver that dies because its image will not pull dies the same way in one second and in
   * five minutes, so backing off is what keeps a crash loop from becoming a stream of pods, and
   * the cap is what keeps the wait from growing past any use to the person waiting.
   */
  private[connect] def backoffFor(attemptsSoFar: Int): Long = {
    if (attemptsSoFar <= 0) {
      return 0L
    }
    val doublings = math.min(attemptsSoFar - 1, MAX_BACKOFF_DOUBLINGS)
    math.min(initialBackoff * (1L << doublings), maxBackoff)
  }

  /**
   * Launch a replacement engine through Kyuubi's ordinary provisioning path and rebind to it.
   *
   * The new engine is a new Kyuubi session with a `kyuubi-unique-tag` of its own and a freshly
   * minted engine credential, because it is a new driver in every sense; reusing the dead
   * engine's tag would leave the locator hunting for a pod that no longer exists.
   */
  private def relaunch(userName: String): Unit = {
    val binding = registry.lookup(userName)
    if (binding.isEmpty) {
      info(s"Abandoning the engine relaunch for $userName: their session binding is gone")
      return
    }
    try {
      val engineToken = SparkConnect.generateToken()
      val sessionId = provisionEngine(SparkConnectEngineRequest(
        userName = userName,
        engineToken = engineToken,
        requestedConf = binding.get.engineConf))
      registry.completeRecovery(
        userName = userName,
        sessionId = sessionId,
        engineTag = sessionId,
        engineToken = engineToken)
      info(s"Relaunched the Spark Connect engine of $userName as session $sessionId." +
        " Its Spark session state -- temporary views, cached frames, artifacts -- did not" +
        " survive the driver that held it.")
    } catch {
      case NonFatal(e) =>
        // One failed launch is not the end of recovery: the attempt is spent, and the next touch
        // of the session tries again until the configured limit runs out. The generation is not
        // bumped, because no new engine exists for it to identify.
        warn(s"Failed to relaunch the Spark Connect engine of $userName", e)
        val spent = registry.failRecoveryAttempt(userName)
        if (spent.exists(_.restartCount >= maxRecoveryAttempts)) {
          registry.abandonRecovery(
            userName,
            s"The last of $maxRecoveryAttempts engine relaunches failed to start:" +
              s" ${e.getMessage}")
        }
    }
  }

  /**
   * Relaunch every dead engine this instance knows a binding for.
   *
   * Only reached with eager recovery switched on. It sweeps the bindings this instance has
   * cached, which is those it has served; a peer sweeps its own, and the persisted `RECOVERING`
   * state keeps two of them from launching two drivers for one user.
   */
  private def reconcileAll(): Unit = {
    registry.cachedUserNames.foreach { userName =>
      try {
        recoverIfDead(userName, STATE_RUNNING)
      } catch {
        case NonFatal(e) =>
          warn(s"Eager Spark Connect recovery failed for $userName", e)
      }
    }
  }
}

/** What a session should be reported as, and the engine binding that answer came from. */
case class SparkConnectSessionStatus(
    state: String,
    binding: Option[SparkConnectSessionInfo])

/** Everything the supervisor needs to launch one Spark Connect engine. */
case class SparkConnectEngineRequest(
    userName: String,
    engineToken: String,
    requestedConf: Map[String, String])

/** What a caller that found a dead session should tell its client. */
sealed trait SparkConnectRecoveryOutcome

object SparkConnectRecoveryOutcome {

  /** The user has no engine binding at all; they have to create a session. */
  case object NoSession extends SparkConnectRecoveryOutcome

  /** The driver is alive, or still starting. Nothing to recover. */
  case object Healthy extends SparkConnectRecoveryOutcome

  /**
   * A replacement engine is on its way. Retryable: a Spark Connect client told `UNAVAILABLE`
   * backs off and comes back, which is exactly the right behaviour while a driver starts.
   */
  case object Recovering extends SparkConnectRecoveryOutcome

  /**
   * Recovery is over and the session is not coming back. Not retryable, and deliberately not
   * `UNAVAILABLE`: that would have PySpark retrying for the better part of ten minutes against a
   * session nothing will ever answer, and hide `reason` behind a deadline exceeded at the end.
   */
  case class Abandoned(reason: String) extends SparkConnectRecoveryOutcome
}

object SparkConnectSessionSupervisor {

  /** The engine has been asked for and no driver has served yet. */
  val STATE_PENDING = "PENDING"

  /** The driver is up and routable. */
  val STATE_RUNNING = "RUNNING"

  /**
   * The driver served and is now gone. Distinct from `PENDING` because the two call for opposite
   * responses: one is waited out, the other is acted on.
   */
  val STATE_DEAD = "DEAD"

  /** A replacement driver is being launched, or its backoff is being waited out. */
  val STATE_RECOVERING = "RECOVERING"

  /** The user closed the session. */
  val STATE_CLOSED = "CLOSED"

  /** Terminal: the launch failed, or recovery gave up. */
  val STATE_FAILED = "FAILED"

  /** States in which a session can actually serve a query. */
  val ACTIVE_STATES: Set[String] = Set(STATE_RUNNING)

  private val RECOVERY_DISABLED_MESSAGE =
    "The Spark driver for this session has died and automatic recovery is switched off on this" +
      " deployment (kyuubi.frontend.spark.connect.recovery.enabled). Create a new session to get" +
      " a new engine."

  private val RECOVERY_ABANDONED_WITHOUT_REASON =
    "Recovery of the Spark driver for this session was abandoned."

  private val UNEXPLAINED_DRIVER_DEATH =
    "not recorded -- the driver pod was gone before Kyuubi could capture a post-mortem"

  private val POD_PHASE_RUNNING = "Running"
  private val POD_PHASE_PENDING = "Pending"

  /** Keeps the shift in [[SparkConnectSessionSupervisor.backoffFor]] inside a `Long`. */
  private val MAX_BACKOFF_DOUBLINGS = 30
}
