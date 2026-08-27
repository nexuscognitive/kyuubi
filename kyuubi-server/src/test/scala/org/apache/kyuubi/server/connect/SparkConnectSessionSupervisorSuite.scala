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

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.server.connect.SparkConnectSessionSupervisor._

/**
 * The reconciliation and recovery rules, driven against a driver observer a test controls.
 *
 * Nothing here talks to Kubernetes. The point of the suite is precisely the situations a live
 * cluster will not produce on request: a pod that has been reclaimed, a driver that dies on every
 * attempt, and a Kyuubi with no cluster to ask.
 */
class SparkConnectSessionSupervisorSuite extends KyuubiFunSuite {

  private val userName = "connect_user"

  private var registry: SparkConnectSessionRegistry = _
  private var observer: FakeSparkConnectDriverObserver = _
  private var provisionedEngines: ConcurrentLinkedQueue[SparkConnectEngineRequest] = _
  private var provisionFailure: Option[Throwable] = None

  override def beforeEach(): Unit = {
    super.beforeEach()
    registry = new SparkConnectSessionRegistry(metadataManager = None)
    observer = new FakeSparkConnectDriverObserver()
    provisionedEngines = new ConcurrentLinkedQueue[SparkConnectEngineRequest]()
    provisionFailure = None
  }

  /**
   * A supervisor over the shared fixtures, whose locator agrees with the observer.
   *
   * The two are kept consistent on purpose: an engine is routable exactly when its pod is running,
   * and a test that let them disagree would be asserting on a state the cluster cannot be in.
   */
  private def newSupervisor(conf: KyuubiConf = KyuubiConf()): SparkConnectSessionSupervisor = {
    val locator = new SparkConnectEngineLocator {
      override def locate(engineTag: String): Option[SparkConnectEngineAddress] =
        observer.driverPod(engineTag)
          .filter(_.phase == FakeSparkConnectDriverObserver.POD_PHASE_RUNNING)
          .flatMap(_.podIp)
          .map(SparkConnectEngineAddress(_, 15002))
    }
    val supervisor = new SparkConnectSessionSupervisor(
      conf,
      registry,
      locator,
      observer,
      request => {
        provisionedEngines.add(request)
        provisionFailure.foreach(throw _)
        UUID.randomUUID().toString
      })
    supervisor.start()
    supervisor
  }

  private def registerSession(engineTag: String = UUID.randomUUID().toString): String = {
    registry.register(userName, engineTag, engineTag, "an-engine-credential")
    engineTag
  }

  /** Recovery is asynchronous, so tests wait for the relaunch rather than assume it happened. */
  private def awaitEngineCount(expected: Int): Unit =
    eventually(timeout(10.seconds), interval(20.milliseconds)) {
      assert(provisionedEngines.size() == expected)
    }

  test("a session whose driver pod is gone does not report as active") {
    val engineTag = registerSession()
    observer.driverIsRunning(engineTag)
    val supervisor = newSupervisor()
    assert(supervisor.sessionStatus(userName, STATE_RUNNING).state == STATE_RUNNING)

    // Exactly the production failure: the driver died, Kubernetes reclaimed the pod, and Kyuubi's
    // own session record knows nothing about either.
    observer.driverDiedAndPodWasReclaimed(engineTag)

    val state = supervisor.sessionStatus(userName, STATE_RUNNING).state
    assert(state == STATE_DEAD)
    assert(!ACTIVE_STATES.contains(state))
  }

  test("a driver that has not started yet is distinguishable from one that died") {
    val neverStarted = registerSession()
    val supervisor = newSupervisor()
    // No pod, and no record that there ever was one: still being submitted.
    assert(supervisor.sessionStatus(userName, STATE_PENDING).state == STATE_PENDING)

    observer.driverIsPending(neverStarted)
    assert(supervisor.sessionStatus(userName, STATE_PENDING).state == STATE_PENDING)

    observer.driverIsRunning(neverStarted)
    observer.driverDiedAndPodWasReclaimed(neverStarted)
    // Same "no pod" observation as the first assertion, opposite answer -- because Kyuubi saw
    // this one serve. The two call for opposite responses, so one word for both is no use.
    assert(supervisor.sessionStatus(userName, STATE_PENDING).state == STATE_DEAD)
  }

  test("a session whose driver died is DEAD even after Kyuubi has forgotten the application") {
    val engineTag = registerSession()
    observer.driverIsRunning(engineTag)
    val supervisor = newSupervisor()
    supervisor.recordDriverDeath(FakeSparkConnectDriverObserver.oomKilledPostMortem(engineTag))
    observer.forget(engineTag)

    // Nothing on the cluster remembers this engine, and the informer store has been cleaned up.
    // The stored post-mortem is what still says a driver existed and died.
    assert(supervisor.sessionStatus(userName, STATE_PENDING).state == STATE_DEAD)
  }

  test("a session on a Kyuubi with no cluster to ask keeps the state its record has") {
    val engineTag = registerSession()
    observer.available = false
    observer.driverDiedAndPodWasReclaimed(engineTag)
    val supervisor = newSupervisor()

    // No Kubernetes client means no information, which is not the same as no driver: reporting
    // every session dead on a deployment that runs engines elsewhere would be worse than the bug.
    assert(supervisor.sessionStatus(userName, STATE_RUNNING).state == STATE_RUNNING)
    assert(supervisor.sessionStatus(userName, STATE_PENDING).state == STATE_PENDING)
  }

  test("a closed or failed session is not overruled by whatever a pod says") {
    val engineTag = registerSession()
    observer.driverIsRunning(engineTag)
    val supervisor = newSupervisor()

    assert(supervisor.sessionStatus(userName, STATE_CLOSED).state == STATE_CLOSED)
    assert(supervisor.sessionStatus(userName, STATE_FAILED).state == STATE_FAILED)
  }

  test("recovery provisions a new engine and the binding points at it") {
    val deadEngineTag = registerSession()
    observer.driverIsRunning(deadEngineTag)
    val supervisor = newSupervisor(recoveryConf())
    observer.driverDiedAndPodWasReclaimed(deadEngineTag)

    assert(supervisor.recoverIfDead(userName, STATE_RUNNING) ==
      SparkConnectRecoveryOutcome.Recovering)
    awaitEngineCount(1)

    eventually(timeout(10.seconds), interval(20.milliseconds)) {
      val binding = registry.lookup(userName).getOrElse(fail("the binding disappeared"))
      assert(binding.engineTag != deadEngineTag, "the binding still names the dead engine")
      assert(binding.hasLiveSession)
      assert(binding.sessionId == binding.engineTag)
      assert(!binding.isRecovering)
      // The signal a client reads to know its Spark session was replaced.
      assert(binding.generation == 1)
      assert(binding.restartCount == 1)
      assert(binding.lastRestartTime > 0)
    }
  }

  test("a recovered engine is launched with the conf the session was created with") {
    val deadEngineTag = UUID.randomUUID().toString
    registry.register(
      userName,
      deadEngineTag,
      deadEngineTag,
      "an-engine-credential",
      engineConf = Map("spark.executor.memory" -> "8g"))
    observer.driverIsRunning(deadEngineTag)
    val supervisor = newSupervisor(recoveryConf())
    observer.driverDiedAndPodWasReclaimed(deadEngineTag)

    supervisor.recoverIfDead(userName, STATE_RUNNING)
    awaitEngineCount(1)

    // Recovery cannot bring back the session's data. There is no reason for it to lose the
    // engine's shape as well.
    assert(provisionedEngines.peek().requestedConf == Map("spark.executor.memory" -> "8g"))
  }

  test("a recovered engine gets a credential of its own") {
    val deadEngineTag = registerSession()
    observer.driverIsRunning(deadEngineTag)
    val supervisor = newSupervisor(recoveryConf())
    observer.driverDiedAndPodWasReclaimed(deadEngineTag)

    supervisor.recoverIfDead(userName, STATE_RUNNING)
    awaitEngineCount(1)

    // A new driver is a new principal; handing it the dead one's credential would keep a
    // credential alive past the process it was minted for.
    assert(provisionedEngines.peek().engineToken != "an-engine-credential")
    eventually(timeout(10.seconds), interval(20.milliseconds)) {
      assert(registry.lookup(userName).map(_.engineToken).contains(
        provisionedEngines.peek().engineToken))
    }
  }

  test("recovery stops after the configured attempts and lands in a terminal failed state") {
    val engineTag = registerSession()
    observer.driverIsRunning(engineTag)
    val supervisor = newSupervisor(recoveryConf(maxAttempts = 2))

    // A crash loop: every engine recovery launches dies the same way the first one did.
    (1 to 2).foreach { attempt =>
      val currentTag = registry.lookup(userName).map(_.engineTag).getOrElse(fail("no binding"))
      observer.driverIsRunning(currentTag)
      supervisor.recordDriverDeath(
        FakeSparkConnectDriverObserver.oomKilledPostMortem(currentTag))
      observer.driverDiedAndPodWasReclaimed(currentTag)
      assert(supervisor.recoverIfDead(userName, STATE_RUNNING) ==
        SparkConnectRecoveryOutcome.Recovering)
      awaitEngineCount(attempt)
      eventually(timeout(10.seconds), interval(20.milliseconds)) {
        assert(registry.lookup(userName).exists(_.restartCount == attempt))
      }
    }

    val currentTag = registry.lookup(userName).map(_.engineTag).getOrElse(fail("no binding"))
    observer.driverIsRunning(currentTag)
    supervisor.recordDriverDeath(FakeSparkConnectDriverObserver.oomKilledPostMortem(currentTag))
    observer.driverDiedAndPodWasReclaimed(currentTag)

    val outcome = supervisor.recoverIfDead(userName, STATE_RUNNING)
    assert(outcome.isInstanceOf[SparkConnectRecoveryOutcome.Abandoned])
    val reason = outcome.asInstanceOf[SparkConnectRecoveryOutcome.Abandoned].reason
    // The reason has to be readable by whoever is asking why their session will not come back.
    assert(reason.contains("OOMKilled"), s"the reason does not say what killed it: $reason")
    assert(reason.contains(FRONTEND_SPARK_CONNECT_RECOVERY_MAX_ATTEMPTS.key))

    // No third driver, however many times the session is touched.
    assert(provisionedEngines.size() == 2)
    assert(supervisor.recoverIfDead(userName, STATE_RUNNING)
      .isInstanceOf[SparkConnectRecoveryOutcome.Abandoned])
    assert(provisionedEngines.size() == 2)

    // Terminal, and reported as such: FAILED rather than DEAD, which is a state recovery acts on.
    assert(supervisor.sessionStatus(userName, STATE_RUNNING).state == STATE_FAILED)
    assert(registry.lookup(userName).exists(_.isRecoveryAbandoned))
  }

  test("recovery is not attempted at all when it is switched off") {
    val engineTag = registerSession()
    observer.driverIsRunning(engineTag)
    val supervisor = newSupervisor(
      recoveryConf().set(FRONTEND_SPARK_CONNECT_RECOVERY_ENABLED, false))
    observer.driverDiedAndPodWasReclaimed(engineTag)

    val outcome = supervisor.recoverIfDead(userName, STATE_RUNNING)
    assert(outcome.isInstanceOf[SparkConnectRecoveryOutcome.Abandoned])
    assert(provisionedEngines.isEmpty)
  }

  test("a healthy session is left alone") {
    val engineTag = registerSession()
    observer.driverIsRunning(engineTag)
    val supervisor = newSupervisor(recoveryConf())

    assert(supervisor.recoverIfDead(userName, STATE_RUNNING) ==
      SparkConnectRecoveryOutcome.Healthy)
    assert(provisionedEngines.isEmpty)
  }

  test("a user with no binding is told to create a session rather than recovered") {
    val supervisor = newSupervisor(recoveryConf())
    assert(supervisor.recoverIfDead("a-stranger", STATE_RUNNING) ==
      SparkConnectRecoveryOutcome.NoSession)
    assert(provisionedEngines.isEmpty)
  }

  test("concurrent calls on one dead session launch one driver between them") {
    val engineTag = registerSession()
    observer.driverIsRunning(engineTag)
    val supervisor = newSupervisor(recoveryConf())
    observer.driverDiedAndPodWasReclaimed(engineTag)

    // A Spark Connect client has several calls in flight at once, and every one of them reaches
    // the relay with the same pre-recovery binding.
    val outcomes = (1 to 8).par.map(_ => supervisor.recoverIfDead(userName, STATE_RUNNING)).toList
    assert(outcomes.forall(_ == SparkConnectRecoveryOutcome.Recovering))
    awaitEngineCount(1)
    Thread.sleep(200)
    assert(provisionedEngines.size() == 1)
  }

  test("a relaunch that fails to start spends an attempt without claiming a new engine") {
    val engineTag = registerSession()
    observer.driverIsRunning(engineTag)
    val supervisor = newSupervisor(recoveryConf(maxAttempts = 2))
    observer.driverDiedAndPodWasReclaimed(engineTag)
    provisionFailure = Some(new IllegalStateException("no capacity in the cluster"))

    supervisor.recoverIfDead(userName, STATE_RUNNING)
    awaitEngineCount(1)

    eventually(timeout(10.seconds), interval(20.milliseconds)) {
      val binding = registry.lookup(userName).getOrElse(fail("the binding disappeared"))
      assert(binding.restartCount == 1)
      // No engine came into being, so nothing was replaced and no client should be told it was.
      assert(binding.generation == 0)
      assert(!binding.isRecovering)
    }
  }

  test("the backoff doubles and is capped") {
    val supervisor = newSupervisor(recoveryConf(maxAttempts = 10)
      .set(FRONTEND_SPARK_CONNECT_RECOVERY_BACKOFF_INITIAL, 1000L)
      .set(FRONTEND_SPARK_CONNECT_RECOVERY_BACKOFF_MAX, 4000L))

    // The first relaunch is immediate: nothing has failed yet that waiting would help with.
    assert(supervisor.backoffFor(0) == 0L)
    assert(supervisor.backoffFor(1) == 1000L)
    assert(supervisor.backoffFor(2) == 2000L)
    assert(supervisor.backoffFor(3) == 4000L)
    // Capped, so that the wait never grows past any use to the person waiting for it.
    assert(supervisor.backoffFor(4) == 4000L)
    assert(supervisor.backoffFor(60) == 4000L)
  }

  test("a driver's post-mortem is stored against the session that owned it") {
    val engineTag = registerSession()
    val supervisor = newSupervisor()

    observer.announceDriverDeath(FakeSparkConnectDriverObserver.oomKilledPostMortem(engineTag))

    val postMortem = registry.lookup(userName).flatMap(_.latestPostMortem)
      .getOrElse(fail("the driver death was not recorded"))
    assert(postMortem.oomKilled)
    assert(postMortem.summary.contains("OOMKilled"))
    assert(postMortem.containers.exists(_.exitCode.contains(137)))
    // The events are the part that Kubernetes would have collected with the pod.
    assert(postMortem.events.exists(_.reason == "Evicted"))
    assert(supervisor != null)
  }

  test("a driver that belongs to no Spark Connect session is ignored") {
    val supervisor = newSupervisor()
    // Every Spark engine driver on the cluster arrives here, batch and Thrift alike.
    supervisor.recordDriverDeath(
      FakeSparkConnectDriverObserver.oomKilledPostMortem("some-batch-engine"))
    assert(registry.lookup(userName).isEmpty)
  }

  test("post-mortems are kept newest first and bounded") {
    val engineTag = registerSession()
    val supervisor = newSupervisor(recoveryConf()
      .set(FRONTEND_SPARK_CONNECT_POST_MORTEM_RETAIN, 3))

    (1 to 6).foreach { index =>
      supervisor.recordDriverDeath(
        FakeSparkConnectDriverObserver.oomKilledPostMortem(engineTag)
          .copy(podName = s"driver-$index", capturedTime = index.toLong))
    }

    val stored = registry.lookup(userName).map(_.driverPostMortems).getOrElse(Nil)
    // Bounded, so one crash-looping session cannot grow its record without limit.
    assert(stored.size == 3)
    // Newest first: the death that explains the current state is the one at the top.
    assert(stored.map(_.driverName) == Seq("driver-6", "driver-5", "driver-4"))
  }

  test("post-mortem events are bounded so a pod that never scheduled cannot fill the record") {
    val engineTag = registerSession()
    val supervisor = newSupervisor(recoveryConf()
      .set(FRONTEND_SPARK_CONNECT_POST_MORTEM_MAX_EVENTS, 2))
    val manyEvents = FakeSparkConnectDriverObserver.oomKilledPostMortem(engineTag)
    supervisor.recordDriverDeath(
      manyEvents.copy(events = (1 to 50).flatMap(_ => manyEvents.events)))

    assert(registry.lookup(userName).flatMap(_.latestPostMortem).map(_.events.size).contains(2))
  }

  private def recoveryConf(maxAttempts: Int = 3): KyuubiConf = KyuubiConf()
    .set(FRONTEND_SPARK_CONNECT_RECOVERY_ENABLED, true)
    .set(FRONTEND_SPARK_CONNECT_RECOVERY_MAX_ATTEMPTS, maxAttempts)
    // No waiting in tests: the backoff has a test of its own, and every other test here would
    // otherwise be paying for it.
    .set(FRONTEND_SPARK_CONNECT_RECOVERY_BACKOFF_INITIAL, 0L)
    .set(FRONTEND_SPARK_CONNECT_RECOVERY_BACKOFF_MAX, 0L)
}
