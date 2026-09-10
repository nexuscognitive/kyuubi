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
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.server.connect.SparkConnectSessionSupervisor._
import org.apache.kyuubi.server.metadata.api.SparkConnectSessionInfo

/**
 * The create-time decision table, driven against a driver observer, a locator and a probe the
 * test controls.
 *
 * "Held" below means a Kyuubi session open on this instance. The restart the suite is built
 * around is a binding whose session is not held: the store kept the binding, and the session it
 * names died with the JVM that held it.
 */
class SparkConnectSessionResolverSuite extends KyuubiFunSuite {

  private val userName = "connect_user"
  private val engineToken = "an-engine-credential"

  private var registry: SparkConnectSessionRegistry = _
  private var observer: FakeSparkConnectDriverObserver = _
  private var supervisor: SparkConnectSessionSupervisor = _
  private var provisionedEngines: ConcurrentLinkedQueue[SparkConnectEngineRequest] = _

  /** The Kyuubi sessions this instance holds, and the state each one's record gives. */
  private var heldSessions: ConcurrentHashMap[String, String] = _

  /** Engines whose port answers, by tag, and the one credential each accepts. */
  @volatile private var servingEngines: Map[String, String] = Map.empty
  private var probeCount: AtomicInteger = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    registry = new SparkConnectSessionRegistry(metadataManager = None)
    observer = new FakeSparkConnectDriverObserver()
    provisionedEngines = new ConcurrentLinkedQueue[SparkConnectEngineRequest]()
    heldSessions = new ConcurrentHashMap[String, String]()
    servingEngines = Map.empty
    probeCount = new AtomicInteger(0)
  }

  override def afterEach(): Unit = {
    if (supervisor != null) supervisor.stop()
    super.afterEach()
  }

  private def newResolver(conf: KyuubiConf = defaultConf()): SparkConnectSessionResolver = {
    // An engine is routable exactly when its pod is running, as it is on a cluster.
    val locator = new SparkConnectEngineLocator {
      override def locate(engineTag: String): Option[SparkConnectEngineAddress] =
        observer.driverPod(engineTag)
          .filter(_.phase == FakeSparkConnectDriverObserver.POD_PHASE_RUNNING)
          .map(_ => SparkConnectEngineAddress(engineTag, 15002))
    }
    val probe = new SparkConnectEngineProbe {
      override def isServing(address: SparkConnectEngineAddress, token: String): Boolean = {
        probeCount.incrementAndGet()
        servingEngines.get(address.host).contains(token)
      }
    }
    val provision: SparkConnectEngineRequest => String = request => {
      provisionedEngines.add(request)
      val sessionId = UUID.randomUUID().toString
      heldSessions.put(sessionId, STATE_PENDING)
      sessionId
    }
    supervisor = new SparkConnectSessionSupervisor(conf, registry, locator, observer, provision)
    supervisor.start()
    new SparkConnectSessionResolver(registry, supervisor, locator, probe, provision)
  }

  private def open(
      resolver: SparkConnectSessionResolver,
      requestedConf: Map[String, String] = Map.empty): SparkConnectSessionResolution =
    resolver.openSession(userName, requestedConf, sessionId => Option(heldSessions.get(sessionId)))

  /**
   * A binding exactly as a restart leaves it: it names a session, and no session on this instance
   * is that session.
   */
  private def bindingLeftByRestart(engineConf: Map[String, String] = Map.empty): String = {
    val sessionId = UUID.randomUUID().toString
    registry.register(userName, sessionId, sessionId, engineToken, engineConf)
    sessionId
  }

  /** A driver that is up, and a port that answers the credential it was launched with. */
  private def engineIsLive(engineTag: String, token: String = engineToken): Unit = {
    observer.driverIsRunning(engineTag)
    servingEngines += engineTag -> token
  }

  private def binding: SparkConnectSessionInfo =
    registry.lookup(userName).getOrElse(fail("the binding disappeared"))

  test("after a restart, a live engine is reattached under a new session, not handed back") {
    val ghostSessionId = bindingLeftByRestart(Map("spark.executor.memory" -> "8g"))
    // The engine has been through a relaunch already, so there is history to keep.
    registry.completeRecovery(userName, ghostSessionId, ghostSessionId, engineToken)
    engineIsLive(ghostSessionId)
    val resolver = newResolver()

    val resolution = open(resolver)

    assert(resolution.outcome == SparkConnectCreateOutcome.Reattached)
    assert(resolution.sessionId != ghostSessionId, "the create handed back the ghost session")
    assert(heldSessions.containsKey(resolution.sessionId))
    // Opened with the credential the running driver was launched with, so engine discovery hands
    // it that driver; and bound to the driver's own tag, which is what the relay routes on.
    assert(provisionedEngines.asScala.map(_.engineToken).toSeq == Seq(engineToken))
    assert(provisionedEngines.peek().requestedConf == Map("spark.executor.memory" -> "8g"))
    assert(binding.sessionId == resolution.sessionId)
    assert(binding.engineTag == ghostSessionId)
    assert(binding.engineToken == engineToken)
    // Nothing was replaced: the client's Spark session is still on that driver.
    assert(binding.generation == 1)
    assert(binding.restartCount == 1)
  }

  test("a reattach reuses a session this instance already holds on that engine") {
    val heldSessionId = UUID.randomUUID().toString
    registry.register(userName, heldSessionId, heldSessionId, engineToken)
    heldSessions.put(heldSessionId, STATE_RUNNING)
    // A create on a peer moved the binding to a session the peer holds.
    registry.reattach(userName, heldSessionId, UUID.randomUUID().toString)
    engineIsLive(heldSessionId)
    val resolver = newResolver()

    val resolution = open(resolver)

    // Creates alternating between instances move the binding back; they do not open a session
    // per create.
    assert(resolution.outcome == SparkConnectCreateOutcome.Reattached)
    assert(resolution.sessionId == heldSessionId)
    assert(provisionedEngines.isEmpty)
    assert(binding.sessionId == heldSessionId)
  }

  test("after a restart, a dead engine is relaunched now, and counted as a recovery") {
    val ghostSessionId = bindingLeftByRestart(Map("spark.executor.memory" -> "8g"))
    observer.driverDiedAndPodWasReclaimed(ghostSessionId)
    val resolver = newResolver()

    val resolution = open(resolver)

    assert(resolution.outcome == SparkConnectCreateOutcome.Relaunched)
    assert(resolution.sessionId != ghostSessionId, "the create handed back the ghost session")
    assert(heldSessions.containsKey(resolution.sessionId))
    assert(provisionedEngines.size() == 1)
    assert(provisionedEngines.peek().requestedConf == Map("spark.executor.memory" -> "8g"))
    // A new driver: tagged by its own session, with a credential of its own.
    assert(binding.sessionId == resolution.sessionId)
    assert(binding.engineTag == resolution.sessionId)
    assert(binding.engineToken != engineToken)
    assert(binding.generation == 1)
    assert(binding.restartCount == 1)
    assert(!binding.isRecovering)
  }

  test("an engine nothing has observed for longer than a launch takes is treated as gone") {
    // After a restart the informer has no record of a pod reclaimed before it started, and no
    // post-mortem was captured: the only evidence is that no launch could still be under way.
    val ghostSessionId = bindingLeftByRestart()
    val resolver = newResolver(defaultConf().set(ENGINE_INIT_TIMEOUT, 0L))
    Thread.sleep(20)

    val resolution = open(resolver)

    assert(resolution.outcome == SparkConnectCreateOutcome.Relaunched)
    assert(resolution.sessionId != ghostSessionId)
  }

  test("relaunching an engine for a create stays within the recovery bounds") {
    val ghostSessionId = bindingLeftByRestart()
    // The single permitted relaunch has already been spent on this binding.
    registry.completeRecovery(userName, ghostSessionId, ghostSessionId, engineToken)
    observer.driverDiedAndPodWasReclaimed(ghostSessionId)
    val resolver = newResolver(
      defaultConf().set(FRONTEND_SPARK_CONNECT_RECOVERY_MAX_ATTEMPTS, 1))

    val resolution = open(resolver, Map("spark.executor.cores" -> "2"))

    // No second relaunch: the user asked for a session, so they get a new one, the way they do
    // on any session recovery gave up on -- with the conf they asked for this time.
    assert(resolution.outcome == SparkConnectCreateOutcome.Created)
    assert(resolution.sessionId != ghostSessionId)
    assert(provisionedEngines.size() == 1)
    assert(provisionedEngines.peek().requestedConf == Map("spark.executor.cores" -> "2"))
    assert(binding.restartCount == 0)
    assert(binding.generation == 0)
  }

  test("a dead engine is not relaunched for a create when recovery is switched off") {
    val ghostSessionId = bindingLeftByRestart()
    observer.driverDiedAndPodWasReclaimed(ghostSessionId)
    val resolver = newResolver(defaultConf().set(FRONTEND_SPARK_CONNECT_RECOVERY_ENABLED, false))

    val resolution = open(resolver)

    assert(resolution.outcome == SparkConnectCreateOutcome.Created)
    assert(resolution.sessionId != ghostSessionId)
    assert(binding.restartCount == 0)
  }

  test("a RECOVERING flag nothing is running is cleared, not waited on") {
    val ghostSessionId = bindingLeftByRestart()
    observer.driverDiedAndPodWasReclaimed(ghostSessionId)
    // The instance that set this flag died before its relaunch ran.
    registry.beginRecovery(userName)
    val resolver = newResolver(staleAfterZeroConf())
    Thread.sleep(20)

    val resolution = open(resolver)

    assert(resolution.outcome != SparkConnectCreateOutcome.AwaitingEngine)
    assert(resolution.sessionId != ghostSessionId, "the create waited on a lost relaunch")
    assert(!binding.isRecovering)
    // Evaluated as a dead engine once the flag was out of the way.
    assert(resolution.outcome == SparkConnectCreateOutcome.Relaunched)
  }

  test("a RECOVERING flag a relaunch may still be acting on is waited for") {
    val ghostSessionId = bindingLeftByRestart()
    observer.driverDiedAndPodWasReclaimed(ghostSessionId)
    // Set moments ago: a peer is relaunching this engine, and a second relaunch here would put
    // two drivers behind one user.
    registry.beginRecovery(userName)
    val resolver = newResolver()

    val resolution = open(resolver)

    assert(resolution.outcome == SparkConnectCreateOutcome.AwaitingEngine)
    assert(resolution.sessionId == ghostSessionId)
    assert(provisionedEngines.isEmpty)
    assert(binding.isRecovering)
  }

  test("an engine starting behind a session this instance does not hold is not replaced") {
    val boundSessionId = bindingLeftByRestart()
    // A peer holds the session and launched the engine a moment ago.
    observer.driverIsPending(boundSessionId)
    val resolver = newResolver()

    val resolution = open(resolver)

    assert(resolution.outcome == SparkConnectCreateOutcome.AwaitingEngine)
    assert(resolution.sessionId == boundSessionId)
    assert(provisionedEngines.isEmpty)
  }

  test("a live session held here is handed back, and only once its engine has answered") {
    val sessionId = UUID.randomUUID().toString
    registry.register(userName, sessionId, sessionId, engineToken)
    heldSessions.put(sessionId, STATE_RUNNING)
    engineIsLive(sessionId)
    val resolver = newResolver()

    val resolution = open(resolver)

    assert(resolution.outcome == SparkConnectCreateOutcome.Reused)
    assert(resolution.sessionId == sessionId)
    assert(probeCount.get() == 1, "the engine was reused without being probed")
    assert(provisionedEngines.isEmpty)
    assert(binding.sessionId == sessionId)
  }

  test("a held session whose engine does not answer is not replaced while its pod is up") {
    val sessionId = UUID.randomUUID().toString
    registry.register(userName, sessionId, sessionId, engineToken)
    heldSessions.put(sessionId, STATE_RUNNING)
    // The pod runs, but the port does not answer: starting, or wedged. Neither is proof of death,
    // and a relaunch would leave this driver running beside its replacement.
    observer.driverIsRunning(sessionId)
    val resolver = newResolver()

    val resolution = open(resolver)

    assert(resolution.outcome == SparkConnectCreateOutcome.AwaitingEngine)
    assert(resolution.sessionId == sessionId)
    assert(probeCount.get() == 1)
    assert(provisionedEngines.isEmpty)
  }

  test("an engine left by a closed session is reused only once it has answered") {
    val engineTag = UUID.randomUUID().toString
    registry.register(userName, engineTag, engineTag, engineToken)
    registry.unregister(engineTag)
    // The pod is up, but the port refuses: a pod the informer calls running is not an engine.
    observer.driverIsRunning(engineTag)
    val resolver = newResolver()

    val refused = open(resolver)
    assert(refused.outcome == SparkConnectCreateOutcome.Created)
    assert(binding.engineTag == refused.sessionId, "an unverified engine was reused")
    assert(provisionedEngines.peek().engineToken != engineToken)
  }

  test("an engine left by a closed session is reused once it answers") {
    val engineTag = UUID.randomUUID().toString
    registry.register(userName, engineTag, engineTag, engineToken)
    registry.unregister(engineTag)
    engineIsLive(engineTag)
    val resolver = newResolver()

    val resolution = open(resolver)

    assert(resolution.outcome == SparkConnectCreateOutcome.Created)
    assert(binding.sessionId == resolution.sessionId)
    assert(binding.engineTag == engineTag)
    assert(binding.engineToken == engineToken)
    assert(provisionedEngines.peek().engineToken == engineToken)
  }

  test("concurrent creates on a dead engine launch one engine and agree on the session") {
    val ghostSessionId = bindingLeftByRestart()
    observer.driverDiedAndPodWasReclaimed(ghostSessionId)
    val resolver = newResolver()

    // A page and a client retrying at once: every one of them finds the same dead engine.
    val resolutions = (1 to 8).par.map(_ => open(resolver)).toList

    assert(provisionedEngines.size() == 1)
    assert(resolutions.map(_.sessionId).distinct == Seq(binding.sessionId))
    assert(!resolutions.exists(_.sessionId == ghostSessionId))
  }

  test("a bound session this instance does not hold is reported from its driver") {
    val boundSessionId = bindingLeftByRestart()
    val resolver = newResolver()

    engineIsLive(boundSessionId)
    assert(resolver.unheldSessionState(binding) == STATE_RUNNING)

    // Up, but not answering: not RUNNING, whatever the pod says.
    servingEngines = Map.empty
    assert(resolver.unheldSessionState(binding) == STATE_PENDING)

    observer.driverDiedAndPodWasReclaimed(boundSessionId)
    assert(resolver.unheldSessionState(binding) == STATE_DEAD)

    registry.beginRecovery(userName)
    assert(resolver.unheldSessionState(binding) == STATE_RECOVERING)
  }

  /** Recovery that does not wait, so a relaunch is observable as soon as it is decided. */
  private def defaultConf(): KyuubiConf = KyuubiConf()
    .set(FRONTEND_SPARK_CONNECT_RECOVERY_BACKOFF_INITIAL, 0L)
    .set(FRONTEND_SPARK_CONNECT_RECOVERY_BACKOFF_MAX, 0L)

  /** A conf under which any RECOVERING flag nothing here is running is already stale. */
  private def staleAfterZeroConf(): KyuubiConf = defaultConf().set(ENGINE_INIT_TIMEOUT, 0L)
}
