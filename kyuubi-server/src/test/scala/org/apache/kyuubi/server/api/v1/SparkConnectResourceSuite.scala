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

package org.apache.kyuubi.server.api.v1

import java.net.{InetAddress, ServerSocket}
import java.util.{Collections, UUID}
import java.util.concurrent.ConcurrentHashMap
import javax.ws.rs.client.Entity
import javax.ws.rs.core.{GenericType, MediaType, Response}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.client.api.v1.dto
import org.apache.kyuubi.client.api.v1.dto.{OperationLog, SessionOpenRequest, SparkConnectDriverEvents, SparkConnectDriverInfo, SparkConnectSession}
import org.apache.kyuubi.client.api.v1.dto.SparkConnectSessionData
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.server.connect.{FakeSparkConnectDriverObserver, FakeSparkConnectEngine, GrpcSparkConnectEngineProbe, SparkConnect, SparkConnectEngineAddress, SparkConnectEngineConf, SparkConnectEngineLocator, SparkConnectSessionRegistry, SparkConnectSessionSupervisor}
import org.apache.kyuubi.server.connect.SparkConnectSessionSupervisor.{STATE_PENDING, STATE_RECOVERING, STATE_RUNNING}
import org.apache.kyuubi.server.connect.SparkConnectTestHelper.SESSION_NOT_FOUND_REPLY
import org.apache.kyuubi.server.http.util.HttpAuthUtils.{basicAuthorizationHeader, AUTHORIZATION_HEADER}
import org.apache.kyuubi.server.metadata.api.{SparkConnectRecoveryState, SparkConnectSessionInfo}
import org.apache.kyuubi.session.{KyuubiSessionManager, SessionHandle}

class SparkConnectResourceSuite extends KyuubiFunSuite with RestFrontendTestHelper {

  import SparkConnectResourceSuite._

  /** Stands in for the pod informer: which driver pods exist, and in what phase. */
  private val driverObserver = new FakeSparkConnectDriverObserver(available = false)

  /** Where each running engine's Spark Connect port is, by engine tag. */
  private val engineAddresses = new ConcurrentHashMap[String, SparkConnectEngineAddress]()

  private def sessionManager: KyuubiSessionManager =
    server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]

  private def registry: SparkConnectSessionRegistry = sessionManager.sparkConnectSessionRegistry

  override def beforeAll(): Unit = {
    super.beforeAll()
    installDriverSeams()
  }

  override def afterEach(): Unit = {
    // Back to a deployment with no cluster to observe, which is what every other test here
    // assumes.
    driverObserver.available = false
    driverObserver.driverPods = Map.empty
    driverObserver.applicationStates = Map.empty
    engineAddresses.clear()
    super.afterEach()
  }

  /**
   * Put the REST paths on a cluster the test controls, and start their supervision afresh -- as a
   * restarted instance would. The observer says which driver pods exist, a running one is
   * reachable wherever the test put its engine, and the probe is the real one.
   */
  private def installDriverSeams(): Unit = {
    val locator = new SparkConnectEngineLocator {
      override def locate(engineTag: String): Option[SparkConnectEngineAddress] =
        driverObserver.driverPod(engineTag)
          .filter(_.phase == FakeSparkConnectDriverObserver.POD_PHASE_RUNNING)
          .flatMap(_ => Option(engineAddresses.get(engineTag)))
    }
    sessionManager.replaceSparkConnectDriverSeams(
      locator,
      driverObserver,
      new GrpcSparkConnectEngineProbe(PROBE_TIMEOUT_MILLIS))
  }

  /**
   * A binding exactly as a restart leaves it: written by the instance that died, and naming a
   * Kyuubi session that no instance holds. Written straight to the store, so that this instance
   * meets it the way a restarted one does -- through a cache miss.
   */
  private def bindingLeftByRestart(
      userName: String,
      restartCount: Int = 0,
      generation: Int = 0,
      recoveryState: String = SparkConnectRecoveryState.NONE,
      lastRestartTime: Long = 0L): SparkConnectSessionInfo = {
    // The store outlives a suite run, and a user's lookup takes their newest row: anything an
    // earlier run left for this user would shadow a binding dated an hour back.
    registry.forget(userName)
    val sessionId = UUID.randomUUID().toString
    val binding = SparkConnectSessionInfo(
      userName = userName,
      sessionId = sessionId,
      engineTag = sessionId,
      engineToken = SparkConnect.generateToken(),
      // Long enough ago that no launch of its engine could still be under way.
      createTime = System.currentTimeMillis() - AN_HOUR_MILLIS,
      generation = generation,
      restartCount = restartCount,
      lastRestartTime = lastRestartTime,
      recoveryState = recoveryState)
    sessionManager.metadataManager.getOrElse(fail("the server has no metadata store"))
      .insertSparkConnectSession(binding)
    binding
  }

  /** Put a running driver behind `engineTag`, serving Spark Connect at `address`. */
  private def engineIsRunning(engineTag: String, address: SparkConnectEngineAddress): Unit = {
    driverObserver.available = true
    driverObserver.driverIsRunning(engineTag)
    engineAddresses.put(engineTag, address)
  }

  private def bindingOf(userName: String): SparkConnectSessionInfo =
    registry.lookup(userName).getOrElse(fail(s"$userName has no binding"))

  /** The session a page that shows one session shows: the first the list returns. */
  private def listedFirst(userName: String): Option[String] =
    listSparkConnectSessions(userName).headOption.map(_.getSessionId)

  test("after a restart, a create reattaches to the engine still running, and lists it") {
    val userName = "restart_live_engine"
    val ghost = bindingLeftByRestart(userName, restartCount = 1, generation = 1)
    val engine = new FakeSparkConnectEngine(SESSION_NOT_FOUND_REPLY, Some(ghost.engineToken))
    try {
      engineIsRunning(ghost.engineTag, engine.address)

      val created = openSparkConnectSession(userName)
      try {
        assert(created.getSessionId != ghost.sessionId, "the create handed back the ghost")
        // Reused only after it answered a call made with its own credential.
        assert(engine.callCount == 1)
        val binding = bindingOf(userName)
        assert(binding.sessionId == created.getSessionId)
        // The same driver, so the relay routes to the same pod with the same token, and the
        // client's Spark session on it -- nothing was replaced -- keeps its generation.
        assert(binding.engineTag == ghost.engineTag)
        assert(binding.engineToken == ghost.engineToken)
        assert(binding.generation == 1)
        assert(sessionConf(created.getSessionId)(SESSION_SPARK_CONNECT_TOKEN.key) ==
          ghost.engineToken)
        assert(listedFirst(userName).contains(created.getSessionId))
      } finally {
        closeSparkConnectSession(userName, created.getSessionId)
      }
    } finally {
      engine.stop()
    }
  }

  test("after a restart, a create relaunches the engine that is gone, and lists it") {
    val userName = "restart_dead_engine"
    val ghost = bindingLeftByRestart(userName)
    // The cluster is observable, and no driver pod carries the tag any more.
    driverObserver.available = true

    val created = openSparkConnectSession(userName)
    try {
      assert(created.getSessionId != ghost.sessionId, "the create handed back the ghost")
      val binding = bindingOf(userName)
      assert(binding.sessionId == created.getSessionId)
      // A new driver: tagged by the session that launched it, with a credential of its own.
      assert(binding.engineTag == created.getSessionId)
      assert(binding.engineToken != ghost.engineToken)
      // Counted as the recovery it is: the client's Spark session did not survive.
      assert(binding.generation == 1)
      assert(binding.restartCount == 1)
      assert(listedFirst(userName).contains(created.getSessionId))
    } finally {
      closeSparkConnectSession(userName, created.getSessionId)
    }
  }

  test("a create after a restart relaunches no more engines than recovery allows") {
    val userName = "restart_exhausted_engine"
    val maxAttempts = conf.get(FRONTEND_SPARK_CONNECT_RECOVERY_MAX_ATTEMPTS)
    val ghost = bindingLeftByRestart(userName, restartCount = maxAttempts, generation = maxAttempts)
    driverObserver.available = true

    val created = openSparkConnectSession(userName)
    try {
      assert(created.getSessionId != ghost.sessionId, "the create handed back the ghost")
      val binding = bindingOf(userName)
      // Not one relaunch past the limit: a new session on a fresh engine, as on any session
      // recovery gave up on.
      assert(binding.sessionId == created.getSessionId)
      assert(binding.restartCount == 0)
      assert(binding.generation == 0)
      assert(listedFirst(userName).contains(created.getSessionId))
    } finally {
      closeSparkConnectSession(userName, created.getSessionId)
    }
  }

  test("a RECOVERING flag a restart left behind is cleared, not waited on") {
    val userName = "restart_lost_relaunch"
    val ghost = bindingLeftByRestart(
      userName,
      restartCount = 1,
      generation = 1,
      recoveryState = SparkConnectRecoveryState.RECOVERING,
      lastRestartTime = System.currentTimeMillis() - AN_HOUR_MILLIS)
    driverObserver.available = true

    val created = openSparkConnectSession(userName)
    try {
      assert(created.getSessionId != ghost.sessionId, "the create waited on a lost relaunch")
      val binding = bindingOf(userName)
      assert(!binding.isRecovering)
      assert(binding.sessionId == created.getSessionId)
      // Relaunched once, now; the relaunch that was lost spent nothing.
      assert(binding.restartCount == 2)
      assert(listedFirst(userName).contains(created.getSessionId))
    } finally {
      closeSparkConnectSession(userName, created.getSessionId)
    }
  }

  test("a relaunch another instance may be running is waited for, and listed") {
    val userName = "peer_relaunch"
    val bound = bindingLeftByRestart(
      userName,
      recoveryState = SparkConnectRecoveryState.RECOVERING,
      lastRestartTime = System.currentTimeMillis())
    driverObserver.available = true
    try {
      val answered = openSparkConnectSession(userName)

      // No second driver: whoever is relaunching rebinds the user when it lands.
      assert(answered.getSessionId == bound.sessionId)
      assert(bindingOf(userName).isRecovering)
      // And what the create answered with is on the list rather than missing from it.
      val listed = listSparkConnectSessions(userName)
      assert(listed.map(_.getSessionId) == Seq(bound.sessionId))
      assert(listed.head.getState == STATE_RECOVERING)
    } finally {
      registry.forget(userName)
    }
  }

  test("a live session is handed back unchanged, and only once its engine has answered") {
    val userName = "live_session"
    val first = openSparkConnectSession(userName)
    try {
      val binding = bindingOf(userName)
      val engine = new FakeSparkConnectEngine(SESSION_NOT_FOUND_REPLY, Some(binding.engineToken))
      try {
        engineIsRunning(binding.engineTag, engine.address)

        val second = openSparkConnectSession(userName)

        assert(second.getSessionId == first.getSessionId)
        assert(engine.callCount == 1, "the engine was reused without being probed")
        assert(bindingOf(userName) == binding)
        assert(listedFirst(userName).contains(first.getSessionId))
      } finally {
        engine.stop()
      }
    } finally {
      closeSparkConnectSession(userName, first.getSessionId)
    }
  }

  test("an engine port that hangs does not hang the create") {
    val userName = "hung_engine"
    val bound = bindingLeftByRestart(userName)
    // Accepts connections from the backlog and never answers: a wedged driver, to a client.
    val hungPort = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
    try {
      engineIsRunning(
        bound.engineTag,
        SparkConnectEngineAddress("127.0.0.1", hungPort.getLocalPort))

      val started = System.nanoTime()
      val answered = Await.result(Future(openSparkConnectSession(userName)), 60.seconds)
      val elapsedMillis = (System.nanoTime() - started) / 1000000

      assert(elapsedMillis < PROBE_TIMEOUT_MILLIS + 5000, s"the create took ${elapsedMillis}ms")
      // A pod that runs without answering may be a driver still starting; replacing it would
      // put two drivers behind the user. It is waited for, and listed as what it is.
      assert(answered.getSessionId == bound.sessionId)
      val listed = listSparkConnectSessions(userName)
      assert(listed.map(_.getSessionId) == Seq(bound.sessionId))
      assert(listed.head.getState == STATE_PENDING)
    } finally {
      hungPort.close()
      registry.forget(userName)
    }
  }

  test("a bound session this instance does not hold can be listed, viewed and closed") {
    val userName = "unheld_session"
    val bound = bindingLeftByRestart(userName)
    val engine = new FakeSparkConnectEngine(SESSION_NOT_FOUND_REPLY, Some(bound.engineToken))
    try {
      engineIsRunning(bound.engineTag, engine.address)

      // What a user sees on opening the page straight after a restart, before any create.
      val listed = listSparkConnectSessions(userName)
      assert(listed.map(_.getSessionId) == Seq(bound.sessionId))
      assert(listed.head.getState == STATE_RUNNING)

      val viewed = webTarget.path(s"api/v1/spark-connect/sessions/${bound.sessionId}")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, basicAuthorizationHeader(userName))
        .get()
      assert(200 == viewed.getStatus)
      assert(viewed.readEntity(classOf[SparkConnectSessionData]).getSessionId == bound.sessionId)
      // The page loads the diagnostics of whatever it lists, and none of them may fail.
      Seq("log", "driver", "driver/log", "driver/events").foreach { path =>
        assert(200 == driverRequest(userName, bound.sessionId, path).getStatus, path)
      }
      // Nobody else reaches it by naming it.
      assert(404 == driverRequest("somebody_else", bound.sessionId, "driver").getStatus)
      assert(404 == closeSparkConnectSession("somebody_else", bound.sessionId))

      assert(200 == closeSparkConnectSession(userName, bound.sessionId))
      assert(listSparkConnectSessions(userName).isEmpty)
      assert(!bindingOf(userName).hasLiveSession)
    } finally {
      engine.stop()
      registry.forget(userName)
    }
  }

  test("starting clears the RECOVERING flags a dead instance left, and only stale ones") {
    val lost = bindingLeftByRestart(
      "startup_lost_relaunch",
      recoveryState = SparkConnectRecoveryState.RECOVERING,
      lastRestartTime = System.currentTimeMillis() - AN_HOUR_MILLIS)
    val live = bindingLeftByRestart(
      "startup_live_relaunch",
      recoveryState = SparkConnectRecoveryState.RECOVERING,
      lastRestartTime = System.currentTimeMillis())
    try {
      installDriverSeams()

      val store = sessionManager.metadataManager.getOrElse(fail("no metadata store"))
      assert(store.getSparkConnectSessionByUserName(lost.userName).exists(!_.isRecovering))
      // Young enough that a live peer may be running it: left for the lazy check.
      assert(store.getSparkConnectSessionByUserName(live.userName).exists(_.isRecovering))
    } finally {
      registry.forget(lost.userName)
      registry.forget(live.userName)
    }
  }

  test("a Spark Connect session gets an engine of its user's own") {
    val conf = SparkConnectEngineConf.serverControlledConf("an-engine-credential")
    assert(conf(SESSION_SPARK_CONNECT_ENABLED.key) == "true")
    assert(conf(SESSION_SPARK_CONNECT_TOKEN.key) == "an-engine-credential")
    assert(conf(ENGINE_TYPE.key) == "SPARK_SQL")
    // The engine serves exactly one user, so state that outlives a session -- artifacts, temp
    // views, cached frames -- is that user's own rather than someone else's leaking across.
    assert(conf(ENGINE_SHARE_LEVEL.key) == "USER")
    // Without a subdomain of its own, a Spark Connect session would be handed the same user's
    // ordinary Thrift engine, which was launched without the Spark Connect plugin.
    assert(conf(ENGINE_SHARE_LEVEL_SUBDOMAIN.key) == SparkConnectEngineConf.ENGINE_SUBDOMAIN)
  }

  test("a Spark Connect engine is always launched in cluster mode") {
    // In client mode the driver JVM runs inside the Kyuubi pod, where the engine's Spark Connect
    // server cannot bind the port the gateway's own frontend already holds -- and where there is
    // no driver pod for the engine locator to find.
    val conf = SparkConnectEngineConf.serverControlledConf("an-engine-credential")
    assert(conf(SparkProcessBuilder.DEPLOY_MODE_KEY) == SparkConnectEngineConf.DEPLOY_MODE_CLUSTER)
  }

  test("a client cannot declare its own engine credential, engine sharing or deploy mode") {
    val requested = Map(
      SESSION_SPARK_CONNECT_TOKEN.key -> "a-token-i-chose",
      SESSION_SPARK_CONNECT_ENABLED.key -> "true",
      ENGINE_SHARE_LEVEL.key -> "SERVER",
      ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "somebody-elses-engine",
      ENGINE_TYPE.key -> "FLINK_SQL",
      SparkProcessBuilder.DEPLOY_MODE_KEY -> "client",
      "spark.sql.shuffle.partitions" -> "42")

    val accepted = SparkConnectEngineConf.clientControlledConf(requested)

    assert(!accepted.contains(SESSION_SPARK_CONNECT_TOKEN.key))
    assert(!accepted.contains(SESSION_SPARK_CONNECT_ENABLED.key))
    assert(!accepted.contains(ENGINE_SHARE_LEVEL.key))
    assert(!accepted.contains(ENGINE_SHARE_LEVEL_SUBDOMAIN.key))
    assert(!accepted.contains(ENGINE_TYPE.key))
    assert(!accepted.contains(SparkProcessBuilder.DEPLOY_MODE_KEY))
    // Ordinary Spark conf is still the caller's to set.
    assert(accepted("spark.sql.shuffle.partitions") == "42")
  }

  test("server controlled conf wins over anything the client sent") {
    val requested = Map(
      ENGINE_SHARE_LEVEL.key -> "SERVER",
      ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "somebody-elses-engine",
      SparkProcessBuilder.DEPLOY_MODE_KEY -> "client")
    val effective =
      SparkConnectEngineConf.serverControlledConf("an-engine-credential") ++
        SparkConnectEngineConf.clientControlledConf(requested)
    assert(effective(ENGINE_SHARE_LEVEL.key) == "USER")
    assert(effective(ENGINE_SHARE_LEVEL_SUBDOMAIN.key) == SparkConnectEngineConf.ENGINE_SUBDOMAIN)
    assert(effective(SparkProcessBuilder.DEPLOY_MODE_KEY) ==
      SparkConnectEngineConf.DEPLOY_MODE_CLUSTER)
  }

  test("cluster mode is pinned on Spark Connect sessions and nowhere else") {
    val sparkConnect = openSparkConnectSession("alice")
    val ordinary = openOrdinarySession("alice")
    try {
      assert(sessionConf(sparkConnect.getSessionId)(SparkProcessBuilder.DEPLOY_MODE_KEY) ==
        SparkConnectEngineConf.DEPLOY_MODE_CLUSTER)
      // A Thrift or ordinary REST session keeps whatever deploy mode the deployment configures.
      assert(!sessionConf(ordinary).contains(SparkProcessBuilder.DEPLOY_MODE_KEY))
    } finally {
      closeSparkConnectSession("alice", sparkConnect.getSessionId)
      closeOrdinarySession(ordinary)
    }
  }

  test("the session DTO has no token to leak") {
    val session = new SparkConnectSession("a-session-id", "sc://host:15002")
    assert(session.toString.contains("a-session-id"))
    // Nothing to hand out: the caller authenticates the gRPC port with the credential they
    // already have, and Kyuubi's own engine credential never leaves the gateway.
    assert(!classOf[SparkConnectSession].getMethods.exists(_.getName == "getToken"))
  }

  test("a session is PENDING until its engine reports in") {
    assert(SparkConnectResource.sessionState(openedTime = -1L, endTime = -1L, failed = false) ==
      SparkConnectSessionSupervisor.STATE_PENDING)
    assert(SparkConnectResource.sessionState(openedTime = 1L, endTime = -1L, failed = false) ==
      SparkConnectSessionSupervisor.STATE_RUNNING)
    assert(SparkConnectResource.sessionState(openedTime = 1L, endTime = 2L, failed = false) ==
      SparkConnectSessionSupervisor.STATE_CLOSED)
    // A failed session may well have opened and closed; the failure is what the user needs to see.
    assert(SparkConnectResource.sessionState(openedTime = 1L, endTime = 2L, failed = true) ==
      SparkConnectSessionSupervisor.STATE_FAILED)
  }

  test("only sessions this resource opened are recognised as Spark Connect ones") {
    assert(SparkConnectResource.isSparkConnectSession(
      Map(SESSION_SPARK_CONNECT_ENABLED.key -> "true")))
    assert(!SparkConnectResource.isSparkConnectSession(
      Map(SESSION_SPARK_CONNECT_ENABLED.key -> "false")))
    // An ordinary Thrift or REST session lives in the same session manager and must not show up.
    assert(!SparkConnectResource.isSparkConnectSession(Map("spark.sql.shuffle.partitions" -> "42")))
  }

  test("the session list only shows the caller their own sessions") {
    val alice = openSparkConnectSession("alice")
    val bob = openSparkConnectSession("bob")
    try {
      val aliceSessions = listSparkConnectSessions("alice")
      assert(aliceSessions.map(_.getSessionId) == Seq(alice.getSessionId))
      assert(aliceSessions.map(_.getUser) == Seq("alice"))

      val bobSessions = listSparkConnectSessions("bob")
      assert(bobSessions.map(_.getSessionId) == Seq(bob.getSessionId))

      // Somebody with no sessions of their own sees an empty list, not everyone else's.
      assert(listSparkConnectSessions("carol").isEmpty)
    } finally {
      closeSparkConnectSession("alice", alice.getSessionId)
      closeSparkConnectSession("bob", bob.getSessionId)
    }
  }

  test("creating a session twice gives the caller the one session they have") {
    val first = openSparkConnectSession("alice")
    try {
      val second = openSparkConnectSession("alice")

      // A second session would be unreachable: the gRPC port routes on who is calling, not on
      // which session id they meant.
      assert(second.getSessionId == first.getSessionId)
      assert(listSparkConnectSessions("alice").map(_.getSessionId) == Seq(first.getSessionId))
    } finally {
      closeSparkConnectSession("alice", first.getSessionId)
    }
  }

  test("two users each get their own session") {
    val alice = openSparkConnectSession("alice")
    val bob = openSparkConnectSession("bob")
    try {
      assert(alice.getSessionId != bob.getSessionId)
    } finally {
      closeSparkConnectSession("alice", alice.getSessionId)
      closeSparkConnectSession("bob", bob.getSessionId)
    }
  }

  test("neither the create response nor the session list carries a token") {
    val alice = openSparkConnectSession("alice")
    try {
      val listBody = webTarget.path("api/v1/spark-connect/sessions")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("alice"))
        .get()
        .readEntity(classOf[String])

      // Asserted against the raw body rather than the DTO, because a credential could only leak
      // through a field the DTO does not model -- which a typed read is exactly what would hide.
      assert(!listBody.toLowerCase.contains("token"))
      assert(listBody.contains(alice.getSessionId))

      val createBody = webTarget.path("api/v1/spark-connect/sessions")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("bob"))
        .post(Entity.entity(
          new SessionOpenRequest(Collections.emptyMap[String, String]()),
          MediaType.APPLICATION_JSON_TYPE))
        .readEntity(classOf[String])
      try {
        assert(!createBody.toLowerCase.contains("token"))
      } finally {
        listSparkConnectSessions("bob").foreach(session =>
          closeSparkConnectSession("bob", session.getSessionId))
      }
    } finally {
      closeSparkConnectSession("alice", alice.getSessionId)
    }
  }

  test("the submit log endpoint serves the caller their own session's log") {
    val alice = openSparkConnectSession("alice")
    try {
      val log = submitLogWhenWritten("alice", alice.getSessionId)
      // The launch operation announces the engine it is opening for the user it belongs to, so
      // this is the caller's own submit log and not some other session's.
      assert(
        log.exists(_.contains("alice")),
        s"the submit log does not look like alice's: $log")
    } finally {
      closeSparkConnectSession("alice", alice.getSessionId)
    }
  }

  test("the submit log is paged rather than returned whole") {
    val alice = openSparkConnectSession("alice")
    try {
      val whole = submitLogWhenWritten("alice", alice.getSessionId)
      assume(whole.size > 1, "the launch had written only one line to page through")
      val firstLine = submitLogPage("alice", alice.getSessionId, from = 0, size = 1)
      assert(firstLine == whole.take(1))
      // `from` is an offset into the log, not a hint: a UI paging forward must not be handed
      // the top of the file again.
      assert(submitLogPage("alice", alice.getSessionId, from = 1, size = 1) == whole.slice(1, 2))
    } finally {
      closeSparkConnectSession("alice", alice.getSessionId)
    }
  }

  /** The whole submit log, once the launch has written to it. */
  private def submitLogWhenWritten(user: String, sessionId: String): Seq[String] = {
    var log = Seq.empty[String]
    eventually(timeout(30.seconds), interval(200.milliseconds)) {
      log = submitLogPage(user, sessionId, from = 0, size = 1000)
      assert(log.nonEmpty, "the launch has not written a submit log yet")
    }
    log
  }

  private def submitLogPage(
      user: String,
      sessionId: String,
      from: Int,
      size: Int): Seq[String] = {
    val response = webTarget.path(s"api/v1/spark-connect/sessions/$sessionId/log")
      .queryParam("from", from.toString)
      .queryParam("size", size.toString)
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader(user))
      .get()
    assert(200 == response.getStatus)
    val log = response.readEntity(classOf[OperationLog])
    assert(log.getRowCount == log.getLogRowSet.size)
    log.getLogRowSet.asScala
  }

  test("the driver endpoints degrade honestly with no Kubernetes client") {
    val alice = openSparkConnectSession("alice")
    try {
      val driver = driverRequest("alice", alice.getSessionId, "driver")
        .readEntity(classOf[SparkConnectDriverInfo])
      assert(!driver.getAvailable)
      assert(driver.getMessage == SparkConnectResource.NO_KUBERNETES_CLIENT_MESSAGE)
      // Not an empty object that reads like a healthy driver with no containers.
      assert(driver.getPodName == null)
      assert(driver.getContainers.isEmpty)
      assert(driver.getSessionId == alice.getSessionId)

      val events = driverRequest("alice", alice.getSessionId, "driver/events")
        .readEntity(classOf[SparkConnectDriverEvents])
      assert(!events.getAvailable)
      assert(events.getMessage == SparkConnectResource.NO_KUBERNETES_CLIENT_MESSAGE)
      assert(events.getEvents.isEmpty)

      val driverLog = driverRequest("alice", alice.getSessionId, "driver/log")
        .readEntity(classOf[OperationLog])
      assert(driverLog.getLogRowSet.asScala ==
        Seq(SparkConnectResource.NO_KUBERNETES_CLIENT_MESSAGE))
    } finally {
      closeSparkConnectSession("alice", alice.getSessionId)
    }
  }

  test("nobody can read another user's driver diagnostics") {
    val alice = openSparkConnectSession("alice")
    try {
      Seq("log", "driver", "driver/log", "driver/events").foreach { path =>
        val response = driverRequest("bob", alice.getSessionId, path)
        assert(
          403 == response.getStatus,
          s"bob must not reach alice's $path")
      }
    } finally {
      closeSparkConnectSession("alice", alice.getSessionId)
    }
  }

  test("the driver endpoints reject a session id that is not the caller's to name") {
    val alice = openSparkConnectSession("alice")
    try {
      assert(400 == driverRequest("alice", "not-a-uuid", "driver").getStatus)
      assert(404 ==
        driverRequest("alice", UUID.randomUUID().toString, "driver").getStatus)
      // An ordinary session of the caller's own is still not a Spark Connect one.
      val ordinary = openOrdinarySession("alice")
      try {
        assert(404 == driverRequest("alice", ordinary, "driver").getStatus)
      } finally {
        closeOrdinarySession(ordinary)
      }
    } finally {
      closeSparkConnectSession("alice", alice.getSessionId)
    }
  }

  private def driverRequest(user: String, sessionId: String, path: String): Response =
    webTarget.path(s"api/v1/spark-connect/sessions/$sessionId/$path")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader(user))
      .get()

  private def sessionConf(sessionId: String): Map[String, String] =
    server.backendService.sessionManager
      .getSessionOption(SessionHandle.fromUUID(sessionId))
      .map(_.conf)
      .getOrElse(fail(s"session $sessionId is not open"))

  private def openOrdinarySession(user: String): String = {
    val response = webTarget.path("api/v1/sessions")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader(user))
      .post(Entity.entity(
        new SessionOpenRequest(Collections.emptyMap[String, String]()),
        MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    response.readEntity(classOf[dto.SessionHandle]).getIdentifier.toString
  }

  private def closeOrdinarySession(sessionId: String): Unit = {
    webTarget.path(s"api/v1/sessions/$sessionId")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("alice"))
      .delete()
  }

  private def openSparkConnectSession(user: String): SparkConnectSession = {
    val request = new SessionOpenRequest(Collections.emptyMap[String, String]())
    val response = webTarget.path("api/v1/spark-connect/sessions")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader(user))
      .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    response.readEntity(classOf[SparkConnectSession])
  }

  private def listSparkConnectSessions(user: String): Seq[SparkConnectSessionData] = {
    val response = webTarget.path("api/v1/spark-connect/sessions")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader(user))
      .get()
    assert(200 == response.getStatus)
    response.readEntity(new GenericType[Seq[SparkConnectSessionData]]() {})
  }

  private def closeSparkConnectSession(user: String, sessionId: String): Int =
    webTarget.path(s"api/v1/spark-connect/sessions/$sessionId")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader(user))
      .delete()
      .getStatus
}

object SparkConnectResourceSuite {

  /**
   * Long enough for a first gRPC connection on loopback in a cold JVM, short enough that the test
   * of a port that hangs costs the suite seconds rather than minutes.
   */
  private val PROBE_TIMEOUT_MILLIS = 2000L

  private val AN_HOUR_MILLIS = 3600000L
}
