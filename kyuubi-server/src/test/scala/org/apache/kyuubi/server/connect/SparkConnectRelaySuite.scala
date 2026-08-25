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

import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.{Callable, Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable

import io.grpc.{ManagedChannel, Metadata, Server, Status}
import io.grpc.netty.NettyServerBuilder

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.server.connect.SparkConnectTestHelper._

/**
 * End-to-end coverage of the proxy over real gRPC: a client, the frontend's relay, and a stand-in
 * engine, each on their own socket. No Spark and no Kubernetes are involved -- engine discovery is
 * replaced by a locator the test drives directly, and the deployment's bearer provider by one that
 * knows a fixed handful of credentials.
 */
class SparkConnectRelaySuite extends KyuubiFunSuite {

  private val aliceCredential = "alice-platform-credential"
  private val bobCredential = "bob-platform-credential"

  private val aliceSessionId = UUID.randomUUID().toString
  private val bobSessionId = UUID.randomUUID().toString
  private val aliceEngineToken = SparkConnect.generateToken()
  private val bobEngineToken = SparkConnect.generateToken()

  private var aliceEngine: FakeSparkConnectEngine = _
  private var bobEngine: FakeSparkConnectEngine = _
  private var proxyServer: Server = _
  private var clientChannelToProxy: ManagedChannel = _
  private var registry: SparkConnectSessionRegistry = _
  private var channelPool: SparkConnectEngineChannelPool = _
  private var authenticationProvider: FakeTokenAuthenticationProvider = _
  private var authenticator: SparkConnectAuthenticator = _

  /** Set per test to decide how an engine answers. */
  private var engineReply: (String, Seq[Array[Byte]]) => FakeEngineReply = _

  /** Set per test to decide whether Alice's engine is discoverable. */
  private var aliceEngineIsServing: Boolean = true

  override def beforeEach(): Unit = {
    super.beforeEach()
    engineReply = (_, requests) => FakeEngineReply(responses = requests)
    aliceEngine = new FakeSparkConnectEngine((method, requests) => engineReply(method, requests))
    bobEngine = new FakeSparkConnectEngine((method, requests) => engineReply(method, requests))
    aliceEngineIsServing = true

    registry = new SparkConnectSessionRegistry(None)
    registry.register("alice", aliceSessionId, aliceSessionId, aliceEngineToken)
    registry.register("bob", bobSessionId, bobSessionId, bobEngineToken)

    authenticationProvider = new FakeTokenAuthenticationProvider(
      Map(aliceCredential -> "alice", bobCredential -> "bob"))
    authenticator = authenticatorFor(authenticationProvider)

    channelPool = new SparkConnectEngineChannelPool(
      SparkConnectEngineChannelConf(
        maxInboundMessageSize = 4 * 1024 * 1024,
        keepAliveTimeMillis = 60000,
        keepAliveTimeoutMillis = 20000))

    val locator = new SparkConnectEngineLocator {
      override def locate(tag: String): Option[SparkConnectEngineAddress] = tag match {
        case `aliceSessionId` if aliceEngineIsServing => Some(aliceEngine.address)
        case `bobSessionId` => Some(bobEngine.address)
        case _ => None
      }
    }

    val relay = new SparkConnectRelay(authenticator, registry, locator, channelPool)
    proxyServer = NettyServerBuilder.forPort(0)
      .fallbackHandlerRegistry(new SparkConnectHandlerRegistry(relay))
      .build()
      .start()
    clientChannelToProxy = clientChannel(proxyServer.getPort)
  }

  override def afterEach(): Unit = {
    if (clientChannelToProxy != null) clientChannelToProxy.shutdownNow()
    if (channelPool != null) channelPool.shutdown()
    if (proxyServer != null) {
      proxyServer.shutdownNow()
      proxyServer.awaitTermination(5, TimeUnit.SECONDS)
    }
    if (aliceEngine != null) aliceEngine.stop()
    if (bobEngine != null) bobEngine.stop()
    super.afterEach()
  }

  private def bytes(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

  private def text(value: Array[Byte]): String = new String(value, StandardCharsets.UTF_8)

  test("an authenticated user's call reaches their own engine") {
    val result = call(
      clientChannelToProxy,
      EXECUTE_PLAN_METHOD,
      Seq(bytes("a plan")),
      bearerHeaders(aliceCredential))

    assert(result.status.getCode == Status.Code.OK)
    assert(result.responses.map(text) == Seq("a plan"))
    assert(aliceEngine.callCount == 1)
    assert(bobEngine.callCount == 0)
  }

  test("two users issuing the same call each reach their own engine") {
    // Both send an identical payload and name the same Spark Connect session id inside it. The
    // frontend never opens the payload, so the only thing separating them is who they are.
    val sharedPayload = Seq(bytes("session_id: the-same-name"))
    call(clientChannelToProxy, EXECUTE_PLAN_METHOD, sharedPayload, bearerHeaders(aliceCredential))
    call(clientChannelToProxy, EXECUTE_PLAN_METHOD, sharedPayload, bearerHeaders(bobCredential))

    assert(aliceEngine.callCount == 1)
    assert(bobEngine.callCount == 1)
    assert(aliceEngine.receivedHeaders.get(SparkConnect.AUTHORIZATION_HEADER) ==
      s"Bearer $aliceEngineToken")
    assert(bobEngine.receivedHeaders.get(SparkConnect.AUTHORIZATION_HEADER) ==
      s"Bearer $bobEngineToken")
  }

  test("the upstream hop carries Kyuubi's engine credential, not the caller's") {
    call(
      clientChannelToProxy,
      EXECUTE_PLAN_METHOD,
      Seq(bytes("a plan")),
      bearerHeaders(
        aliceCredential,
        Map("cookie" -> "session=platform-secret", "x-trace-id" -> "trace-1")))

    val upstream = aliceEngine.receivedHeaders
    assert(upstream.get(SparkConnect.AUTHORIZATION_HEADER) == s"Bearer $aliceEngineToken")
    // The whole point: a long-lived platform credential must not reach a JVM the user controls.
    assert(!upstream.get(SparkConnect.AUTHORIZATION_HEADER).contains(aliceCredential))
    assert(upstream.get(Metadata.Key.of("cookie", Metadata.ASCII_STRING_MARSHALLER)) == null)
    // Headers the proxy knows nothing about still reach the engine; that is what lets it stay
    // ignorant of the Spark Connect protocol.
    assert(
      upstream.get(Metadata.Key.of("x-trace-id", Metadata.ASCII_STRING_MARSHALLER)) == "trace-1")
  }

  test("a call with no bearer credential is rejected with a trailers-only UNAUTHENTICATED") {
    val result =
      call(clientChannelToProxy, EXECUTE_PLAN_METHOD, Seq(bytes("a plan")), new Metadata())

    assert(result.status.getCode == Status.Code.UNAUTHENTICATED)
    assert(result.isTrailersOnly)
    assert(aliceEngine.callCount == 0)
  }

  test("a credential the provider does not know is rejected with UNAUTHENTICATED") {
    val result = call(
      clientChannelToProxy,
      EXECUTE_PLAN_METHOD,
      Seq(bytes("a plan")),
      bearerHeaders("a-credential-from-nowhere"))

    assert(result.status.getCode == Status.Code.UNAUTHENTICATED)
    assert(result.isTrailersOnly)
    assert(aliceEngine.callCount == 0)
    assert(bobEngine.callCount == 0)
    // A rejected credential must not become a cache entry anyone can plant.
    assert(authenticator.cachedPrincipalCount == 0)
  }

  test("an authenticated user with no session is told how to create one") {
    registry.forget("alice")

    val result = call(
      clientChannelToProxy,
      EXECUTE_PLAN_METHOD,
      Seq(bytes("a plan")),
      bearerHeaders(aliceCredential))

    // Deliberately not UNAVAILABLE: a Spark Connect client retries that for minutes, and no
    // amount of retrying creates a session. The user has to read the message.
    assert(result.status.getCode == Status.Code.FAILED_PRECONDITION)
    assert(result.status.getDescription.contains("/api/v1/spark-connect/sessions"))
    assert(result.status.getDescription.contains("web UI"))
    assert(aliceEngine.callCount == 0)
  }

  test("a user whose session has closed is told to create one, not routed to the stale engine") {
    registry.unregister(aliceSessionId)

    val result = call(
      clientChannelToProxy,
      EXECUTE_PLAN_METHOD,
      Seq(bytes("a plan")),
      bearerHeaders(aliceCredential))

    assert(result.status.getCode == Status.Code.FAILED_PRECONDITION)
    assert(aliceEngine.callCount == 0)
  }

  test("an engine that is not serving yet yields a trailers-only UNAVAILABLE") {
    aliceEngineIsServing = false

    val result = call(
      clientChannelToProxy,
      EXECUTE_PLAN_METHOD,
      Seq(bytes("a plan")),
      bearerHeaders(aliceCredential))

    // UNAVAILABLE is what makes a slow engine start look like a slow connect to the client: Spark
    // Connect's default retry policy backs off and retries for minutes on this code.
    assert(result.status.getCode == Status.Code.UNAVAILABLE)
    assert(result.isTrailersOnly)
    assert(aliceEngine.callCount == 0)
  }

  test("a credential is resolved once, not on every relayed call") {
    // PySpark issues a ReleaseExecute after every response batch, so a provider round trip per
    // RPC would put the deployment's identity service on the hot path of every query.
    (1 to 20).foreach { index =>
      call(
        clientChannelToProxy,
        EXECUTE_PLAN_METHOD,
        Seq(bytes(s"plan-$index")),
        bearerHeaders(aliceCredential))
    }

    assert(aliceEngine.callCount == 20)
    assert(authenticationProvider.callCount == 1)
  }

  test("an engine error is relayed with its status, message and trailers intact") {
    val engineTrailers = new Metadata()
    engineTrailers.put(
      Metadata.Key.of("x-spark-error-class", Metadata.ASCII_STRING_MARSHALLER),
      "AnalysisException")
    engineReply = (_, _) =>
      FakeEngineReply(
        status = Status.INTERNAL.withDescription("TABLE_OR_VIEW_NOT_FOUND"),
        trailers = engineTrailers)

    val result = call(
      clientChannelToProxy,
      EXECUTE_PLAN_METHOD,
      Seq(bytes("a plan")),
      bearerHeaders(aliceCredential))

    assert(result.status.getCode == Status.Code.INTERNAL)
    assert(result.status.getDescription == "TABLE_OR_VIEW_NOT_FOUND")
    assert(result.trailers.get(
      Metadata.Key.of("x-spark-error-class", Metadata.ASCII_STRING_MARSHALLER)) ==
      "AnalysisException")
  }

  test("a streamed response is relayed message by message") {
    val batches = (1 to 20).map(index => bytes(s"batch-$index"))
    engineReply = (_, _) => FakeEngineReply(responses = batches)

    val result = call(
      clientChannelToProxy,
      EXECUTE_PLAN_METHOD,
      Seq(bytes("a plan")),
      bearerHeaders(aliceCredential))

    assert(result.status.getCode == Status.Code.OK)
    assert(result.responses.map(text) == batches.map(text))
  }

  test("concurrent calls for one session share a single upstream connection") {
    val methods = Seq(EXECUTE_PLAN_METHOD, RELEASE_EXECUTE_METHOD)
    val executor = Executors.newFixedThreadPool(8)
    try {
      val tasks = (1 to 16).map { index =>
        new Callable[ProxyCallResult] {
          override def call(): ProxyCallResult = SparkConnectTestHelper.call(
            clientChannelToProxy,
            methods(index % methods.size),
            Seq(bytes(s"request-$index")),
            bearerHeaders(aliceCredential))
        }
      }
      val results = executor.invokeAll(tasks.asJava).asScala.map(_.get(60, TimeUnit.SECONDS))
      assert(results.forall(_.status.getCode == Status.Code.OK))
      assert(results.flatMap(_.responses).map(text).toSet ==
        (1 to 16).map(index => s"request-$index").toSet)
    } finally {
      executor.shutdownNow()
    }

    assert(aliceEngine.callCount == 16)
    // gRPC multiplexes streams, so sixteen concurrent RPCs need exactly one connection.
    assert(channelPool.pooledSessionCount == 1)
  }

  test("every Spark Connect RPC is relayed without being named individually") {
    // Enumerating RPCs is how a proxy like this breaks ReleaseExecute, which PySpark issues after
    // every response batch. Nothing here is registered per method, so a method the frontend has
    // never heard of works too.
    val seen = mutable.Set[String]()
    engineReply = (method, requests) => {
      seen += method
      FakeEngineReply(responses = requests)
    }
    val rpcs = Seq(
      "ExecutePlan",
      "AnalyzePlan",
      "Config",
      "AddArtifacts",
      "ArtifactStatus",
      "Interrupt",
      "ReattachExecute",
      "ReleaseExecute",
      "ReleaseSession",
      "FetchErrorDetails",
      "CloneSession",
      "SomeRpcThatDoesNotExistYet")

    rpcs.foreach { rpc =>
      val result = call(
        clientChannelToProxy,
        SparkConnect.SERVICE_PATH_PREFIX + rpc,
        Seq(bytes(rpc)),
        bearerHeaders(aliceCredential))
      assert(result.status.getCode == Status.Code.OK, s"$rpc was not relayed")
      assert(result.responses.map(text) == Seq(rpc))
    }
    assert(seen.size == rpcs.size)
  }

  test("a path outside the Spark Connect service is not claimed by the frontend") {
    val result = call(
      clientChannelToProxy,
      "some.other.Service/Method",
      Seq(bytes("payload")),
      bearerHeaders(aliceCredential))

    assert(result.status.getCode == Status.Code.UNIMPLEMENTED)
    assert(aliceEngine.callCount == 0)
  }

  test("closing a session tears down its upstream connection") {
    call(
      clientChannelToProxy,
      EXECUTE_PLAN_METHOD,
      Seq(bytes("a plan")),
      bearerHeaders(aliceCredential))
    assert(channelPool.pooledSessionCount == 1)

    registry.onSessionClosed(channelPool.release)
    registry.unregister(aliceSessionId)

    assert(channelPool.pooledSessionCount == 0)
  }
}
