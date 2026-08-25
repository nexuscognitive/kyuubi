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
 * replaced by a locator the test drives directly.
 */
class SparkConnectRelaySuite extends KyuubiFunSuite {

  private val engineTag = UUID.randomUUID().toString
  private val sessionId = engineTag

  private var engine: FakeSparkConnectEngine = _
  private var proxyServer: Server = _
  private var clientChannelToProxy: ManagedChannel = _
  private var registry: SparkConnectSessionRegistry = _
  private var channelPool: SparkConnectEngineChannelPool = _
  private var token: String = _

  /** Set per test to decide how the engine answers. */
  private var engineReply: (String, Seq[Array[Byte]]) => FakeEngineReply = _

  /** Set per test to decide whether the engine is discoverable. */
  private var locatedAddress: Option[SparkConnectEngineAddress] = None

  override def beforeEach(): Unit = {
    super.beforeEach()
    engineReply = (_, requests) => FakeEngineReply(responses = requests)
    engine = new FakeSparkConnectEngine((method, requests) => engineReply(method, requests))
    locatedAddress = Some(engine.address)

    registry = new SparkConnectSessionRegistry(None)
    token = SparkConnect.generateToken()
    registry.register(token, sessionId, "connect_user", engineTag)

    channelPool = new SparkConnectEngineChannelPool(
      SparkConnectEngineChannelConf(
        maxInboundMessageSize = 4 * 1024 * 1024,
        keepAliveTimeMillis = 60000,
        keepAliveTimeoutMillis = 20000))

    val locator = new SparkConnectEngineLocator {
      override def locate(tag: String): Option[SparkConnectEngineAddress] =
        if (tag == engineTag) locatedAddress else None
    }

    val relay = new SparkConnectRelay(registry, locator, channelPool)
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
    if (engine != null) engine.stop()
    super.afterEach()
  }

  private def bytes(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

  private def text(value: Array[Byte]): String = new String(value, StandardCharsets.UTF_8)

  test("relay forwards a call to the engine and returns its response verbatim") {
    val result = call(
      clientChannelToProxy,
      EXECUTE_PLAN_METHOD,
      Seq(bytes("a plan")),
      bearerHeaders(token))

    assert(result.status.getCode == Status.Code.OK)
    assert(result.responses.map(text) == Seq("a plan"))
    assert(engine.callCount == 1)
  }

  test("relay presents its own token upstream and drops the caller's credential") {
    call(
      clientChannelToProxy,
      EXECUTE_PLAN_METHOD,
      Seq(bytes("a plan")),
      bearerHeaders(token, Map("cookie" -> "session=platform-secret", "x-trace-id" -> "trace-1")))

    val upstream = engine.receivedHeaders
    assert(upstream.get(SparkConnect.AUTHORIZATION_HEADER) == s"Bearer $token")
    assert(upstream.get(Metadata.Key.of("cookie", Metadata.ASCII_STRING_MARSHALLER)) == null)
    // Headers the proxy knows nothing about still reach the engine; that is what lets it stay
    // ignorant of the Spark Connect protocol.
    assert(
      upstream.get(Metadata.Key.of("x-trace-id", Metadata.ASCII_STRING_MARSHALLER)) == "trace-1")
  }

  test("a call with no bearer token is rejected with a trailers-only UNAUTHENTICATED") {
    val result =
      call(clientChannelToProxy, EXECUTE_PLAN_METHOD, Seq(bytes("a plan")), new Metadata())

    assert(result.status.getCode == Status.Code.UNAUTHENTICATED)
    assert(result.isTrailersOnly)
    assert(engine.callCount == 0)
  }

  test("an unknown token is rejected with a trailers-only UNAUTHENTICATED") {
    val result = call(
      clientChannelToProxy,
      EXECUTE_PLAN_METHOD,
      Seq(bytes("a plan")),
      bearerHeaders(SparkConnect.generateToken()))

    assert(result.status.getCode == Status.Code.UNAUTHENTICATED)
    assert(result.isTrailersOnly)
    assert(engine.callCount == 0)
    // An unknown token must not be admitted to the cache, or the port becomes a memory leak.
    assert(registry.cachedSessionCount == 1)
  }

  test("an engine that is not serving yet yields a trailers-only UNAVAILABLE") {
    locatedAddress = None

    val result = call(
      clientChannelToProxy,
      EXECUTE_PLAN_METHOD,
      Seq(bytes("a plan")),
      bearerHeaders(token))

    // UNAVAILABLE is what makes a slow engine start look like a slow connect to the client: Spark
    // Connect's default retry policy backs off and retries for minutes on this code.
    assert(result.status.getCode == Status.Code.UNAVAILABLE)
    assert(result.isTrailersOnly)
    assert(engine.callCount == 0)
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
      bearerHeaders(token))

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
      bearerHeaders(token))

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
            bearerHeaders(token))
        }
      }
      val results = executor.invokeAll(tasks.asJava).asScala.map(_.get(60, TimeUnit.SECONDS))
      assert(results.forall(_.status.getCode == Status.Code.OK))
      assert(results.flatMap(_.responses).map(text).toSet ==
        (1 to 16).map(index => s"request-$index").toSet)
    } finally {
      executor.shutdownNow()
    }

    assert(engine.callCount == 16)
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
        bearerHeaders(token))
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
      bearerHeaders(token))

    assert(result.status.getCode == Status.Code.UNIMPLEMENTED)
    assert(engine.callCount == 0)
  }

  test("closing a session tears down its upstream connection") {
    call(clientChannelToProxy, EXECUTE_PLAN_METHOD, Seq(bytes("a plan")), bearerHeaders(token))
    assert(channelPool.pooledSessionCount == 1)

    registry.onSessionClosed(channelPool.release)
    registry.unregister(sessionId)

    assert(channelPool.pooledSessionCount == 0)
    // The token no longer routes, so a client that kept using it is told so rather than reaching
    // whatever now sits at the old address.
    val result =
      call(clientChannelToProxy, EXECUTE_PLAN_METHOD, Seq(bytes("a plan")), bearerHeaders(token))
    assert(result.status.getCode == Status.Code.UNAUTHENTICATED)
  }
}
