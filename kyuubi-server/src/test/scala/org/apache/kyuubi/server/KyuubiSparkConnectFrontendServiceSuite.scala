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

package org.apache.kyuubi.server

import java.io.FileOutputStream
import java.net.InetSocketAddress
import java.nio.file.Path
import java.security.KeyStore
import java.util.concurrent.TimeUnit

import io.grpc.{ManagedChannel, Metadata, Server, Status}
import io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import io.netty.handler.ssl.util.{InsecureTrustManagerFactory, SelfSignedCertificate}

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.server.connect._
import org.apache.kyuubi.server.connect.SparkConnectTestHelper._

class KyuubiSparkConnectFrontendServiceSuite extends KyuubiFunSuite {

  private val keystorePassword = "kyuubi-test"

  private def baseConf: KyuubiConf = KyuubiConf(loadSysDefault = false)
    .set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.REST.toString))
    .set(FRONTEND_SPARK_CONNECT_ENABLED, true)
    .set(FRONTEND_SPARK_CONNECT_SSL_ENABLED, true)
    .set(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH, "/tmp/spark-connect-keystore.jks")

  test("a fully configured Spark Connect frontend passes validation") {
    KyuubiSparkConnectFrontendService.validateConf(baseConf)
  }

  test("TLS is on unless an operator turns it off") {
    // The opt-out has to be something the operator wrote down. If the default were false, silence
    // and consent would look identical and a plaintext listener could appear by accident.
    assert(KyuubiConf(loadSysDefault = false).get(FRONTEND_SPARK_CONNECT_SSL_ENABLED))
  }

  test("the frontend refuses to start without a keystore") {
    val conf = baseConf.unset(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH)
    val e = intercept[KyuubiException](KyuubiSparkConnectFrontendService.validateConf(conf))
    assert(e.getMessage.contains(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH.key))
  }

  test("turning TLS off drops the keystore requirement") {
    // A keystore nothing validates is a credential to rotate and a startup dependency that can
    // fail, so an operator terminating TLS at an ingress must not be made to mint one.
    val conf = baseConf
      .set(FRONTEND_SPARK_CONNECT_SSL_ENABLED, false)
      .unset(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH)
    KyuubiSparkConnectFrontendService.validateConf(conf)
  }

  test("the frontend refuses to start without the REST frontend") {
    // Spark Connect has no open-session RPC, so without REST there is no way to create a session
    // and every token presented on the gRPC port would be unroutable.
    val conf = baseConf.set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.THRIFT_BINARY.toString))
    val e = intercept[KyuubiException](KyuubiSparkConnectFrontendService.validateConf(conf))
    assert(e.getMessage.contains("REST"))
  }

  test("building an SSL context fails clearly when the keystore is absent") {
    val conf = baseConf.set(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH, "/nonexistent/keystore.jks")
    val e = intercept[KyuubiException](KyuubiSparkConnectFrontendService.buildSslContext(conf))
    assert(e.getMessage.contains("/nonexistent/keystore.jks"))
  }

  test("the plaintext warning names both ways it can go wrong") {
    val warning = KyuubiSparkConnectFrontendService.PLAINTEXT_LISTENER_WARNING
    assert(warning.contains(FRONTEND_SPARK_CONNECT_SSL_ENABLED.key))
    assert(warning.contains(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH.key))
    assert(warning.contains("PLAINTEXT"))
    // The trap this warning exists for: a direct client fails the handshake instead of falling
    // back, and the error it prints says nothing about this setting.
    assert(warning.contains("DIRECTLY"))
    assert(warning.contains("handshake"))
    assert(warning.contains("terminates TLS"))
  }

  test("a plaintext listener serves gRPC when TLS is turned off") {
    val conf = baseConf.set(FRONTEND_SPARK_CONNECT_SSL_ENABLED, false)
    withFrontendTransport(conf) { port =>
      withChannel(plaintextChannel(port)) { channel =>
        // A tokenless call is rejected by the relay, which means the whole plaintext path --
        // HTTP/2 framing, method dispatch, trailers -- ran to completion.
        val result = call(channel, EXECUTE_PLAN_METHOD, Seq(Array[Byte](1)), new Metadata)
        assert(result.status.getCode == Status.Code.UNAUTHENTICATED)
        assert(result.isTrailersOnly)
      }
    }
  }

  test("a TLS listener still refuses plaintext clients and serves TLS ones") {
    withKeystore { keystore =>
      val conf = baseConf.set(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH, keystore.toString)
        .set(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PASSWORD, keystorePassword)
      withFrontendTransport(conf) { port =>
        withChannel(tlsChannel(port)) { channel =>
          val result =
            call(channel, EXECUTE_PLAN_METHOD, Seq(Array[Byte](1)), new Metadata)
          assert(result.status.getCode == Status.Code.UNAUTHENTICATED)
        }
        withChannel(plaintextChannel(port)) { channel =>
          val result =
            call(channel, EXECUTE_PLAN_METHOD, Seq(Array[Byte](1)), new Metadata)
          // A plaintext client against a TLS port never gets as far as a gRPC status of its own.
          assert(result.status.getCode != Status.Code.UNAUTHENTICATED)
        }
      }
    }
  }

  test("SPARK_CONNECT is a recognised frontend protocol") {
    val conf = KyuubiConf(loadSysDefault = false)
      .set(FRONTEND_PROTOCOLS, Seq("REST", "SPARK_CONNECT"))
    assert(conf.get(FRONTEND_PROTOCOLS).contains(FrontendProtocols.SPARK_CONNECT.toString))
  }

  test("Spark Connect config entries are server-side and default to off") {
    val conf = KyuubiConf(loadSysDefault = false)
    assert(!conf.get(FRONTEND_SPARK_CONNECT_ENABLED))
    assert(conf.get(FRONTEND_SPARK_CONNECT_BIND_PORT) == 15002)
    assert(conf.get(FRONTEND_SPARK_CONNECT_ENGINE_PORT) == 15002)
    assert(conf.get(FRONTEND_SPARK_CONNECT_MAX_MESSAGE_SIZE) == 128 * 1024 * 1024)
    assert(conf.get(FRONTEND_SPARK_CONNECT_ENGINE_MAX_MESSAGE_SIZE) == 128 * 1024 * 1024)

    // The token must never reach the engine as a Spark conf; the SERVER audience is what keeps it
    // out of the driver command line and the Spark UI environment page.
    conf.set(SESSION_SPARK_CONNECT_TOKEN, "a-secret-token")
    conf.set(SESSION_SPARK_CONNECT_ENABLED, true)
    val engineConf = conf.getEngineConf(org.apache.kyuubi.engine.EngineType.SPARK_SQL)
    assert(!engineConf.contains(SESSION_SPARK_CONNECT_TOKEN.key))
    assert(!engineConf.contains(SESSION_SPARK_CONNECT_ENABLED.key))
  }

  /**
   * Run a real gRPC server whose transport is configured exactly as the frontend configures its
   * own, and hand the caller the port it landed on.
   *
   * The frontend's own relay is behind it, so a call that comes back with a gRPC status has
   * genuinely crossed the transport rather than merely proved that a builder could be built.
   */
  private def withFrontendTransport(conf: KyuubiConf)(f: Int => Unit): Unit = {
    val relay = new SparkConnectRelay(
      new SparkConnectSessionRegistry(None),
      new SparkConnectEngineLocator {
        override def locate(engineTag: String): Option[SparkConnectEngineAddress] = None
      },
      new SparkConnectEngineChannelPool(
        SparkConnectEngineChannelConf(
          maxInboundMessageSize = 4 * 1024 * 1024,
          keepAliveTimeMillis = 60000,
          keepAliveTimeoutMillis = 20000)))
    var server: Server = null
    try {
      server = KyuubiSparkConnectFrontendService
        .serverBuilder(conf, new InetSocketAddress("localhost", 0))
        .fallbackHandlerRegistry(new SparkConnectHandlerRegistry(relay))
        .build()
        .start()
      f(server.getPort)
    } finally {
      if (server != null) {
        server.shutdownNow()
        server.awaitTermination(5, TimeUnit.SECONDS)
      }
    }
  }

  private def withChannel(channel: ManagedChannel)(f: ManagedChannel => Unit): Unit = {
    try f(channel)
    finally channel.shutdownNow()
  }

  private def plaintextChannel(port: Int): ManagedChannel =
    NettyChannelBuilder.forAddress("localhost", port).usePlaintext().build()

  private def tlsChannel(port: Int): ManagedChannel =
    NettyChannelBuilder.forAddress("localhost", port)
      .sslContext(
        GrpcSslContexts.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build())
      .build()

  /**
   * A throwaway self-signed keystore, so the TLS path is exercised end to end rather than
   * described by a mock.
   */
  private def withKeystore(f: Path => Unit): Unit = {
    val certificate = new SelfSignedCertificate("localhost")
    val directory = Utils.createTempDir(prefix = "kyuubi-spark-connect-tls")
    val keystorePath = directory.resolve("keystore.jks")
    try {
      val password = keystorePassword.toCharArray
      val keyStore = KeyStore.getInstance("JKS")
      keyStore.load(null, password)
      keyStore.setKeyEntry(
        "spark-connect",
        certificate.key(),
        password,
        Array(certificate.cert()))
      val out = new FileOutputStream(keystorePath.toFile)
      try keyStore.store(out, password)
      finally out.close()
      f(keystorePath)
    } finally {
      certificate.delete()
      Utils.deleteDirectoryRecursively(directory.toFile)
    }
  }
}
