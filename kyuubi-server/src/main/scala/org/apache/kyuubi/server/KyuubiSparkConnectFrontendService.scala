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

import java.io.{File, FileInputStream}
import java.net.InetSocketAddress
import java.security.KeyStore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import javax.net.ssl.KeyManagerFactory

import io.grpc.Server
import io.grpc.netty.{GrpcSslContexts, NettyServerBuilder}
import io.netty.handler.ssl.{SslContext, SslContextBuilder}

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.server.connect._
import org.apache.kyuubi.service.{AbstractFrontendService, Serverable, Service}
import org.apache.kyuubi.session.KyuubiSessionManager
import org.apache.kyuubi.util.JavaUtils

/**
 * A frontend service that speaks the Spark Connect gRPC protocol.
 *
 * It owns no protocol logic of its own. Every RPC under `spark.connect.SparkConnectService` is
 * relayed, byte for byte, to the Spark engine identified by the caller's bearer token; see
 * [[SparkConnectRelay]]. Sessions are not created here -- Spark Connect has no open-session RPC,
 * so a client obtains its token from the REST API first and this port only attaches to an engine
 * that already exists or is on its way up.
 */
class KyuubiSparkConnectFrontendService(override val serverable: Serverable)
  extends AbstractFrontendService("KyuubiSparkConnectFrontendService") {

  private val isStarted = new AtomicBoolean(false)

  private var grpcServer: Server = _
  private var channelPool: SparkConnectEngineChannelPool = _

  lazy val host: String = conf.get(FRONTEND_SPARK_CONNECT_BIND_HOST)
    .getOrElse {
      if (conf.get(KyuubiConf.FRONTEND_CONNECTION_URL_USE_HOSTNAME)) {
        JavaUtils.findLocalInetAddress.getCanonicalHostName
      } else {
        JavaUtils.findLocalInetAddress.getHostAddress
      }
    }

  private lazy val port: Int = conf.get(FRONTEND_SPARK_CONNECT_BIND_PORT)

  private def sessionManager: KyuubiSessionManager =
    serverable.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.conf = conf
    KyuubiSparkConnectFrontendService.validateConf(conf)

    channelPool = new SparkConnectEngineChannelPool(
      SparkConnectEngineChannelConf(
        maxInboundMessageSize = conf.get(FRONTEND_SPARK_CONNECT_ENGINE_MAX_MESSAGE_SIZE),
        keepAliveTimeMillis = conf.get(FRONTEND_SPARK_CONNECT_ENGINE_KEEPALIVE_TIME),
        keepAliveTimeoutMillis = conf.get(FRONTEND_SPARK_CONNECT_ENGINE_KEEPALIVE_TIMEOUT)))

    // A closed session's engine is gone, so its upstream connection goes with it rather than
    // idling until the keepalive notices.
    sessionManager.sparkConnectSessionRegistry.onSessionClosed(channelPool.release)

    val relay = new SparkConnectRelay(
      sessionManager.sparkConnectSessionRegistry,
      new KubernetesSparkConnectEngineLocator(
        sessionManager.applicationManager,
        conf.get(FRONTEND_SPARK_CONNECT_ENGINE_PORT)),
      channelPool)

    grpcServer = NettyServerBuilder.forAddress(new InetSocketAddress(host, port))
      .maxInboundMessageSize(conf.get(FRONTEND_SPARK_CONNECT_MAX_MESSAGE_SIZE))
      .sslContext(KyuubiSparkConnectFrontendService.buildSslContext(conf))
      .fallbackHandlerRegistry(new SparkConnectHandlerRegistry(relay))
      .build()

    super.initialize(conf)
  }

  override def connectionUrl: String = {
    checkInitialized()
    conf.get(FRONTEND_ADVERTISED_HOST) match {
      case Some(advertisedHost) => s"$advertisedHost:$port"
      case None => s"$host:$port"
    }
  }

  override def start(): Unit = synchronized {
    if (!isStarted.get) {
      try {
        grpcServer.start()
        isStarted.set(true)
        info(s"$getName has started at $host:${grpcServer.getPort}")
      } catch {
        case e: Exception => throw new KyuubiException(s"Cannot start $getName", e)
      }
    }
    super.start()
  }

  override def stop(): Unit = synchronized {
    if (isStarted.getAndSet(false)) {
      grpcServer.shutdown()
      if (!grpcServer.awaitTermination(
          KyuubiSparkConnectFrontendService.SHUTDOWN_TIMEOUT_SECONDS,
          TimeUnit.SECONDS)) {
        grpcServer.shutdownNow()
      }
    }
    if (channelPool != null) {
      channelPool.shutdown()
    }
    super.stop()
  }

  override val discoveryService: Option[Service] = None
}

object KyuubiSparkConnectFrontendService {

  private val SHUTDOWN_TIMEOUT_SECONDS = 5L

  /**
   * Reject a configuration that would produce a listener no client can talk to.
   *
   * Spark Connect's Python client silently upgrades to `ssl_channel_credentials()` as soon as a
   * token is configured for anything but a loopback host, and a token is always configured here.
   * A plaintext port would therefore answer a TLS ClientHello with gRPC frames, and the user
   * would see a bare handshake failure with nothing pointing at the real cause -- so TLS is
   * required rather than merely recommended, and the failure is raised at startup where an
   * operator will see it.
   */
  private[kyuubi] def validateConf(conf: KyuubiConf): Unit = {
    if (!conf.get(FRONTEND_SPARK_CONNECT_SSL_ENABLED)) {
      throw new KyuubiException(
        s"${FRONTEND_SPARK_CONNECT_SSL_ENABLED.key} must be true when the Spark Connect frontend" +
          " is enabled: Spark Connect clients negotiate TLS whenever a bearer token is set for a" +
          " non-loopback host, so a plaintext listener is unusable.")
    }
    if (conf.get(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH).isEmpty) {
      throw new KyuubiException(
        s"${FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH.key} is required when" +
          s" ${FRONTEND_SPARK_CONNECT_SSL_ENABLED.key} is true.")
    }
    if (!conf.isRESTEnabled) {
      throw new KyuubiException(
        "The Spark Connect frontend requires the REST frontend, which is where Spark Connect" +
          " sessions are created; add REST to " + FRONTEND_PROTOCOLS.key + ".")
    }
  }

  private[kyuubi] def buildSslContext(conf: KyuubiConf): SslContext = {
    val keystorePath = conf.get(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH).getOrElse {
      throw new KyuubiException(s"${FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH.key} is required")
    }
    val keystoreFile = new File(keystorePath)
    if (!keystoreFile.isFile) {
      throw new KyuubiException(s"Spark Connect keystore $keystorePath does not exist")
    }
    val password = conf.get(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PASSWORD).map(_.toCharArray).orNull
    val keyStore = KeyStore.getInstance(conf.get(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_TYPE))
    val keystoreStream = new FileInputStream(keystoreFile)
    try {
      keyStore.load(keystoreStream, password)
    } finally {
      keystoreStream.close()
    }
    val keyManagerFactory =
      KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keyManagerFactory.init(keyStore, password)
    GrpcSslContexts.configure(SslContextBuilder.forServer(keyManagerFactory)).build()
  }
}
