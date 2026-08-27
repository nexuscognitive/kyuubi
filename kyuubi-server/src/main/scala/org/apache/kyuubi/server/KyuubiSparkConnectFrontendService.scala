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

import org.apache.kyuubi.{KyuubiException, Logging}
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
 * relayed, byte for byte, to the Spark engine that serves the authenticated caller; see
 * [[SparkConnectRelay]]. The caller presents the platform credential they already hold, resolved
 * through the same provider the HTTP frontend uses. Sessions are not created here -- Spark Connect
 * has no open-session RPC, and provisioning an engine on the first call would block it for a
 * minute or two -- so this port only attaches to a session the REST API already opened.
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
      new SparkConnectAuthenticator(conf),
      sessionManager.sparkConnectSessionRegistry,
      sessionManager.sparkConnectEngineLocator,
      channelPool,
      sessionManager.sparkConnectSessionSupervisor)

    grpcServer = KyuubiSparkConnectFrontendService
      .serverBuilder(conf, new InetSocketAddress(host, port))
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

object KyuubiSparkConnectFrontendService extends Logging {

  private val SHUTDOWN_TIMEOUT_SECONDS = 5L

  /**
   * What an operator sees when they turn TLS off.
   *
   * Whether a plaintext listener is fine or catastrophic depends entirely on what sits in front
   * of it, which Kyuubi cannot see, so the warning has to name both cases -- and in particular
   * the direct-client one, because its symptom is a bare TLS handshake failure with nothing in
   * it pointing back at this setting.
   */
  private[kyuubi] val PLAINTEXT_LISTENER_WARNING: String =
    s"${FRONTEND_SPARK_CONNECT_SSL_ENABLED.key} is false: the Spark Connect frontend is serving" +
      " PLAINTEXT gRPC. Bearer tokens, queries and results all cross this port unencrypted, so" +
      " it is only safe behind a proxy that terminates TLS -- a Kubernetes ingress, say -- with" +
      " the hop from that proxy to this port confined to a trusted network. Spark Connect" +
      " clients that dial this port DIRECTLY will fail their handshake rather than fall back to" +
      " plaintext: the client upgrades to a secure channel on its own as soon as a bearer token" +
      " is set for a non-loopback host, and this port answers a TLS ClientHello with gRPC" +
      s" frames. To serve those clients, set ${FRONTEND_SPARK_CONNECT_SSL_ENABLED.key}=true and" +
      s" point ${FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH.key} at a keystore."

  /**
   * Reject a configuration that would produce a listener no client can talk to.
   */
  private[kyuubi] def validateConf(conf: KyuubiConf): Unit = {
    if (conf.get(FRONTEND_SPARK_CONNECT_SSL_ENABLED) &&
      conf.get(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH).isEmpty) {
      throw new KyuubiException(
        s"${FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH.key} is required when" +
          s" ${FRONTEND_SPARK_CONNECT_SSL_ENABLED.key} is true.")
    }
    if (!conf.isRESTEnabled) {
      throw new KyuubiException(
        "The Spark Connect frontend requires the REST frontend, which is where Spark Connect" +
          " sessions are created; add REST to " + FRONTEND_PROTOCOLS.key + ".")
    }
    // Without a bearer provider the port would accept a connection and then reject every call as
    // UNAUTHENTICATED, which looks like a broken deployment rather than an unconfigured one.
    if (conf.get(AUTHENTICATION_CUSTOM_BEARER_CLASS).forall(_.trim.isEmpty)) {
      throw new KyuubiException(
        s"${AUTHENTICATION_CUSTOM_BEARER_CLASS.key} is required by the Spark Connect frontend:" +
          " callers authenticate every gRPC call with the same bearer credential the HTTP" +
          " frontends accept, and it is resolved through that provider.")
    }
  }

  /**
   * The gRPC transport for the frontend, with TLS unless the operator has opted out.
   *
   * TLS is the default because a client that dials Kyuubi directly can use nothing else. It is
   * opt-out rather than mandatory because the common Kubernetes deployment puts an ingress in
   * front that terminates TLS with a real certificate and speaks plaintext gRPC to the pod: the
   * client never sees this hop, and nginx-ingress does not verify backend certificates, so
   * requiring one here would only mean minting a self-signed keystore that nothing validates.
   */
  private[kyuubi] def serverBuilder(
      conf: KyuubiConf,
      address: InetSocketAddress): NettyServerBuilder = {
    val builder = NettyServerBuilder.forAddress(address)
      .maxInboundMessageSize(conf.get(FRONTEND_SPARK_CONNECT_MAX_MESSAGE_SIZE))
    if (conf.get(FRONTEND_SPARK_CONNECT_SSL_ENABLED)) {
      builder.sslContext(buildSslContext(conf))
    } else {
      warn(PLAINTEXT_LISTENER_WARNING)
      builder
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
