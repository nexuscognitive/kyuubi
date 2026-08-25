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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import io.grpc.ManagedChannel

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.server.metadata.api.SparkConnectSessionInfo
import org.apache.kyuubi.util.ThreadUtils

/**
 * Settings for the frontend-to-engine leg of the proxy.
 *
 * @param maxInboundMessageSize largest single message accepted from the engine.
 * @param keepAliveTimeMillis interval between HTTP/2 keepalive pings.
 * @param keepAliveTimeoutMillis how long to wait for a ping acknowledgement.
 */
case class SparkConnectEngineChannelConf(
    maxInboundMessageSize: Int,
    keepAliveTimeMillis: Long,
    keepAliveTimeoutMillis: Long)

/**
 * One HTTP/2 connection per Spark Connect session, shared by every concurrent RPC on it.
 *
 * gRPC multiplexes streams over a single connection, so a session that is running a query while
 * fetching results and releasing an earlier execution needs exactly one upstream connection, not
 * three. Pooling per session rather than per engine keeps the lifetime obvious: the connection
 * dies with the session that owns it.
 */
class SparkConnectEngineChannelPool(
    channelConf: SparkConnectEngineChannelConf,
    channelFactory: (SparkConnectEngineAddress, SparkConnectEngineChannelConf) => ManagedChannel =
      SparkConnectEngineChannelPool.buildNettyChannel)
  extends Logging {

  private case class PooledChannel(
      sessionId: String,
      tokenId: String,
      address: SparkConnectEngineAddress,
      channel: ManagedChannel)

  private val channelsBySessionId = new ConcurrentHashMap[String, PooledChannel]()

  private val forceShutdownExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
    "spark-connect-channel-reaper")

  /**
   * The connection for `sessionInfo`, creating it on first use.
   *
   * Before handing back a pooled connection this re-checks that the presented token still matches
   * the one the connection was opened for. Two sessions can never share a connection, so a token
   * that resolves to session A can never be relayed over a connection that authenticated as
   * session B -- even if a store or cache bug were to make two tokens resolve to the same session
   * id. A pooled connection to a stale address is also discarded rather than reused, which is
   * what makes an engine that came back at a new IP recover on the next call.
   */
  def acquire(
      sessionInfo: SparkConnectSessionInfo,
      address: SparkConnectEngineAddress): ManagedChannel = {
    val pooled = channelsBySessionId.compute(
      sessionInfo.sessionId,
      (_, existing) => {
        if (existing == null) {
          PooledChannel(
            sessionInfo.sessionId,
            sessionInfo.tokenId,
            address,
            channelFactory(address, channelConf))
        } else if (!SparkConnect.tokenIdsMatch(existing.tokenId, sessionInfo.tokenId) ||
          existing.address != address) {
          warn(s"Discarding pooled Spark Connect channel to ${existing.address} for session" +
            s" ${sessionInfo.sessionId}: it no longer matches the presented token and address")
          shutdownQuietly(existing.channel)
          PooledChannel(
            sessionInfo.sessionId,
            sessionInfo.tokenId,
            address,
            channelFactory(address, channelConf))
        } else {
          existing
        }
      })
    pooled.channel
  }

  /** Tear down the connection for a session that has ended. */
  def release(sessionId: String): Unit =
    Option(channelsBySessionId.remove(sessionId)).foreach { pooled =>
      info(s"Closing Spark Connect channel to ${pooled.address} for session $sessionId")
      shutdownQuietly(pooled.channel)
    }

  def shutdown(): Unit = {
    channelsBySessionId.keys().asScala.toSeq.foreach(release)
    ThreadUtils.shutdown(forceShutdownExecutor)
  }

  private[connect] def pooledSessionCount: Int = channelsBySessionId.size()

  /**
   * Start a graceful shutdown and return immediately, forcing it later if it has not finished.
   *
   * `shutdown()` stops new calls at once but lets in-flight ones drain, and a channel with a live
   * HTTP/2 connection can take seconds to reach terminated. Blocking for that here would put those
   * seconds on the session-close path -- and on whatever thread happened to be closing the
   * session -- so the wait is handed to a daemon thread instead.
   */
  private def shutdownQuietly(channel: ManagedChannel): Unit =
    try {
      channel.shutdown()
      if (!channel.isTerminated) {
        forceShutdownExecutor.schedule(
          new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              if (!channel.isTerminated) {
                channel.shutdownNow()
              }
            }
          },
          SparkConnectEngineChannelPool.SHUTDOWN_GRACE_SECONDS,
          TimeUnit.SECONDS)
      }
    } catch {
      case NonFatal(e) =>
        warn("Failed to close a Spark Connect engine channel cleanly", e)
    }
}

object SparkConnectEngineChannelPool {
  private val SHUTDOWN_GRACE_SECONDS = 5L

  /**
   * Plaintext HTTP/2 to the driver pod.
   *
   * The hop runs inside the cluster network and is authenticated by the per-session token, which
   * the driver checks. Encrypting it as well would need a certificate per engine pod; that is
   * left to the service mesh, or to a later change.
   */
  private[connect] def buildNettyChannel(
      address: SparkConnectEngineAddress,
      channelConf: SparkConnectEngineChannelConf): ManagedChannel =
    io.grpc.netty.NettyChannelBuilder.forAddress(address.host, address.port)
      .usePlaintext()
      .maxInboundMessageSize(channelConf.maxInboundMessageSize)
      .keepAliveTime(channelConf.keepAliveTimeMillis, TimeUnit.MILLISECONDS)
      .keepAliveTimeout(channelConf.keepAliveTimeoutMillis, TimeUnit.MILLISECONDS)
      .build()
}
