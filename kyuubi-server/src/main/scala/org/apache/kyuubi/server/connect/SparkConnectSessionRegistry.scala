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

import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.google.common.cache.{Cache, CacheBuilder}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.server.metadata.MetadataManager
import org.apache.kyuubi.server.metadata.api.SparkConnectSessionInfo

/**
 * Resolves a Spark Connect bearer token to the session -- and therefore the engine -- that owns
 * it.
 *
 * Backed by the metadata store so that a Kyuubi restart, or a second HA replica that never saw
 * the `POST` which created the session, can still route for it. The in-memory cache in front is
 * only there to keep the store off the per-RPC path: PySpark issues a `ReleaseExecute` after
 * every response batch, so lookups are frequent.
 */
class SparkConnectSessionRegistry(
    metadataManager: Option[MetadataManager],
    maxCacheSize: Long = SparkConnectSessionRegistry.DEFAULT_MAX_CACHE_SIZE,
    cacheExpirySeconds: Long = SparkConnectSessionRegistry.DEFAULT_CACHE_EXPIRY_SECONDS)
  extends Logging {

  private val sessionsByTokenId: Cache[String, SparkConnectSessionInfo] = CacheBuilder.newBuilder()
    .maximumSize(maxCacheSize)
    .expireAfterWrite(cacheExpirySeconds, TimeUnit.SECONDS)
    .build[String, SparkConnectSessionInfo]()

  // Only holds sessions this instance created, so it is bounded by the local live session count.
  // A peer's sessions are reachable through the store, and are cleaned up by whoever closes them.
  private val tokenIdsBySessionId = new ConcurrentHashMap[String, String]()

  private val closeListeners = new CopyOnWriteArrayList[String => Unit]()

  /**
   * Register a callback invoked when a session goes away, so that resources keyed by session --
   * the upstream connection, above all -- are torn down instead of lingering until GC.
   */
  def onSessionClosed(listener: String => Unit): Unit = closeListeners.add(listener)

  /**
   * Record a freshly created Spark Connect session and return its routing entry.
   *
   * The raw token is never stored -- only its digest -- so it is the caller's job to hand the
   * token back to the client, once, in the response to the create call.
   */
  def register(
      token: String,
      sessionId: String,
      userName: String,
      engineTag: String): SparkConnectSessionInfo = {
    val sessionInfo = SparkConnectSessionInfo(
      tokenId = SparkConnect.tokenId(token),
      sessionId = sessionId,
      userName = userName,
      engineTag = engineTag,
      createTime = System.currentTimeMillis())
    metadataManager.foreach(_.insertSparkConnectSession(sessionInfo))
    sessionsByTokenId.put(sessionInfo.tokenId, sessionInfo)
    tokenIdsBySessionId.put(sessionId, sessionInfo.tokenId)
    sessionInfo
  }

  /**
   * The session that owns `token`, or [[None]] if no session does.
   *
   * A miss is deliberately not cached. Caching negative lookups would let anyone who can reach
   * the port grow the cache without bound simply by presenting fresh garbage tokens.
   */
  def lookup(token: String): Option[SparkConnectSessionInfo] = {
    val id = SparkConnect.tokenId(token)
    Option(sessionsByTokenId.getIfPresent(id)).orElse {
      val persisted =
        try {
          metadataManager.flatMap(_.getSparkConnectSessionByTokenId(id))
        } catch {
          case NonFatal(e) =>
            error(s"Failed to look up the Spark Connect session for token $id", e)
            None
        }
      persisted.foreach(sessionsByTokenId.put(id, _))
      persisted
    }
  }

  /**
   * Forget a session and tear down what was keyed to it.
   *
   * Called for every closing session, Spark Connect or not, so it must stay cheap and silent for
   * the overwhelming majority that were never registered here.
   */
  def unregister(sessionId: String): Unit = {
    val tokenId = tokenIdsBySessionId.remove(sessionId)
    if (tokenId == null) {
      return
    }
    sessionsByTokenId.invalidate(tokenId)
    closeListeners.asScala.foreach { listener =>
      try {
        listener(sessionId)
      } catch {
        case NonFatal(e) =>
          warn(s"A Spark Connect close listener failed for session $sessionId", e)
      }
    }
    try {
      metadataManager.foreach(_.cleanupSparkConnectSessionBySessionId(sessionId))
    } catch {
      case NonFatal(e) =>
        error(s"Failed to remove the Spark Connect routing record for session $sessionId", e)
    }
  }

  private[connect] def cachedSessionCount: Long = sessionsByTokenId.size()
}

object SparkConnectSessionRegistry {
  private val DEFAULT_MAX_CACHE_SIZE = 10000L
  private val DEFAULT_CACHE_EXPIRY_SECONDS = 300L
}
