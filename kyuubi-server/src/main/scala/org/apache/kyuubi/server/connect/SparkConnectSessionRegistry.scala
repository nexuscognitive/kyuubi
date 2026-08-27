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
import org.apache.kyuubi.server.metadata.api.{SparkConnectDriverPostMortem, SparkConnectRecoveryState, SparkConnectSessionInfo}

/**
 * Resolves an authenticated user to the Spark Connect engine that serves them.
 *
 * One user has at most one engine, so the user name -- as the authentication chain resolved it --
 * is the whole routing key. Nothing about the credential the caller presented is kept here: it is
 * verified per call by [[SparkConnectAuthenticator]] and then discarded.
 *
 * Backed by the metadata store so that a Kyuubi restart, or a second HA replica that never saw
 * the `POST` which created the session, can still route. The in-memory cache in front is only
 * there to keep the store off the per-RPC path: PySpark issues a `ReleaseExecute` after every
 * response batch, so lookups are frequent.
 */
class SparkConnectSessionRegistry(
    metadataManager: Option[MetadataManager],
    maxCacheSize: Long = SparkConnectSessionRegistry.DEFAULT_MAX_CACHE_SIZE,
    cacheExpirySeconds: Long = SparkConnectSessionRegistry.DEFAULT_CACHE_EXPIRY_SECONDS)
  extends Logging {

  private val bindingsByUserName: Cache[String, SparkConnectSessionInfo] = CacheBuilder.newBuilder()
    .maximumSize(maxCacheSize)
    .expireAfterWrite(cacheExpirySeconds, TimeUnit.SECONDS)
    .build[String, SparkConnectSessionInfo]()

  /**
   * Which user an engine belongs to, for the one lookup that arrives with a tag and nothing else:
   * a driver pod dying.
   *
   * Deliberately without the write expiry the binding cache carries. A driver that dies at three
   * in the morning under a session nobody has touched since the previous evening is precisely the
   * case this exists for, and an expiring entry would have gone by then. It holds two strings per
   * user and is bounded the same way, so keeping it is cheap; a miss falls through to the store.
   */
  private val userNamesByEngineTag: Cache[String, String] = CacheBuilder.newBuilder()
    .maximumSize(maxCacheSize)
    .build[String, String]()

  // Only holds sessions this instance created, so it is bounded by the local live session count.
  // A peer's sessions are reachable through the store, and are cleaned up by whoever closes them.
  private val userNamesBySessionId = new ConcurrentHashMap[String, String]()

  private val closeListeners = new CopyOnWriteArrayList[String => Unit]()

  /**
   * Register a callback invoked when a session goes away, so that resources keyed by session --
   * the upstream connection, above all -- are torn down instead of lingering until GC.
   */
  def onSessionClosed(listener: String => Unit): Unit = closeListeners.add(listener)

  /**
   * Bind `userName` to the engine their new session opened, replacing any earlier binding.
   *
   * `engineToken` is Kyuubi's own credential for that engine, not the caller's, and it is what
   * the relay presents on the upstream hop. It has to survive here because a Kyuubi instance that
   * did not launch the engine still has to authenticate to it.
   */
  def register(
      userName: String,
      sessionId: String,
      engineTag: String,
      engineToken: String,
      engineConf: Map[String, String] = Map.empty): SparkConnectSessionInfo = {
    val binding = SparkConnectSessionInfo(
      userName = userName,
      sessionId = sessionId,
      engineTag = engineTag,
      engineToken = engineToken,
      createTime = System.currentTimeMillis(),
      engineConf = engineConf)
    metadataManager.foreach { manager =>
      // A user has one engine, so the previous binding is replaced rather than accumulated.
      manager.cleanupSparkConnectSessionByUserName(userName)
      manager.insertSparkConnectSession(binding)
    }
    cacheBinding(binding)
    userNamesBySessionId.put(sessionId, userName)
    binding
  }

  private def cacheBinding(binding: SparkConnectSessionInfo): Unit = {
    bindingsByUserName.put(binding.userName, binding)
    userNamesByEngineTag.put(binding.engineTag, binding.userName)
  }

  /**
   * The engine bound to `userName`, whether or not a session is still open on it.
   *
   * A miss is deliberately not cached. Caching negative lookups would let anyone who can present
   * a valid credential grow the cache without bound, and the store round trip only happens for
   * users who have no engine -- whose calls are rejected rather than relayed.
   */
  def lookup(userName: String): Option[SparkConnectSessionInfo] =
    Option(bindingsByUserName.getIfPresent(userName)).orElse {
      val persisted =
        try {
          metadataManager.flatMap(_.getSparkConnectSessionByUserName(userName))
        } catch {
          case NonFatal(e) =>
            error(s"Failed to look up the Spark Connect engine binding for $userName", e)
            None
        }
      persisted.foreach(cacheBinding)
      persisted
    }

  /** The engine bound to `userName` while a Kyuubi session is still open on it. */
  def liveSession(userName: String): Option[SparkConnectSessionInfo] =
    lookup(userName).filter(_.hasLiveSession)

  /**
   * Forget the session on an engine, keeping the engine binding itself.
   *
   * The engine outlives its session: it is shared at `USER` level, so closing the session leaves
   * a driver running that the user's next session will be handed straight back by engine
   * discovery -- still carrying the `kyuubi-unique-tag` and the credential it was launched with.
   * Dropping the binding here would lose both, and the next session would route to a tag that no
   * pod carries. What is dropped is the session id, which is what makes the gRPC port answer
   * "create a session first" instead of relaying into an engine nobody is holding open.
   *
   * Called for every closing session, Spark Connect or not, so it must stay cheap and silent for
   * the overwhelming majority that were never registered here.
   */
  def unregister(sessionId: String): Unit = {
    val userName = userNamesBySessionId.remove(sessionId)
    if (userName == null) {
      return
    }
    Option(bindingsByUserName.getIfPresent(userName))
      .foreach(binding => bindingsByUserName.put(userName, binding.copy(sessionId = "")))
    closeListeners.asScala.foreach { listener =>
      try {
        listener(sessionId)
      } catch {
        case NonFatal(e) =>
          warn(s"A Spark Connect close listener failed for session $sessionId", e)
      }
    }
    try {
      metadataManager.foreach(_.detachSparkConnectSessionBySessionId(sessionId))
    } catch {
      case NonFatal(e) =>
        error(s"Failed to detach the Spark Connect routing record for session $sessionId", e)
    }
  }

  /** Drop the binding outright, for an engine that is known to be gone. */
  def forget(userName: String): Unit = {
    Option(bindingsByUserName.getIfPresent(userName)).foreach { binding =>
      userNamesBySessionId.remove(binding.sessionId)
      userNamesByEngineTag.invalidate(binding.engineTag)
    }
    bindingsByUserName.invalidate(userName)
    try {
      metadataManager.foreach(_.cleanupSparkConnectSessionByUserName(userName))
    } catch {
      case NonFatal(e) =>
        error(s"Failed to remove the Spark Connect engine binding for $userName", e)
    }
  }

  /**
   * The binding whose engine carries `engineTag`, or [[None]] when no Spark Connect session owns
   * that engine.
   *
   * The reverse index answers for any engine this instance has seen; the store answers for one it
   * has not, which is the normal case for an instance that restarted while a driver was running.
   * [[None]] is the answer for every batch and Thrift engine, so callers on the driver-death path
   * must treat it as ordinary rather than as an error.
   */
  def lookupByEngineTag(engineTag: String): Option[SparkConnectSessionInfo] = {
    Option(userNamesByEngineTag.getIfPresent(engineTag))
      .flatMap(lookup)
      .filter(_.engineTag == engineTag)
      .orElse {
        try {
          val persisted = metadataManager.flatMap(_.getSparkConnectSessionByEngineTag(engineTag))
          persisted.foreach(cacheBinding)
          persisted
        } catch {
          case NonFatal(e) =>
            error(s"Failed to look up the Spark Connect binding for engine $engineTag", e)
            None
        }
      }
  }

  /**
   * Record what killed a driver, against the binding that owns it.
   *
   * The post-mortem is prepended and the list trimmed to `retain`, so the newest death -- the one
   * explaining the current state -- is always first and the record cannot grow without bound.
   *
   * @return the updated binding, or [[None]] when no Spark Connect session owns that engine.
   */
  def recordDriverPostMortem(
      postMortem: SparkConnectDriverPostMortem,
      retain: Int): Option[SparkConnectSessionInfo] =
    lookupByEngineTag(postMortem.engineTag).map { binding =>
      persist(binding.copy(
        driverPostMortems = (postMortem +: binding.driverPostMortems).take(math.max(retain, 1))))
    }

  /**
   * Mark the user's engine as being relaunched.
   *
   * Written before the relaunch rather than after it so that a concurrent caller -- another
   * instance's gRPC relay, above all -- sees `RECOVERING` and answers the client with a retryable
   * status instead of starting a second driver.
   */
  def beginRecovery(userName: String): Option[SparkConnectSessionInfo] =
    lookup(userName).map { binding =>
      persist(binding.copy(
        recoveryState = SparkConnectRecoveryState.RECOVERING,
        recoveryMessage = None,
        lastRestartTime = System.currentTimeMillis()))
    }

  /**
   * Bind the user to the engine that recovery just launched, and count the restart.
   *
   * Bumping `generation` is the part that is not bookkeeping: it is the only durable statement
   * that this user's Spark session was replaced, and everything that tells a client its state is
   * gone reads from it.
   */
  def completeRecovery(
      userName: String,
      sessionId: String,
      engineTag: String,
      engineToken: String): Option[SparkConnectSessionInfo] =
    lookup(userName).map { binding =>
      val recovered = binding.copy(
        sessionId = sessionId,
        engineTag = engineTag,
        engineToken = engineToken,
        generation = binding.generation + 1,
        restartCount = binding.restartCount + 1,
        lastRestartTime = System.currentTimeMillis(),
        recoveryState = SparkConnectRecoveryState.NONE,
        recoveryMessage = None)
      userNamesBySessionId.remove(binding.sessionId)
      userNamesBySessionId.put(sessionId, userName)
      persist(recovered)
    }

  /**
   * Give up on the user's engine, terminally, and say why.
   *
   * The session stays in the store rather than being dropped: the reason and the post-mortems it
   * carries are the whole of what an operator has left to read, and deleting the row would take
   * them with it.
   */
  def abandonRecovery(userName: String, reason: String): Option[SparkConnectSessionInfo] =
    lookup(userName).map { binding =>
      warn(s"Abandoning Spark Connect engine recovery for $userName: $reason")
      persist(binding.copy(
        sessionId = "",
        recoveryState = SparkConnectRecoveryState.ABANDONED,
        recoveryMessage = Some(reason)))
    }

  /**
   * Spend one recovery attempt on a relaunch that never produced an engine.
   *
   * The attempt is counted and the binding leaves `RECOVERING`, so the next touch of the session
   * tries again -- but `generation` is untouched, because no new engine came into being for it to
   * identify and a client must not be told its session was replaced when it was not.
   */
  def failRecoveryAttempt(userName: String): Option[SparkConnectSessionInfo] =
    lookup(userName).map { binding =>
      persist(binding.copy(
        sessionId = "",
        restartCount = binding.restartCount + 1,
        lastRestartTime = System.currentTimeMillis(),
        recoveryState = SparkConnectRecoveryState.NONE))
    }

  /** The users this instance holds a binding for, for a sweep that has no other list to walk. */
  def cachedUserNames: Seq[String] = bindingsByUserName.asMap().keySet().asScala.toSeq

  private def persist(binding: SparkConnectSessionInfo): SparkConnectSessionInfo = {
    cacheBinding(binding)
    try {
      metadataManager.foreach(_.updateSparkConnectSessionRecovery(binding))
    } catch {
      case NonFatal(e) =>
        // The cache is already updated, so this instance keeps working; a peer will read the
        // older row and at worst repeat an attempt. Losing the record is not worth failing a
        // recovery over.
        error(s"Failed to persist the Spark Connect binding for ${binding.userName}", e)
    }
    binding
  }

  private[connect] def cachedBindingCount: Long = bindingsByUserName.size()
}

object SparkConnectSessionRegistry {
  private val DEFAULT_MAX_CACHE_SIZE = 10000L
  private val DEFAULT_CACHE_EXPIRY_SECONDS = 300L
}
