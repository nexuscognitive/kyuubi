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

package org.apache.kyuubi.server.metadata.api

/**
 * The record binding one user to their Spark Connect engine.
 *
 * Persisting this lets any Kyuubi instance route Spark Connect traffic for a session it did not
 * create, which is what makes a restart or a second HA replica transparent to a connected client.
 * The engine's network location is intentionally absent: it is rediscovered from the Kubernetes
 * API server by each instance's own driver pod informer, so it never goes stale in the store.
 *
 * Nothing here derives from the caller's own credential. Callers authenticate every gRPC call with
 * the platform credential they already hold, and that credential is resolved through Kyuubi's
 * authentication chain rather than looked up here, so it is neither stored nor digested.
 *
 * @param userName the user this engine belongs to, and the key the frontend routes on.
 * @param sessionId the Kyuubi session handle that opened the engine, or empty once that session
 *                  has closed while the engine itself is still up. An empty value is what the
 *                  frontend answers with "create a session first" rather than routing.
 * @param engineTag value of the engine's `kyuubi-unique-tag` pod label.
 * @param engineToken the credential Kyuubi presents to this engine, which the driver checks as
 *                    `spark.connect.authenticate.token`. It is Kyuubi's own, minted per engine
 *                    and never shown to a client.
 * @param createTime when the binding was created.
 * @param generation how many engines this binding has had. It starts at 0 and is incremented
 *                   every time recovery replaces a dead driver, which makes it the signal a
 *                   client uses to tell that its Spark session was replaced: a new generation is
 *                   a new driver JVM, with none of the temporary views, cached frames, artifacts
 *                   or session conf the old one held.
 * @param restartCount how many times recovery has relaunched a driver for this binding. Equal to
 *                     `generation` today and kept separate because they answer different
 *                     questions -- one identifies an engine, the other counts failures.
 * @param lastRestartTime when the most recent relaunch was started, or 0 if there has been none.
 * @param recoveryState where recovery stands: see [[SparkConnectRecoveryState]].
 * @param recoveryMessage why recovery is where it is -- above all, why it was abandoned. This is
 *                        what an operator reads on a session that is never coming back.
 * @param engineConf the conf the client asked for when it created the session, minus everything
 *                   only Kyuubi may set. Kept so that a relaunched engine comes up configured the
 *                   way the user asked for rather than on defaults: recovery cannot restore the
 *                   session's data, but there is no reason for it to lose its shape as well.
 * @param driverPostMortems what killed this binding's drivers, newest first, captured while each
 *                          pod still existed and bounded to the most recent few. The whole point
 *                          of keeping more than one is that the same failure three times running
 *                          is a crash loop, while three different failures are three problems.
 */
case class SparkConnectSessionInfo(
    userName: String,
    sessionId: String,
    engineTag: String,
    engineToken: String,
    createTime: Long = 0L,
    generation: Int = 0,
    restartCount: Int = 0,
    lastRestartTime: Long = 0L,
    recoveryState: String = SparkConnectRecoveryState.NONE,
    recoveryMessage: Option[String] = None,
    engineConf: Map[String, String] = Map.empty,
    driverPostMortems: Seq[SparkConnectDriverPostMortem] = Nil) {

  /** Whether a Kyuubi session is still open on this engine, as opposed to the engine alone. */
  def hasLiveSession: Boolean = sessionId.nonEmpty

  /** Whether a relaunch is in flight, or waiting out its backoff before the next attempt. */
  def isRecovering: Boolean = recoveryState == SparkConnectRecoveryState.RECOVERING

  /**
   * Whether recovery has given up. Terminal: nothing in Kyuubi will bring this engine back, and
   * the user has to create a session again.
   */
  def isRecoveryAbandoned: Boolean = recoveryState == SparkConnectRecoveryState.ABANDONED

  /** Whether this binding has ever had a driver replaced under it. */
  def wasRestarted: Boolean = restartCount > 0

  /** The most recent driver death, which is the one that explains the current state. */
  def latestPostMortem: Option[SparkConnectDriverPostMortem] = driverPostMortems.headOption

  /** Redacts the engine credential, which would otherwise reach any log that prints a record. */
  override def toString: String =
    s"SparkConnectSessionInfo(userName=$userName, sessionId=$sessionId," +
      s" engineTag=$engineTag, engineToken=***, createTime=$createTime," +
      s" generation=$generation, restartCount=$restartCount," +
      s" lastRestartTime=$lastRestartTime, recoveryState=$recoveryState," +
      s" driverPostMortems=${driverPostMortems.size})"
}

/**
 * Where recovery stands for one binding.
 *
 * Kept as strings rather than an enumeration because the value is persisted in the metadata store
 * and read back by instances that may be running a different Kyuubi build.
 */
object SparkConnectRecoveryState {

  /** No recovery has been needed, or the last one succeeded. */
  val NONE = ""

  /** A relaunch is in flight, or its backoff is being waited out. */
  val RECOVERING = "RECOVERING"

  /** Recovery ran out of attempts. Terminal -- see [[SparkConnectSessionInfo.recoveryMessage]]. */
  val ABANDONED = "ABANDONED"
}
