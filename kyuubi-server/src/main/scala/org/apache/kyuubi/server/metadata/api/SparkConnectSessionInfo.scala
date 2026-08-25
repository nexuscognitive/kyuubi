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
 */
case class SparkConnectSessionInfo(
    userName: String,
    sessionId: String,
    engineTag: String,
    engineToken: String,
    createTime: Long = 0L) {

  /** Whether a Kyuubi session is still open on this engine, as opposed to the engine alone. */
  def hasLiveSession: Boolean = sessionId.nonEmpty

  /** Redacts the engine credential, which would otherwise reach any log that prints a record. */
  override def toString: String =
    s"SparkConnectSessionInfo(userName=$userName, sessionId=$sessionId," +
      s" engineTag=$engineTag, engineToken=***, createTime=$createTime)"
}
