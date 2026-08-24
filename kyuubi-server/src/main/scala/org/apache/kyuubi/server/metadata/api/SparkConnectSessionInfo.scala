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
 * The routing record for one Spark Connect session.
 *
 * Persisting this lets any Kyuubi instance route Spark Connect traffic for a session it did not
 * create, which is what makes a restart or a second HA replica transparent to a connected client.
 * The engine's network location is intentionally absent: it is rediscovered from the Kubernetes
 * API server by each instance's own driver pod informer, so it never goes stale in the store.
 *
 * @param tokenId SHA-256 hex digest of the bearer token. Only the digest is persisted, so a
 *                reader of the metadata store cannot impersonate a live session.
 * @param sessionId the Kyuubi session handle that owns the engine.
 * @param userName the user the session was opened for.
 * @param engineTag value of the engine's `kyuubi-unique-tag` pod label.
 * @param createTime when the session was created.
 */
case class SparkConnectSessionInfo(
    tokenId: String,
    sessionId: String,
    userName: String,
    engineTag: String,
    createTime: Long = 0L)
