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

package org.apache.kyuubi.engine

/**
 * What Kyuubi can tell an operator about a Spark driver pod.
 *
 * A read-only snapshot taken from the Kubernetes API, deliberately not the fabric8 `Pod` itself:
 * these fields are the ones a diagnostics view renders, and pinning them here keeps the whole of
 * a cluster object -- annotations, tokens projected into the spec, environment -- from reaching a
 * REST response by accident.
 */
case class KubernetesDriverPod(
    name: String,
    namespace: String,
    nodeName: Option[String],
    phase: String,
    reason: Option[String],
    startTime: Option[String],
    podIp: Option[String],
    containers: Seq[KubernetesDriverContainer])

/**
 * One container of a driver pod.
 *
 * `restartCount` and `lastTermination*` are the pair that explains a driver which looks healthy
 * now but lost its Spark Connect state: a restarted driver has none of the session's temporary
 * views, cached frames or artifacts left, even though its container is `Running` again.
 */
case class KubernetesDriverContainer(
    name: String,
    state: String,
    stateReason: Option[String],
    ready: Boolean,
    restartCount: Int,
    exitCode: Option[Int],
    lastTerminationReason: Option[String],
    lastTerminationExitCode: Option[Int],
    requests: Map[String, String],
    limits: Map[String, String])

/**
 * One Kubernetes event recorded against a driver pod.
 *
 * Events are what actually explain a session that never came up -- `FailedScheduling`,
 * `ErrImagePull`, `OOMKilled` -- none of which appear in the pod's own status once it has settled.
 */
case class KubernetesDriverPodEvent(
    eventType: String,
    reason: String,
    message: String,
    count: Int,
    firstTimestamp: Option[String],
    lastTimestamp: Option[String])
