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

/**
 * Why one driver container stopped, captured while the pod still existed.
 *
 * `oomKilled` is broken out rather than left for a reader to infer from `reason`, because it is
 * the single most common cause of a Spark Connect driver dying under a user who then finds no pod
 * and no events to explain it, and because the response to it -- raise `spark.driver.memory` --
 * differs from every other termination reason.
 */
case class KubernetesDriverContainerTermination(
    name: String,
    reason: Option[String],
    message: Option[String],
    exitCode: Option[Int],
    signal: Option[Int],
    oomKilled: Boolean,
    restartCount: Int,
    finishedAt: Option[String])

/**
 * The post-mortem of a driver pod, taken at the moment Kyuubi observed it terminate.
 *
 * Kubernetes events are namespaced objects with a short TTL that are garbage-collected once their
 * involved object is gone, so a driver pod that died overnight takes the only explanation of its
 * death with it. Everything an operator would have gone looking for is therefore copied out here
 * while the pod is still observable, and persisted with the session binding so that it outlives
 * both the pod and the Kyuubi process that saw it die.
 *
 * @param engineTag the `kyuubi-unique-tag` of the engine whose driver this was.
 * @param capturedTime when the snapshot was taken, which is the closest thing to a time of death
 *                     Kyuubi can honestly report -- the pod's own timestamps may be absent.
 * @param applicationState the state Kyuubi itself derived, which is what drove its own decisions,
 *                         as opposed to the raw pod phase Kubernetes reported.
 * @param events the pod's events as of that moment, newest first and bounded.
 */
case class KubernetesDriverPostMortem(
    engineTag: String,
    capturedTime: Long,
    podName: String,
    namespace: String,
    finalPhase: String,
    applicationState: String,
    reason: Option[String],
    message: Option[String],
    containers: Seq[KubernetesDriverContainerTermination],
    events: Seq[KubernetesDriverPodEvent])
