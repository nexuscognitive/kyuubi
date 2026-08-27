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

import org.apache.kyuubi.engine.{KubernetesDriverPodEvent, KubernetesDriverPostMortem}

/** One event recorded against a driver while it was alive, kept for as long as the post-mortem. */
case class SparkConnectDriverEventRecord(
    eventType: String,
    reason: String,
    message: String,
    count: Int,
    firstTimestamp: Option[String],
    lastTimestamp: Option[String])

/** Why one of the driver's containers stopped. */
case class SparkConnectDriverContainerExit(
    name: String,
    reason: Option[String],
    message: Option[String],
    exitCode: Option[Int],
    signal: Option[Int],
    oomKilled: Boolean,
    restartCount: Int,
    finishedAt: Option[String])

/**
 * The stored post-mortem of one dead Spark Connect driver.
 *
 * This is the persisted, cluster-manager-neutral form of what
 * [[org.apache.kyuubi.engine.KubernetesDriverPostMortem]] captures from a dying pod. It is a copy
 * rather than the capture type itself so that the JSON written into the metadata store keeps a
 * shape of the store's own, and so that a second cluster manager can populate it without the
 * column format becoming Kubernetes-specific.
 *
 * @param driverName what the driver was called on the cluster -- the pod name, on Kubernetes.
 * @param location where it ran -- the namespace, on Kubernetes.
 * @param finalState the terminal state the cluster manager reported, verbatim.
 * @param applicationState the state Kyuubi derived from it, which is what drove Kyuubi's own
 *                         decisions and can differ from `finalState`.
 * @param capturedTime when Kyuubi took the snapshot. The closest honest answer to "when did it
 *                     die": the driver's own timestamps are frequently absent.
 */
case class SparkConnectDriverPostMortem(
    engineTag: String,
    capturedTime: Long,
    driverName: String,
    location: String,
    finalState: String,
    applicationState: String,
    reason: Option[String],
    message: Option[String],
    containers: Seq[SparkConnectDriverContainerExit],
    events: Seq[SparkConnectDriverEventRecord]) {

  /** Whether any container was killed by the out-of-memory killer. */
  def oomKilled: Boolean = containers.exists(_.oomKilled)

  /**
   * A one-line explanation, for a UI badge or a log line.
   *
   * Prefers the container's termination reason over the pod's, because a driver that was OOM
   * killed reports `Failed` at the pod level and `OOMKilled` at the container level, and only the
   * latter tells an operator what to change.
   */
  def summary: String = {
    val containerReason = containers.flatMap(container =>
      container.reason.map { reason =>
        val exit = container.exitCode.map(code => s" (exit $code)").getOrElse("")
        s"$reason$exit"
      }).headOption
    containerReason
      .orElse(reason)
      .orElse(message)
      .getOrElse(finalState)
  }
}

object SparkConnectDriverPostMortem {

  /**
   * The neutral record for a post-mortem captured from a Kubernetes driver pod.
   *
   * `maxEvents` bounds what is persisted: events are the largest part of the record by far and a
   * driver that failed to schedule can accumulate hundreds of near-identical ones, none of which
   * add to the first few.
   */
  def fromDriverPod(
      postMortem: KubernetesDriverPostMortem,
      maxEvents: Int): SparkConnectDriverPostMortem =
    SparkConnectDriverPostMortem(
      engineTag = postMortem.engineTag,
      capturedTime = postMortem.capturedTime,
      driverName = postMortem.podName,
      location = postMortem.namespace,
      finalState = postMortem.finalPhase,
      applicationState = postMortem.applicationState,
      reason = postMortem.reason,
      message = postMortem.message,
      containers = postMortem.containers.map(container =>
        SparkConnectDriverContainerExit(
          name = container.name,
          reason = container.reason,
          message = container.message,
          exitCode = container.exitCode,
          signal = container.signal,
          oomKilled = container.oomKilled,
          restartCount = container.restartCount,
          finishedAt = container.finishedAt)),
      events = postMortem.events.take(math.max(maxEvents, 0)).map(driverEventRecord))

  private def driverEventRecord(event: KubernetesDriverPodEvent): SparkConnectDriverEventRecord =
    SparkConnectDriverEventRecord(
      eventType = event.eventType,
      reason = event.reason,
      message = event.message,
      count = event.count,
      firstTimestamp = event.firstTimestamp,
      lastTimestamp = event.lastTimestamp)
}
