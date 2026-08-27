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

import scala.collection.mutable.ListBuffer

import org.apache.kyuubi.engine.{ApplicationState, KubernetesDriverContainerTermination, KubernetesDriverPod, KubernetesDriverPodEvent, KubernetesDriverPostMortem}
import org.apache.kyuubi.engine.ApplicationState.ApplicationState

/**
 * A driver observer a test drives by hand.
 *
 * The situations this feature exists for -- a pod that was there and is gone, a driver that died
 * three times running, a deployment with no Kubernetes at all -- cannot be produced on demand on
 * a real cluster, so they are produced here instead. That is also why the supervisor takes the
 * observer rather than the Kubernetes operation: a reconciliation rule that can only be exercised
 * against a live cluster is a rule that is never exercised.
 */
class FakeSparkConnectDriverObserver(var available: Boolean = true)
  extends SparkConnectDriverObserver {

  import FakeSparkConnectDriverObserver._

  private val terminationListeners = ListBuffer[KubernetesDriverPostMortem => Unit]()

  var applicationStates: Map[String, ApplicationState] = Map.empty
  var driverPods: Map[String, KubernetesDriverPod] = Map.empty

  override def isAvailable: Boolean = available

  override def applicationState(engineTag: String): Option[ApplicationState] =
    applicationStates.get(engineTag)

  override def driverPod(engineTag: String): Option[KubernetesDriverPod] =
    driverPods.get(engineTag)

  override def onDriverTerminated(listener: KubernetesDriverPostMortem => Unit): Unit =
    terminationListeners += listener

  /** Put a live, routable driver behind `engineTag`. */
  def driverIsRunning(engineTag: String): Unit = {
    applicationStates += engineTag -> ApplicationState.RUNNING
    driverPods += engineTag -> podInPhase(engineTag, POD_PHASE_RUNNING)
  }

  /** Put a driver behind `engineTag` that has been scheduled but is not serving yet. */
  def driverIsPending(engineTag: String): Unit = {
    applicationStates += engineTag -> ApplicationState.PENDING
    driverPods += engineTag -> podInPhase(engineTag, POD_PHASE_PENDING)
  }

  /**
   * Kill the driver behind `engineTag` the way Kubernetes eventually leaves it: the pod is gone,
   * and only Kyuubi's own record of the application says there ever was one.
   */
  def driverDiedAndPodWasReclaimed(engineTag: String): Unit = {
    applicationStates += engineTag -> ApplicationState.FAILED
    driverPods -= engineTag
  }

  /** Forget everything about `engineTag`, as a Kyuubi that has just restarted would have. */
  def forget(engineTag: String): Unit = {
    applicationStates -= engineTag
    driverPods -= engineTag
  }

  /** Deliver a driver death to whatever registered for it, as the pod informer would. */
  def announceDriverDeath(postMortem: KubernetesDriverPostMortem): Unit =
    terminationListeners.foreach(_(postMortem))
}

object FakeSparkConnectDriverObserver {

  val POD_PHASE_RUNNING = "Running"
  val POD_PHASE_PENDING = "Pending"

  def podInPhase(engineTag: String, phase: String): KubernetesDriverPod = KubernetesDriverPod(
    name = s"driver-$engineTag",
    namespace = "kyuubi",
    nodeName = Some("a-node"),
    phase = phase,
    reason = None,
    startTime = None,
    podIp = Some("10.0.0.1"),
    containers = Nil)

  /**
   * A post-mortem shaped like the one an OOM-killed driver leaves behind, which is the commonest
   * way a Spark Connect driver dies out from under a user with no warning.
   */
  def oomKilledPostMortem(engineTag: String): KubernetesDriverPostMortem =
    KubernetesDriverPostMortem(
      engineTag = engineTag,
      capturedTime = System.currentTimeMillis(),
      podName = s"driver-$engineTag",
      namespace = "kyuubi",
      finalPhase = "Failed",
      applicationState = ApplicationState.FAILED.toString,
      reason = None,
      message = None,
      containers = Seq(KubernetesDriverContainerTermination(
        name = "spark-kubernetes-driver",
        reason = Some("OOMKilled"),
        message = None,
        exitCode = Some(137),
        signal = Some(9),
        oomKilled = true,
        restartCount = 0,
        finishedAt = Some("2026-08-27T02:11:04Z"))),
      events = Seq(KubernetesDriverPodEvent(
        eventType = "Warning",
        reason = "Evicted",
        message = "The node was low on resource: memory",
        count = 1,
        firstTimestamp = Some("2026-08-27T02:11:03Z"),
        lastTimestamp = Some("2026-08-27T02:11:03Z"))))
}
