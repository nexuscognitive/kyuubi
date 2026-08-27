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

import org.apache.kyuubi.engine.{KubernetesDriverPod, KubernetesDriverPostMortem, KyuubiApplicationManager}
import org.apache.kyuubi.engine.ApplicationState.ApplicationState

/**
 * What the supervisor is allowed to know about a driver.
 *
 * Narrow on purpose. Everything behind it is a live Kubernetes cluster, and a reconciliation rule
 * that can only be exercised against one is a rule that is never exercised: the states that matter
 * here -- a pod that was there and is gone, a pod that never appeared, a driver that died three
 * times running -- are exactly the ones nobody can produce on demand in a unit test.
 */
trait SparkConnectDriverObserver {

  /**
   * Whether this Kyuubi instance can observe drivers at all.
   *
   * `false` on a deployment that launches engines somewhere other than Kubernetes, where the
   * honest answer to every question below is "no information" rather than "no driver" -- and the
   * two must not be conflated, because the second would report every session dead.
   */
  def isAvailable: Boolean

  /** The application state Kyuubi holds for `engineTag`, or [[None]] if it holds none. */
  def applicationState(engineTag: String): Option[ApplicationState]

  /** The driver pod carrying `engineTag`, or [[None]] if no pod does. */
  def driverPod(engineTag: String): Option[KubernetesDriverPod]

  /** Register a callback invoked once per engine when its driver is observed to terminate. */
  def onDriverTerminated(listener: KubernetesDriverPostMortem => Unit): Unit
}

/**
 * Reads drivers from the pod informer
 * [[org.apache.kyuubi.engine.KubernetesApplicationOperation]] already runs.
 *
 * No watch and no client of its own: the informer is maintained for engine lifecycle management
 * regardless, and a second one would double the load this gateway puts on the API server to learn
 * nothing new.
 */
class KubernetesSparkConnectDriverObserver(applicationManager: KyuubiApplicationManager)
  extends SparkConnectDriverObserver {

  private def operation = applicationManager.getKubernetesApplicationOperation
    .filter(_.hasKubernetesClient)

  override def isAvailable: Boolean = operation.isDefined

  override def applicationState(engineTag: String): Option[ApplicationState] =
    operation.flatMap(_.getEngineApplicationStateByTag(engineTag))

  override def driverPod(engineTag: String): Option[KubernetesDriverPod] =
    operation.flatMap(_.getDriverPodDetailByTag(engineTag))

  override def onDriverTerminated(listener: KubernetesDriverPostMortem => Unit): Unit =
    operation.foreach(_.onDriverTerminated(listener))
}
