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

import org.apache.kyuubi.engine.KyuubiApplicationManager

/** Where an engine's Spark Connect gRPC server can be reached. */
case class SparkConnectEngineAddress(host: String, port: Int) {
  override def toString: String = s"$host:$port"
}

/**
 * Turns an engine's `kyuubi-unique-tag` into a network address.
 */
trait SparkConnectEngineLocator {

  /**
   * The address of the engine tagged `engineTag`, or [[None]] while it is not serving.
   *
   * A [[None]] covers both "no such engine" and "the engine exists but is still starting"; the
   * caller answers both the same way, with a retryable `UNAVAILABLE`.
   */
  def locate(engineTag: String): Option[SparkConnectEngineAddress]
}

/**
 * Locates engines from the driver pod informer that
 * [[org.apache.kyuubi.engine.KubernetesApplicationOperation]] already maintains -- no second
 * watch, and no call to the API server on the request path.
 *
 * The pod IP is used directly rather than a DNS name. Spark driver pods run with
 * `restartPolicy: Never`, so the IP is stable for the lifetime of the application, and skipping
 * DNS removes a resolver dependency from the hot path.
 */
class KubernetesSparkConnectEngineLocator(
    applicationManager: KyuubiApplicationManager,
    enginePort: Int) extends SparkConnectEngineLocator {

  override def locate(engineTag: String): Option[SparkConnectEngineAddress] =
    applicationManager.getKubernetesApplicationOperation
      .flatMap(_.getRunningEnginePodIpByTag(engineTag))
      .map(SparkConnectEngineAddress(_, enginePort))
}
