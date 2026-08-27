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

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.SparkProcessBuilder

/**
 * The conf that makes a Kyuubi session a Spark Connect one.
 *
 * It lives here rather than beside the REST endpoint because two callers need it: the endpoint,
 * which opens a session for a user asking for one, and the supervisor, which opens one to replace
 * an engine that died. There is exactly one way to provision a Spark Connect engine, and a second
 * copy of these keys is how the two would drift.
 */
object SparkConnectEngineConf {

  /**
   * The subdomain that keeps a Spark Connect engine to itself.
   *
   * At `USER` share level the engine space is keyed by user and subdomain, so without this a
   * Spark Connect session would be handed whatever ordinary Thrift or REST engine the same user
   * already had -- a driver launched without the Spark Connect plugin, which answers nothing on
   * the gRPC port. It also keeps the reverse from happening to an unsuspecting Thrift session.
   */
  val ENGINE_SUBDOMAIN = "spark-connect"

  /** The only deploy mode a Spark Connect engine works in -- see [[serverControlledConf]]. */
  val DEPLOY_MODE_CLUSTER = "cluster"

  /**
   * Conf that only Kyuubi may set.
   *
   * The engine share level is `USER`, with a subdomain of its own. A Spark Connect session is
   * stateful -- artifacts, temporary views, cached frames -- so the engine cannot be shared
   * across users; within one user it can be, because there is at most one Spark Connect session
   * per user and the state that survives between two of their own sessions is their own. Sharing
   * it that far is what lets a user who closes a session and opens another get their engine back
   * in a second rather than waiting out another cold start.
   *
   * The token is Kyuubi's credential for the engine, minted when the engine is launched and
   * reused for as long as that driver lives. It is never returned to a client: callers
   * authenticate with their own platform credential, which terminates at the frontend.
   *
   * The deploy mode is pinned to `cluster` for Spark Connect alone -- other engine types keep
   * whatever the deployment configures. In client mode the driver JVM runs inside the Kyuubi
   * pod, where `SparkConnectPlugin` tries to bind [[FRONTEND_SPARK_CONNECT_ENGINE_PORT]] in the
   * same network namespace that the gateway's own Spark Connect frontend already listens on, and
   * fails outright because `spark.port.maxRetries` defaults to 0. Even on a free port a
   * client-mode engine would be unreachable: [[SparkConnectEngineLocator]] resolves an engine by
   * finding the driver pod that carries its `kyuubi-unique-tag`, and in client mode there is no
   * such pod.
   */
  def serverControlledConf(engineToken: String): Map[String, String] = Map(
    SESSION_SPARK_CONNECT_ENABLED.key -> "true",
    SESSION_SPARK_CONNECT_TOKEN.key -> engineToken,
    ENGINE_TYPE.key -> "SPARK_SQL",
    ENGINE_SHARE_LEVEL.key -> "USER",
    ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> ENGINE_SUBDOMAIN,
    SparkProcessBuilder.DEPLOY_MODE_KEY -> DEPLOY_MODE_CLUSTER)

  val SERVER_CONTROLLED_KEYS: Set[String] = Set(
    SESSION_SPARK_CONNECT_ENABLED.key,
    SESSION_SPARK_CONNECT_TOKEN.key,
    ENGINE_TYPE.key,
    ENGINE_SHARE_LEVEL.key,
    ENGINE_SHARE_LEVEL_SUBDOMAIN.key,
    SparkProcessBuilder.DEPLOY_MODE_KEY)

  /**
   * The caller's conf, minus anything only Kyuubi may set.
   *
   * Stripping rather than rejecting keeps a client that echoes a previous session's conf working,
   * and a self-declared engine token would be inert anyway -- the frontend presents the one from
   * the routing record, which only the session endpoint writes -- but letting one through would
   * still be a needless surprise.
   */
  def clientControlledConf(requestedConf: Map[String, String]): Map[String, String] =
    requestedConf -- SERVER_CONTROLLED_KEYS

  /**
   * Whether a session belongs to the Spark Connect frontend.
   *
   * Keyed off the conf the session endpoint itself pins, so a session opened through any other
   * frontend -- Thrift, the ordinary REST session API -- is never listed as a Spark Connect one
   * even though it lives in the same session manager.
   */
  def isSparkConnectSession(sessionConf: Map[String, String]): Boolean =
    sessionConf.get(SESSION_SPARK_CONNECT_ENABLED.key).contains("true")
}
