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

package org.apache.kyuubi.server

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kyuubi.Utils
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.server.metadata.api.MetadataFilter
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.session.{KyuubiSessionManager, SessionHandle, SessionType}
import org.apache.kyuubi.util.ThreadUtils

/**
 * Periodically reclaims batch sessions whose owning Kyuubi instance is no longer registered in
 * discovery, so that a crashed server does not strand its in-flight batches.
 *
 * It also picks up batches this instance already owns in the metadata store but has no live
 * session for, which happens after a peer hands its batches off during a graceful shutdown.
 */
class BatchTakeoverService(
    restFrontend: KyuubiRestFrontendService,
    sessionManager: KyuubiSessionManager)
  extends AbstractService(classOf[BatchTakeoverService].getSimpleName) {

  private def selfInstance: String = restFrontend.connectionUrl

  private lazy val metadataManager = sessionManager.metadataManager.get

  private val running = new AtomicBoolean(false)

  private val sweepIntervalMs = 15000L

  private var sweeper: ScheduledExecutorService = _

  override def start(): Unit = {
    running.set(true)
    sweeper = ThreadUtils.newDaemonSingleThreadScheduledExecutor("batch-takeover-sweeper")
    val task = new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError(sweep())
    }
    sweeper.scheduleWithFixedDelay(task, sweepIntervalMs, sweepIntervalMs, TimeUnit.MILLISECONDS)
    info(s"Started batch takeover sweeper with interval ${sweepIntervalMs}ms")
    super.start()
  }

  override def stop(): Unit = {
    running.set(false)
    if (sweeper != null) {
      ThreadUtils.shutdown(sweeper)
      sweeper = null
    }
    super.stop()
  }

  private[kyuubi] def sweep(): Unit = {
    if (!running.get) return
    restFrontend.waitForServerStarted()

    val live = restFrontend.liveRestInstances()

    val activeBatches = Seq(OperationState.PENDING, OperationState.RUNNING).flatMap { state =>
      metadataManager.getBatches(
        MetadataFilter(sessionType = SessionType.BATCH, state = state.toString),
        0,
        Int.MaxValue)
    }

    // batches owned by an instance that has left discovery: claim them for ourselves
    val reclaimed = activeBatches.filter { batch =>
      val owner = batch.getKyuubiInstance
      owner != null && owner != selfInstance && !live.contains(owner) &&
      metadataManager.transferBatchOwnership(batch.getId, owner, selfInstance)
    }.map(_.getId)
    reclaimed.foreach(id => info(s"Reclaimed orphaned batch $id from a dead instance"))

    // batches already assigned to us with no live session, e.g. handed off by a peer
    val ownUntracked = activeBatches.filter { batch =>
      batch.getKyuubiInstance == selfInstance &&
      sessionManager.getBatchSession(SessionHandle.fromUUID(batch.getId)).isEmpty
    }.map(_.getId)

    val toRecover = (reclaimed ++ ownUntracked).distinct
    if (toRecover.nonEmpty) {
      val recovered = restFrontend.recoverBatchSessionsFromReassign(toRecover)
      if (recovered.nonEmpty) {
        info(s"Took over ${recovered.size} batch session(s): ${recovered.mkString(", ")}")
      }
    }
  }
}
