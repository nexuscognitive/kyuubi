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
import org.apache.kyuubi.server.metadata.MetadataManager
import org.apache.kyuubi.server.metadata.api.MetadataFilter
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.session.{KyuubiSessionManager, SessionHandle, SessionType}
import org.apache.kyuubi.util.ThreadUtils

/**
 * Periodically reconciles batch ownership against live cluster membership, so a batch whose owning
 * Kyuubi instance disappears (crash, OOM, node loss, scale-down, failed preStop) keeps being
 * tracked by a surviving instance. It:
 *
 *   - reclaims RUNNING/PENDING batches whose owner is no longer registered in discovery (its lease
 *     expired), using an atomic compare-and-set so that even if every live node races for the same
 *     batch, only one wins ownership - no split-brain;
 *   - recovers batches already owned by this instance but not locally tracked (e.g. handed off by a
 *     peer during graceful shutdown, or missed on startup).
 *
 * The Spark driver runs independently (cluster deploy mode), so takeover only re-attaches tracking
 * of an already-running job - it never resubmits or kills it. Death is judged by discovery
 * membership (etcd/ZK lease), not by pinging the peer: a partitioned node loses its own lease and
 * self-fences, which a ping-based check cannot guarantee.
 */
class BatchTakeoverService(
    restFrontend: KyuubiRestFrontendService,
    sessionManager: KyuubiSessionManager)
  extends AbstractService(classOf[BatchTakeoverService].getSimpleName) {

  private def selfInstance: String = restFrontend.connectionUrl
  private lazy val metadataManager: MetadataManager = sessionManager.metadataManager.get
  private val running = new AtomicBoolean(false)
  // A dead instance's discovery lease expires within ~10s; sweep a little slower than that.
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

    // Reclaim batches whose owner is no longer live (CAS: only one node wins each batch).
    val reclaimed = activeBatches.filter { batch =>
      val owner = batch.getKyuubiInstance
      owner != null && owner != selfInstance && !live.contains(owner) &&
      metadataManager.transferBatchOwnership(batch.getId, owner, selfInstance)
    }.map(_.getId)

    reclaimed.foreach(id => info(s"Reclaimed orphaned batch $id from a dead instance"))

    // Recover batches now owned by us but not yet tracked (just reclaimed, or handed off to us).
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
