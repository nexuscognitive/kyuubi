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

package org.apache.kyuubi.server.metadata

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import com.google.common.annotations.VisibleForTesting

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.client.api.v1.dto.Batch
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.METADATA_MAX_AGE
import org.apache.kyuubi.engine.{ApplicationInfo, ApplicationState}
import org.apache.kyuubi.engine.spark.SparkBatchProcessBuilder
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.server.metadata.api.{KubernetesEngineInfo, Metadata, MetadataFilter, SparkConnectSessionInfo}
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.session.SessionType
import org.apache.kyuubi.util.{ClassUtils, JdbcUtils, ThreadUtils}
import org.apache.kyuubi.util.ThreadUtils.scheduleTolerableRunnableWithFixedDelay

class MetadataManager extends AbstractService("MetadataManager") {
  import MetadataManager._

  private var _metadataStore: MetadataStore = _

  // Visible for testing.
  private[metadata] val identifierRequestsAsyncRetryRefs =
    new ConcurrentHashMap[String, MetadataRequestsRetryRef]()

  // Visible for testing.
  private[metadata] val identifierRequestsAsyncRetryingCounts =
    new ConcurrentHashMap[String, AtomicInteger]()

  private lazy val requestsRetryInterval =
    conf.get(KyuubiConf.METADATA_REQUEST_RETRY_INTERVAL)

  private lazy val requestsAsyncRetryEnabled =
    conf.get(KyuubiConf.METADATA_REQUEST_ASYNC_RETRY_ENABLED)

  private lazy val requestsAsyncRetryTrigger =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("metadata-requests-async-retry-trigger")

  private lazy val requestsAsyncRetryExecutor: ThreadPoolExecutor =
    ThreadUtils.newDaemonFixedThreadPool(
      conf.get(KyuubiConf.METADATA_REQUEST_ASYNC_RETRY_THREADS),
      "metadata-requests-async-retry")

  private lazy val cleanerEnabled = conf.get(KyuubiConf.METADATA_CLEANER_ENABLED)

  private lazy val metadataCleaner =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("metadata-cleaner")

  override def initialize(conf: KyuubiConf): Unit = {
    _metadataStore = MetadataManager.createMetadataStore(conf)
    super.initialize(conf)
  }

  override def start(): Unit = {
    super.start()
    if (requestsAsyncRetryEnabled) {
      startMetadataRequestsAsyncRetryTrigger()
    }
    if (cleanerEnabled) {
      startMetadataCleaner()
    }
  }

  override def stop(): Unit = {
    if (requestsAsyncRetryEnabled) {
      ThreadUtils.shutdown(requestsAsyncRetryTrigger)
      ThreadUtils.shutdown(requestsAsyncRetryExecutor)
    }
    if (cleanerEnabled) {
      ThreadUtils.shutdown(metadataCleaner)
    }
    _metadataStore.close()
    super.stop()
  }

  private def withMetadataRequestMetrics[T](block: => T): T = {
    MetricsSystem.tracing(_.incCount(MetricsConstants.METADATA_REQUEST_OPENED))
    try {
      block
    } catch {
      case e: Throwable =>
        MetricsSystem.tracing(_.markMeter(MetricsConstants.METADATA_REQUEST_FAIL))
        throw e
    } finally {
      MetricsSystem.tracing(_.decCount(MetricsConstants.METADATA_REQUEST_OPENED))
      MetricsSystem.tracing(_.markMeter(MetricsConstants.METADATA_REQUEST_TOTAL))
    }
  }

  protected def unrecoverableDBErr(cause: Throwable): Boolean = {
    // cover other cases in the future
    JdbcUtils.isDuplicatedKeyDBErr(cause)
  }

  def insertMetadata(metadata: Metadata, asyncRetryOnError: Boolean = true): Unit = {
    try {
      withMetadataRequestMetrics(_metadataStore.insertMetadata(metadata))
    } catch {
      // stop to retry when encounter duplicated key error.
      case rethrow: Throwable if unrecoverableDBErr(rethrow) =>
        throw rethrow
      case e: Throwable if requestsAsyncRetryEnabled && asyncRetryOnError =>
        error(s"Error inserting metadata for session ${metadata.identifier}", e)
        addMetadataRetryRequest(InsertMetadata(metadata))
    }
  }

  def getBatch(batchId: String): Option[Batch] = {
    getBatchSessionMetadata(batchId).map(buildBatch)
  }

  def getBatchSessionMetadata(batchId: String): Option[Metadata] = {
    Option(withMetadataRequestMetrics(_metadataStore.getMetadata(batchId)))
      .filter(_.sessionType == SessionType.BATCH)
  }

  def getKubernetesApplicationInfo(identifier: String): Option[ApplicationInfo] = {
    Option(withMetadataRequestMetrics(_metadataStore.getKubernetesMetaEngineInfo(identifier))).map {
      metadata =>
        val appInfo = buildApplicationInfo(metadata)
        info(s"Retrieve $appInfo from kubernetes engine info store: $metadata")
        appInfo
    }
  }

  def getBatches(
      filter: MetadataFilter,
      from: Int,
      size: Int,
      desc: Boolean = false): Seq[Batch] = {
    withMetadataRequestMetrics(_metadataStore.getMetadataList(
      filter,
      from,
      size,
      // if create_file field is set, order by create_time, which is faster, otherwise by key_id
      orderBy = if (filter.createTime > 0) Some("create_time") else Some("key_id"),
      direction = if (desc) "DESC" else "ASC")).map(buildBatch)
  }

  def countBatch(
      batchType: String,
      batchUser: String,
      batchState: String,
      kyuubiInstance: String): Int = {
    val filter = MetadataFilter(
      sessionType = SessionType.BATCH,
      engineType = batchType,
      username = batchUser,
      state = batchState,
      kyuubiInstance = kyuubiInstance)
    withMetadataRequestMetrics(_metadataStore.countMetadata(filter))
  }

  def pickBatchForSubmitting(kyuubiInstance: String): Option[Metadata] =
    withMetadataRequestMetrics(_metadataStore.pickMetadata(kyuubiInstance))

  def cancelUnscheduledBatch(batchId: String): Boolean = {
    _metadataStore.transformMetadataState(batchId, "INITIALIZED", "CANCELED")
  }

  def getBatchesRecoveryMetadata(
      state: String,
      kyuubiInstance: String,
      from: Int,
      size: Int): Seq[Metadata] = {
    val filter = MetadataFilter(
      sessionType = SessionType.BATCH,
      state = state,
      kyuubiInstance = kyuubiInstance)
    withMetadataRequestMetrics(_metadataStore.getMetadataList(filter, from, size))
  }

  def getPeerInstanceClosedBatchesMetadata(
      state: String,
      kyuubiInstance: String,
      from: Int,
      size: Int): Seq[Metadata] = {
    val filter = MetadataFilter(
      sessionType = SessionType.BATCH,
      state = state,
      kyuubiInstance = kyuubiInstance,
      peerInstanceClosed = true)
    withMetadataRequestMetrics(_metadataStore.getMetadataList(filter, from, size))
  }

  def updateMetadata(metadata: Metadata, asyncRetryOnError: Boolean = true): Unit = {
    try {
      withMetadataRequestMetrics(_metadataStore.updateMetadata(metadata))
    } catch {
      case e: Throwable if requestsAsyncRetryEnabled && asyncRetryOnError =>
        error(s"Error updating metadata for session ${metadata.identifier}", e)
        addMetadataRetryRequest(UpdateMetadata(metadata))
    }
  }

  def transferBatchOwnership(
      identifier: String,
      fromKyuubiInstance: String,
      toKyuubiInstance: String): Boolean =
    withMetadataRequestMetrics(
      _metadataStore.transferMetadataOwnership(identifier, fromKyuubiInstance, toKyuubiInstance))

  def upsertKubernetesMetadata(kubernetesEngineInfo: KubernetesEngineInfo): Unit = {
    try {
      withMetadataRequestMetrics(_metadataStore.upsertKubernetesEngineInfo(kubernetesEngineInfo))
    } catch {
      case e: Throwable =>
        error(
          s"Error updating kubernetes engine info for session ${kubernetesEngineInfo.identifier}",
          e)
    }
  }

  def cleanupMetadataById(batchId: String): Unit = {
    withMetadataRequestMetrics(_metadataStore.cleanupMetadataByIdentifier(batchId))
  }

  def cleanupKubernetesMetadataById(identifier: String): Unit = {
    withMetadataRequestMetrics(_metadataStore.cleanupKubernetesEngineInfoByIdentifier(identifier))
  }

  def insertSparkConnectSession(sessionInfo: SparkConnectSessionInfo): Unit = {
    withMetadataRequestMetrics(_metadataStore.insertSparkConnectSession(sessionInfo))
  }

  def getSparkConnectSessionByTokenId(tokenId: String): Option[SparkConnectSessionInfo] = {
    withMetadataRequestMetrics(_metadataStore.getSparkConnectSessionByTokenId(tokenId))
  }

  def cleanupSparkConnectSessionBySessionId(sessionId: String): Unit = {
    withMetadataRequestMetrics(_metadataStore.cleanupSparkConnectSessionBySessionId(sessionId))
  }

  private def startMetadataCleaner(): Unit = {
    val stateMaxAge = conf.get(METADATA_MAX_AGE)
    val interval = conf.get(KyuubiConf.METADATA_CLEANER_INTERVAL)
    val batchSize = conf.get(KyuubiConf.METADATA_CLEANER_BATCH_SIZE)
    val batchInterval = conf.get(KyuubiConf.METADATA_CLEANER_BATCH_INTERVAL)
    val cleanerTask: Runnable = () => {
      try {
        cleanupMetadata(stateMaxAge, batchSize, batchInterval)
      } catch {
        case e: Throwable => error("Error cleaning up the metadata by age", e)
      }
      // Same schedule/process that ages batches out of the UI also removes their submit logs.
      Utils.tryLogNonFatalError(cleanupOrphanedBatchSubmitLogs())
    }

    scheduleTolerableRunnableWithFixedDelay(
      metadataCleaner,
      cleanerTask,
      interval,
      interval,
      TimeUnit.MILLISECONDS)
  }

  @VisibleForTesting
  private[metadata] def cleanupMetadata(maxAge: Long, batchSize: Int, batchInterval: Long): Unit = {
    var needToCleanMetadata = true
    var needToCleanKubernetesInfo = true
    var needToCleanSparkConnectSession = true
    var cleanupLoop = 0

    val MAX_CLEANUP_LOOPS = 100 // a guard in case it runs into an infinite loop
    while ((needToCleanMetadata || needToCleanKubernetesInfo || needToCleanSparkConnectSession) &&
      cleanupLoop < MAX_CLEANUP_LOOPS) {
      cleanupLoop += 1
      if (needToCleanMetadata) {
        needToCleanMetadata =
          withMetadataRequestMetrics(_metadataStore.cleanupMetadataByAge(
            maxAge,
            batchSize)) >= batchSize
      }
      if (needToCleanKubernetesInfo) {
        needToCleanKubernetesInfo =
          withMetadataRequestMetrics(_metadataStore.cleanupKubernetesEngineInfoByAge(
            maxAge,
            batchSize)) >= batchSize
      }
      if (needToCleanSparkConnectSession) {
        needToCleanSparkConnectSession =
          withMetadataRequestMetrics(_metadataStore.cleanupSparkConnectSessionByAge(
            maxAge,
            batchSize)) >= batchSize
      }
      if (needToCleanMetadata || needToCleanKubernetesInfo || needToCleanSparkConnectSession) {
        if (cleanupLoop < MAX_CLEANUP_LOOPS) {
          info(s"Sleep $batchInterval ms before next batch of metadata cleaning.")
          Thread.sleep(batchInterval)
        } else {
          warn(s"Metadata cleaning reaches the maximum loop $MAX_CLEANUP_LOOPS, " +
            s"will continue in the next round.")
        }
      }
    }
  }

  /**
   * Delete spark-submit logs (`kyuubi-spark-batch-submit-<batchId>.log`, written by
   * [[SparkBatchProcessBuilder]]) whose batch has left the metastore -- i.e. its row was already
   * removed by the age-based metadata cleanup (so it has disappeared from the UI), or it never
   * had a record (orphaned by a crash before insert). Runs on the metadata-cleaner schedule.
   * The log is local to the instance that submitted the batch, so each instance only ever
   * deletes its own files; deletion for a batch owned by a peer is a no-op here. Best effort:
   * never throws.
   */
  private[metadata] def cleanupOrphanedBatchSubmitLogs(): Unit = {
    sys.env.get("KYUUBI_WORK_DIR_ROOT").map(Paths.get(_)).filter(p => Files.isDirectory(p))
      .foreach(cleanupOrphanedBatchSubmitLogsUnder)
  }

  private[metadata] def cleanupOrphanedBatchSubmitLogsUnder(workRoot: Path): Unit = {
    val prefix = s"${SparkBatchProcessBuilder.BATCH_SUBMIT_LOG_MODULE}-"
    val suffix = ".log"
    withDirectoryStream(workRoot) { userDirs =>
      userDirs.asScala.filter(p => Files.isDirectory(p)).foreach { userDir =>
        withDirectoryStream(userDir, s"$prefix*$suffix") { logs =>
          logs.asScala.foreach { logPath =>
            val batchId = logPath.getFileName.toString.stripPrefix(prefix).stripSuffix(suffix)
            if (batchId.nonEmpty && _metadataStore.getMetadata(batchId) == null) {
              if (Files.deleteIfExists(logPath)) {
                info(s"Cleaned up orphaned batch submit log $logPath")
              }
            }
          }
        }
      }
    }
  }

  private def withDirectoryStream[T](dir: Path, glob: String = "*")(
      f: java.nio.file.DirectoryStream[Path] => T): Unit = {
    Utils.tryLogNonFatalError {
      val stream = Files.newDirectoryStream(dir, glob)
      try f(stream)
      finally stream.close()
    }
  }

  def addMetadataRetryRequest(request: MetadataRequest): Unit = {
    val maxRequestsAsyncRetryRefs: Int =
      conf.get(KyuubiConf.METADATA_REQUEST_ASYNC_RETRY_QUEUE_SIZE)
    if (identifierRequestsAsyncRetryRefs.size() > maxRequestsAsyncRetryRefs) {
      throw new KyuubiException(
        "The number of metadata requests retry instances exceeds the limitation:" +
          maxRequestsAsyncRetryRefs)
    }
    val identifier = request.metadata.identifier
    val ref = identifierRequestsAsyncRetryRefs.computeIfAbsent(
      identifier,
      identifier => {
        val ref = new MetadataRequestsRetryRef
        debug(s"Created MetadataRequestsRetryRef for session $identifier.")
        ref
      })
    ref.addRetryingMetadataRequest(request)
    identifierRequestsAsyncRetryRefs.putIfAbsent(identifier, ref)
    MetricsSystem.tracing(_.markMeter(MetricsConstants.METADATA_REQUEST_RETRYING))
  }

  def getMetadataRequestsRetryRef(identifier: String): MetadataRequestsRetryRef = {
    identifierRequestsAsyncRetryRefs.get(identifier)
  }

  def deRegisterRequestsRetryRef(identifier: String): Unit = {
    identifierRequestsAsyncRetryRefs.remove(identifier)
    identifierRequestsAsyncRetryingCounts.remove(identifier)
  }

  private def startMetadataRequestsAsyncRetryTrigger(): Unit = {
    val triggerTask = new Runnable {
      override def run(): Unit = {
        identifierRequestsAsyncRetryRefs.forEach { (id, ref) =>
          if (!ref.hasRemainingRequests()) {
            identifierRequestsAsyncRetryRefs.remove(id)
            identifierRequestsAsyncRetryingCounts.remove(id)
          } else {
            val retryingCount = identifierRequestsAsyncRetryingCounts
              .computeIfAbsent(id, _ => new AtomicInteger(0))

            if (retryingCount.get() == 0) {
              val retryTask = new Runnable {
                override def run(): Unit = {
                  try {
                    info(s"Retrying metadata requests for $id")
                    var request = ref.metadataRequests.peek()
                    while (request != null) {
                      request match {
                        case insert: InsertMetadata =>
                          insertMetadata(insert.metadata, asyncRetryOnError = false)
                        case update: UpdateMetadata =>
                          updateMetadata(update.metadata, asyncRetryOnError = false)
                      }
                      ref.metadataRequests.remove(request)
                      MetricsSystem.tracing(_.markMeter(
                        MetricsConstants.METADATA_REQUEST_RETRYING,
                        -1L))
                      request = ref.metadataRequests.peek()
                    }
                  } catch {
                    case e: Throwable =>
                      error(s"Error retrying metadata requests for $id", e)
                  } finally {
                    retryingCount.decrementAndGet()
                  }
                }
              }

              try {
                retryingCount.incrementAndGet()
                requestsAsyncRetryExecutor.submit(retryTask)
              } catch {
                case e: Throwable =>
                  error(s"Error submitting metadata retry requests for $id", e)
                  retryingCount.decrementAndGet()
              }
            }
          }
        }
      }
    }

    scheduleTolerableRunnableWithFixedDelay(
      requestsAsyncRetryTrigger,
      triggerTask,
      requestsRetryInterval,
      requestsRetryInterval,
      TimeUnit.MILLISECONDS)
  }
}

object MetadataManager extends Logging {
  def createMetadataStore(conf: KyuubiConf): MetadataStore = {
    val className = conf.get(KyuubiConf.METADATA_STORE_CLASS)
    if (className.isEmpty) {
      throw new KyuubiException(
        s"${KyuubiConf.METADATA_STORE_CLASS.key} cannot be empty.")
    }
    ClassUtils.createInstance(className, classOf[MetadataStore], conf)
  }

  def buildBatch(batchMetadata: Metadata): Batch = {
    val batchState =
      if (batchMetadata.peerInstanceClosed &&
        !OperationState.isTerminal(OperationState.withName(batchMetadata.state))) {
        OperationState.CANCELED.toString
      } else {
        batchMetadata.state
      }

    val name = Option(batchMetadata.requestName).getOrElse(Option(batchMetadata.engineName).orNull)

    new Batch(
      batchMetadata.identifier,
      batchMetadata.username,
      batchMetadata.engineType,
      name,
      batchMetadata.engineOpenTime,
      batchMetadata.engineId,
      batchMetadata.engineUrl,
      batchMetadata.engineState,
      batchMetadata.engineError.orNull,
      batchMetadata.kyuubiInstance,
      batchState,
      batchMetadata.createTime,
      batchMetadata.endTime,
      Map.empty[String, String].asJava)
  }

  def buildApplicationInfo(metadata: KubernetesEngineInfo): ApplicationInfo = {
    ApplicationInfo(
      id = metadata.engineId,
      name = metadata.engineName,
      state = ApplicationState.withName(metadata.engineState),
      error = metadata.engineError,
      podName = Option(metadata.podName))
  }
}
