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

import java.io.Closeable

import org.apache.kyuubi.server.metadata.api.{KubernetesEngineInfo, Metadata, MetadataFilter, SparkConnectSessionInfo}

trait MetadataStore extends Closeable {

  /**
   * Insert a metadata into metadata store.
   */
  def insertMetadata(metadata: Metadata): Unit

  /**
   * Find unscheduled batch job metadata and pick up it to submit.
   * @param kyuubiInstance the Kyuubi instance picked batch job
   * @return selected metadata for submitting or None if no sufficient items
   */
  def pickMetadata(kyuubiInstance: String): Option[Metadata]

  /**
   * Transfer state of metadata from the existing state to another
   * @param identifier the identifier.
   * @param fromState the desired current state
   * @param targetState the desired target state
   * @return `true` if the metadata state was same as `fromState`, and successfully
   *         transitioned to `targetState`, otherwise `false` is returned
   */
  def transformMetadataState(identifier: String, fromState: String, targetState: String): Boolean

  /**
   * Get the persisted metadata by batch identifier.
   * @param identifier the identifier.
   * @return selected metadata.
   */
  def getMetadata(identifier: String): Metadata

  /**
   * Get the metadata list with filter conditions, offset and size.
   * @param filter the metadata filter conditions.
   * @param from the metadata offset.
   * @param size the size to get.
   * @param desc the order of metadata list.
   * @param orderBy the order by column, default is the auto increment primary key, `key_id`.
   * @param direction the order direction, default is `ASC`.
   * @return selected metadata list.
   */
  def getMetadataList(
      filter: MetadataFilter,
      from: Int,
      size: Int,
      orderBy: Option[String] = Some("key_id"),
      direction: String = "ASC"): Seq[Metadata]

  /**
   * Count the metadata list with filter conditions.
   * @param filter the metadata filter conditions.
   * @return the count of metadata satisfied the filter condition.
   */
  def countMetadata(filter: MetadataFilter): Int

  /**
   * Update the metadata according to identifier.
   * Note that, it will only update the state and engine related metadata.
   */
  def updateMetadata(metadata: Metadata): Unit

  /**
   * Atomically transfer ownership of a metadata record to another kyuubi instance, conditioned on
   * the record still being owned by fromKyuubiInstance. Returns true iff this call performed the
   * transfer. This is the safe primitive for reclaiming a dead instance's batches: even if several
   * live instances race to take over the same batch, only one wins the compare-and-set.
   */
  def transferMetadataOwnership(
      identifier: String,
      fromKyuubiInstance: String,
      toKyuubiInstance: String): Boolean

  /**
   * Upsert the kubernetes engine info. Insert if not exists, otherwise update.
   */
  def upsertKubernetesEngineInfo(engineInfo: KubernetesEngineInfo): Unit

  /**
   * Get the persisted kubernetes engine info by unique identifier.
   * @param identifier the identifier.
   * @return selected engine info.
   */
  def getKubernetesMetaEngineInfo(identifier: String): KubernetesEngineInfo

  /**
   * Cleanup meta data by identifier.
   */
  def cleanupMetadataByIdentifier(identifier: String): Unit

  /**
   * Check and cleanup the terminated batches information with maxAge limitation.
   * @param maxAge the batch state info maximum age.
   * @param limit the maximum number of metadata to be cleaned up.
   */
  def cleanupMetadataByAge(maxAge: Long, limit: Int): Int

  /**
   * Cleanup kubernetes engine info by identifier.
   */
  def cleanupKubernetesEngineInfoByIdentifier(identifier: String): Unit

  /**
   * Check and cleanup the kubernetes engine info with maxAge limitation.
   * @param maxAge the kubernetes engine info maximum age.
   * @param limit the maximum number of kubernetes engine info to be cleaned up.
   */
  def cleanupKubernetesEngineInfoByAge(maxAge: Long, limit: Int): Int

  /**
   * Persist the record binding a user to their Spark Connect engine.
   */
  def insertSparkConnectSession(sessionInfo: SparkConnectSessionInfo): Unit

  /**
   * Look up the Spark Connect engine bound to a user.
   * @return the record, or [[None]] when the user has no engine.
   */
  def getSparkConnectSessionByUserName(userName: String): Option[SparkConnectSessionInfo]

  /**
   * Clear the session handle on a Spark Connect record, leaving the engine binding in place.
   * Called when the session closes; the engine outlives it and the user's next session is handed
   * the same driver by engine discovery.
   */
  def detachSparkConnectSessionBySessionId(sessionId: String): Unit

  /**
   * Drop a user's Spark Connect record outright.
   */
  def cleanupSparkConnectSessionByUserName(userName: String): Unit

  /**
   * Check and cleanup Spark Connect records with maxAge limitation. An engine stops on its own
   * once idle, so this reclaims bindings whose driver is long gone, as well as rows orphaned by
   * an instance that died mid-session.
   * @param maxAge the record maximum age.
   * @param limit the maximum number of records to be cleaned up.
   */
  def cleanupSparkConnectSessionByAge(maxAge: Long, limit: Int): Int
}
