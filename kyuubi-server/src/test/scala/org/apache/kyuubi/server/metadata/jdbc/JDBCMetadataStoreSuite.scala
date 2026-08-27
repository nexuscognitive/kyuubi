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

package org.apache.kyuubi.server.metadata.jdbc

import java.sql.DriverManager
import java.util.UUID

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ApplicationState
import org.apache.kyuubi.server.metadata.MetadataManager
import org.apache.kyuubi.server.metadata.api.{KubernetesEngineInfo, Metadata, MetadataFilter, SparkConnectDriverContainerExit, SparkConnectDriverEventRecord, SparkConnectDriverPostMortem, SparkConnectRecoveryState, SparkConnectSessionInfo}
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf._
import org.apache.kyuubi.session.SessionType

class JDBCMetadataStoreSuite extends KyuubiFunSuite {
  private val conf = KyuubiConf()
    .set(METADATA_STORE_JDBC_DATABASE_TYPE, DatabaseType.SQLITE.toString)
    .set(METADATA_STORE_JDBC_DATABASE_SCHEMA_INIT, true)
    .set(s"$METADATA_STORE_JDBC_DATASOURCE_PREFIX.connectionTimeout", "3000")
    .set(s"$METADATA_STORE_JDBC_DATASOURCE_PREFIX.maximumPoolSize", "99")
    .set(s"$METADATA_STORE_JDBC_DATASOURCE_PREFIX.idleTimeout", "60000")
  private val jdbcMetadataStore = new JDBCMetadataStore(conf)

  override def afterAll(): Unit = {
    super.afterAll()
    jdbcMetadataStore.getMetadataList(MetadataFilter(), 0, Int.MaxValue).foreach {
      batch =>
        jdbcMetadataStore.cleanupMetadataByIdentifier(batch.identifier)
    }
    jdbcMetadataStore.cleanupKubernetesEngineInfoByAge(0, Int.MaxValue)
    jdbcMetadataStore.close()
  }

  test("test jdbc datasource properties") {
    assert(jdbcMetadataStore.hikariDataSource.getConnectionTimeout == 3000)
    assert(jdbcMetadataStore.hikariDataSource.getMaximumPoolSize == 99)
    assert(jdbcMetadataStore.hikariDataSource.getIdleTimeout == 60000)
  }

  test("test get init schema stream") {
    assert(jdbcMetadataStore.getInitSchema(DatabaseType.MYSQL).isDefined)
    assert(jdbcMetadataStore.getInitSchema(DatabaseType.POSTGRESQL).isDefined)
    assert(jdbcMetadataStore.getInitSchema(DatabaseType.CUSTOM).isEmpty)
  }

  test("jdbc metadata store") {
    val batchId = UUID.randomUUID().toString
    val kyuubiInstance = "localhost:10099"
    var batchMetadata = Metadata(
      identifier = batchId,
      sessionType = SessionType.BATCH,
      realUser = "kyuubi",
      username = "kyuubi",
      ipAddress = "127.0.0.1",
      kyuubiInstance = kyuubiInstance,
      state = "PENDING",
      resource = "intern",
      className = "org.apache.kyuubi.SparkWC",
      requestName = "kyuubi_batch",
      requestConf = Map("spark.master" -> "local"),
      requestArgs = Seq("100"),
      createTime = System.currentTimeMillis(),
      engineType = "spark",
      clusterManager = Some("local"))

    jdbcMetadataStore.insertMetadata(batchMetadata)

    // the engine type is formatted with UPPER
    batchMetadata = batchMetadata.copy(engineType = "SPARK")
    assert(jdbcMetadataStore.getMetadata(batchId) == batchMetadata)

    jdbcMetadataStore.cleanupMetadataByIdentifier(batchId)
    assert(jdbcMetadataStore.getMetadata(batchId) == null)

    jdbcMetadataStore.insertMetadata(batchMetadata)

    val batchState2 = batchMetadata.copy(identifier = UUID.randomUUID().toString)
    jdbcMetadataStore.insertMetadata(batchState2)

    var batches =
      jdbcMetadataStore.getMetadataList(
        MetadataFilter(
          sessionType = SessionType.BATCH,
          engineType = "Spark"),
        0,
        1)
    assert(batches == Seq(batchMetadata))

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        engineType = "Spark",
        username = "kyuubi"),
      0,
      Int.MaxValue)
    assert(batches == Seq(batchMetadata, batchState2))

    jdbcMetadataStore.cleanupMetadataByIdentifier(batchState2.identifier)

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.INTERACTIVE,
        engineType = "Spark",
        username = "kyuubi",
        state = "PENDING"),
      0,
      Int.MaxValue)
    assert(batches.isEmpty)

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        engineType = "Spark",
        username = "kyuubi",
        state = "PENDING"),
      0,
      Int.MaxValue)
    assert(batches == Seq(batchMetadata))

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        engineType = "Spark",
        username = "kyuubi",
        state = "RUNNING"),
      0,
      Int.MaxValue)
    assert(batches.isEmpty)

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        engineType = "Spark",
        username = "no_kyuubi",
        state = "PENDING"),
      0,
      Int.MaxValue)
    assert(batches.isEmpty)

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        engineType = "SPARK",
        state = "PENDING"),
      0,
      Int.MaxValue)
    assert(batches == Seq(batchMetadata))

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(sessionType = SessionType.BATCH),
      0,
      Int.MaxValue)
    assert(batches == Seq(batchMetadata))

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        peerInstanceClosed = true),
      0,
      Int.MaxValue)
    assert(batches.isEmpty)

    jdbcMetadataStore.updateMetadata(Metadata(
      identifier = batchMetadata.identifier,
      peerInstanceClosed = true))

    batchMetadata = batchMetadata.copy(peerInstanceClosed = true)

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        peerInstanceClosed = true),
      0,
      Int.MaxValue)
    assert(batches === Seq(batchMetadata))

    var batchesToRecover = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        state = "PENDING",
        kyuubiInstance = kyuubiInstance),
      0,
      Int.MaxValue)
    assert(batchesToRecover == Seq(batchMetadata))

    batchesToRecover = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        state = "RUNNING",
        kyuubiInstance = kyuubiInstance),
      0,
      Int.MaxValue)
    assert(batchesToRecover.isEmpty)

    var newBatchState = batchMetadata.copy(
      state = "RUNNING",
      engineId = "app_id",
      engineName = "app_name",
      engineUrl = "app_url",
      engineState = "RUNNING",
      engineError = None)
    jdbcMetadataStore.updateMetadata(newBatchState)
    assert(jdbcMetadataStore.getMetadata(batchId) == newBatchState)

    newBatchState = newBatchState.copy(state = "FINISHED", endTime = System.currentTimeMillis())
    jdbcMetadataStore.updateMetadata(newBatchState)

    assert(jdbcMetadataStore.getMetadata(batchId) == newBatchState)
    assert(jdbcMetadataStore.countMetadata(MetadataFilter()) > 0)

    assert(jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        state = "PENDING",
        kyuubiInstance = kyuubiInstance),
      0,
      Int.MaxValue).isEmpty)

    assert(jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        state = "RUNNING",
        kyuubiInstance = kyuubiInstance),
      0,
      Int.MaxValue).isEmpty)

    eventually(Timeout(3.seconds)) {
      jdbcMetadataStore.cleanupMetadataByAge(1000, Int.MaxValue)
      assert(jdbcMetadataStore.getMetadata(batchId) == null)
    }
  }

  test("transformMetadataState should transition state correctly") {
    val batchId = UUID.randomUUID().toString
    val batchMetadata = Metadata(
      identifier = batchId,
      sessionType = SessionType.BATCH,
      realUser = "kyuubi",
      username = "kyuubi",
      ipAddress = "127.0.0.1",
      state = "INITIALIZED",
      resource = "intern",
      className = "org.apache.kyuubi.SparkWC",
      requestName = "test_transform",
      createTime = System.currentTimeMillis(),
      engineType = "SPARK")

    jdbcMetadataStore.insertMetadata(batchMetadata)

    val result = jdbcMetadataStore.transformMetadataState(batchId, "INITIALIZED", "CANCELED")
    assert(result, "should successfully transition from INITIALIZED to CANCELED")

    val metadata = jdbcMetadataStore.getMetadata(batchId)
    assert(metadata.state == "CANCELED", s"state should be CANCELED but was ${metadata.state}")

    jdbcMetadataStore.cleanupMetadataByIdentifier(batchId)
  }

  test("throw exception if update count is 0") {
    val metadata = Metadata(identifier = UUID.randomUUID().toString, state = "RUNNING")
    intercept[KyuubiException] {
      jdbcMetadataStore.updateMetadata(metadata)
    }
  }

  test("updateMetadata drops regressive state changes (monotonic + terminal freeze)") {
    val id = UUID.randomUUID().toString
    jdbcMetadataStore.insertMetadata(Metadata(
      identifier = id,
      sessionType = SessionType.BATCH,
      realUser = "kyuubi",
      username = "kyuubi",
      ipAddress = "127.0.0.1",
      kyuubiInstance = "localhost:10099",
      state = "PENDING",
      createTime = System.currentTimeMillis(),
      engineType = "spark"))

    // forward transition applies
    jdbcMetadataStore.updateMetadata(Metadata(identifier = id, state = "RUNNING"))
    assert(jdbcMetadataStore.getMetadata(id).state == "RUNNING")

    // backward transition (RUNNING -> PENDING) is dropped without error
    jdbcMetadataStore.updateMetadata(Metadata(identifier = id, state = "PENDING"))
    assert(jdbcMetadataStore.getMetadata(id).state == "RUNNING")

    // reach a terminal state
    jdbcMetadataStore.updateMetadata(Metadata(identifier = id, state = "FINISHED"))
    assert(jdbcMetadataStore.getMetadata(id).state == "FINISHED")

    // terminal is frozen: a stale RUNNING (late async-retry replay / post-takeover
    // zombie) is dropped
    jdbcMetadataStore.updateMetadata(Metadata(identifier = id, state = "RUNNING"))
    assert(jdbcMetadataStore.getMetadata(id).state == "FINISHED")

    // terminal is frozen: a resubmit-failure ERROR cannot overwrite the real FINISHED state
    jdbcMetadataStore.updateMetadata(Metadata(identifier = id, state = "ERROR"))
    assert(jdbcMetadataStore.getMetadata(id).state == "FINISHED")

    // non-state fields can still be updated on a terminal row (e.g. peer-close bookkeeping)
    jdbcMetadataStore.updateMetadata(Metadata(identifier = id, peerInstanceClosed = true))
    assert(jdbcMetadataStore.getMetadata(id).peerInstanceClosed)
    assert(jdbcMetadataStore.getMetadata(id).state == "FINISHED")

    jdbcMetadataStore.cleanupMetadataByIdentifier(id)
  }

  test("migrateSchema adds the version column to an existing (older) metadata table") {
    val dbFile = Utils.createTempDir().resolve("old_schema.db")
    val url = s"jdbc:sqlite:$dbFile"
    // Simulate a database created by an older Kyuubi: a metadata table WITHOUT the version column.
    val conn = DriverManager.getConnection(url)
    try {
      val st = conn.createStatement()
      st.executeUpdate(
        """CREATE TABLE metadata(
          |  key_id INTEGER PRIMARY KEY AUTOINCREMENT,
          |  identifier varchar(36) NOT NULL,
          |  session_type varchar(32) NOT NULL,
          |  real_user varchar(255) NOT NULL,
          |  user_name varchar(255) NOT NULL,
          |  ip_address varchar(128),
          |  kyuubi_instance varchar(1024),
          |  state varchar(128) NOT NULL,
          |  resource varchar(1024),
          |  class_name varchar(1024),
          |  request_name varchar(1024),
          |  request_conf mediumtext,
          |  request_args mediumtext,
          |  create_time BIGINT NOT NULL,
          |  engine_type varchar(32) NOT NULL,
          |  cluster_manager varchar(128),
          |  engine_open_time bigint,
          |  engine_id varchar(128),
          |  engine_name mediumtext,
          |  engine_url varchar(1024),
          |  engine_state varchar(32),
          |  engine_error mediumtext,
          |  end_time bigint,
          |  priority INTEGER NOT NULL DEFAULT 10,
          |  peer_instance_closed boolean default '0'
          |)""".stripMargin)
      st.close()
    } finally {
      conn.close()
    }

    val migratingConf = KyuubiConf()
      .set(METADATA_STORE_JDBC_DATABASE_TYPE, DatabaseType.SQLITE.toString)
      .set(METADATA_STORE_JDBC_DATABASE_SCHEMA_INIT, true)
      .set(METADATA_STORE_JDBC_URL, url)
    val store = new JDBCMetadataStore(migratingConf)
    try {
      // If migration didn't add `version`, the optimistic-CAS update (which reads/writes version)
      // would fail. A successful forward transition proves the column was added on startup.
      val id = UUID.randomUUID().toString
      store.insertMetadata(Metadata(
        identifier = id,
        sessionType = SessionType.BATCH,
        realUser = "kyuubi",
        username = "kyuubi",
        ipAddress = "127.0.0.1",
        state = "PENDING",
        createTime = System.currentTimeMillis(),
        engineType = "spark"))
      store.updateMetadata(Metadata(identifier = id, state = "RUNNING"))
      assert(store.getMetadata(id).state == "RUNNING")
    } finally {
      store.close()
    }
  }

  test("get schema urls with correct version ordering") {
    val url1 = "metadata-store-schema-1.7.0.mysql.sql"
    val url2 = "metadata-store-schema-1.7.1.mysql.sql"
    val url3 = "metadata-store-schema-1.8.0.mysql.sql"
    val url4 = "metadata-store-schema-1.10.0.mysql.sql"
    val url5 = "metadata-store-schema-2.1.0.mysql.sql"
    assert(jdbcMetadataStore.getSchemaVersion(url1) === ((1, 7, 0)))
    assert(jdbcMetadataStore.getSchemaVersion(url2) === ((1, 7, 1)))
    assert(jdbcMetadataStore.getSchemaVersion(url3) === ((1, 8, 0)))
    assert(jdbcMetadataStore.getSchemaVersion(url4) === ((1, 10, 0)))
    assert(jdbcMetadataStore.getSchemaVersion(url5) === ((2, 1, 0)))
    assert(jdbcMetadataStore.getLatestSchemaUrl(Seq(url1, url2, url3, url4)).get === url4)
    assert(jdbcMetadataStore.getLatestSchemaUrl(Seq(url1, url3, url4, url2)).get === url4)
    assert(jdbcMetadataStore.getLatestSchemaUrl(Seq(url1, url2, url3, url4, url5)).get === url5)
  }

  test("kubernetes engine info") {
    val tag = UUID.randomUUID().toString
    val metadata = KubernetesEngineInfo(
      identifier = tag,
      context = Some("context"),
      namespace = Some("namespace"),
      podName = "podName",
      podState = "podState",
      containerState = "containerState",
      engineId = "appId",
      engineName = "appName",
      engineState = "FINISHED",
      engineError = Some("appError"))

    jdbcMetadataStore.upsertKubernetesEngineInfo(metadata)

    val metadata2 = jdbcMetadataStore.getKubernetesMetaEngineInfo(tag)
    assert(metadata2.identifier == metadata.identifier)
    assert(metadata2.context == metadata.context)
    assert(metadata2.namespace == metadata.namespace)
    assert(metadata2.podName == metadata.podName)
    assert(metadata2.podState == metadata.podState)
    assert(metadata2.containerState == metadata.containerState)
    assert(metadata2.engineId == metadata.engineId)
    assert(metadata2.engineName == metadata.engineName)
    assert(metadata2.engineState == metadata.engineState)
    assert(metadata2.engineError == metadata.engineError)
    assert(metadata2.updateTime > 0)

    val metadata3 = KubernetesEngineInfo(
      identifier = tag,
      context = Some("context2"),
      namespace = Some("namespace2"),
      podName = "podName2",
      podState = "podState2",
      containerState = "containerState2",
      engineId = "appId2",
      engineName = "appName2",
      engineState = "FAILED",
      engineError = Some("appError2"))
    // update_time is stamped with the wall clock at write time, so back-to-back
    // upserts can land in the same millisecond and leave updateTime unchanged.
    // Wait for the clock to advance so the refresh below is observable.
    Thread.sleep(2)
    jdbcMetadataStore.upsertKubernetesEngineInfo(metadata3)

    val metadata4 = jdbcMetadataStore.getKubernetesMetaEngineInfo(tag)
    assert(metadata4.identifier == metadata3.identifier)
    assert(metadata4.context == metadata3.context)
    assert(metadata4.namespace == metadata3.namespace)
    assert(metadata4.podName == metadata3.podName)
    assert(metadata4.podState == metadata3.podState)
    assert(metadata4.containerState == metadata3.containerState)
    assert(metadata4.engineId == metadata3.engineId)
    assert(metadata4.engineName == metadata3.engineName)
    assert(metadata4.engineState == metadata3.engineState)
    assert(metadata4.engineError == metadata3.engineError)
    assert(metadata4.updateTime > metadata2.updateTime)

    val applicationInfo =
      MetadataManager.buildApplicationInfo(jdbcMetadataStore.getKubernetesMetaEngineInfo(tag))
    assert(applicationInfo.id == "appId2")
    assert(applicationInfo.name == "appName2")
    assert(applicationInfo.state == ApplicationState.FAILED)
    assert(applicationInfo.error == Some("appError2"))
    assert(applicationInfo.podName == Some("podName2"))

    jdbcMetadataStore.cleanupKubernetesEngineInfoByIdentifier(tag)
    assert(jdbcMetadataStore.getKubernetesMetaEngineInfo(tag) == null)
  }

  test("spark connect engine bindings are stored per user") {
    val sessionId = UUID.randomUUID().toString
    val sessionInfo = SparkConnectSessionInfo(
      userName = "connect_user",
      sessionId = sessionId,
      engineTag = sessionId,
      engineToken = "an-engine-credential",
      createTime = System.currentTimeMillis())
    jdbcMetadataStore.insertSparkConnectSession(sessionInfo)

    val persisted = jdbcMetadataStore.getSparkConnectSessionByUserName("connect_user")
    assert(persisted.contains(sessionInfo))
    assert(jdbcMetadataStore.getSparkConnectSessionByUserName("somebody_else").isEmpty)

    jdbcMetadataStore.cleanupSparkConnectSessionByUserName("connect_user")
    assert(jdbcMetadataStore.getSparkConnectSessionByUserName("connect_user").isEmpty)
  }

  test("closing a session detaches it but leaves the engine binding") {
    val sessionId = UUID.randomUUID().toString
    val sessionInfo = SparkConnectSessionInfo(
      userName = "detaching_user",
      sessionId = sessionId,
      engineTag = sessionId,
      engineToken = "an-engine-credential",
      createTime = System.currentTimeMillis())
    jdbcMetadataStore.insertSparkConnectSession(sessionInfo)

    jdbcMetadataStore.detachSparkConnectSessionBySessionId(sessionId)

    // The engine outlives its session, and the user's next session inherits its tag and
    // credential -- a reused driver keeps both, and neither can be changed from outside it.
    val detached = jdbcMetadataStore.getSparkConnectSessionByUserName("detaching_user")
    assert(detached.exists(!_.hasLiveSession))
    assert(detached.map(_.engineTag).contains(sessionId))
    assert(detached.map(_.engineToken).contains("an-engine-credential"))

    jdbcMetadataStore.cleanupSparkConnectSessionByUserName("detaching_user")
  }

  test("spark connect engine bindings are reclaimed by age") {
    val sessionId = UUID.randomUUID().toString
    jdbcMetadataStore.insertSparkConnectSession(SparkConnectSessionInfo(
      userName = "ageing_user",
      sessionId = sessionId,
      engineTag = sessionId,
      engineToken = "an-engine-credential",
      createTime = System.currentTimeMillis() - 60000))

    // Asserted on this row rather than on a row count: a detached binding lingers until this
    // sweep reaches it, so any earlier suite sharing the store contributes rows of its own.
    jdbcMetadataStore.cleanupSparkConnectSessionByAge(600000, Int.MaxValue)
    assert(jdbcMetadataStore.getSparkConnectSessionByUserName("ageing_user").isDefined)

    assert(jdbcMetadataStore.cleanupSparkConnectSessionByAge(1000, Int.MaxValue) >= 1)
    assert(jdbcMetadataStore.getSparkConnectSessionByUserName("ageing_user").isEmpty)
  }

  test("a binding is found by the engine tag a dying driver arrives with") {
    val sessionId = UUID.randomUUID().toString
    jdbcMetadataStore.insertSparkConnectSession(SparkConnectSessionInfo(
      userName = "tagged_user",
      sessionId = sessionId,
      engineTag = sessionId,
      engineToken = "an-engine-credential",
      createTime = System.currentTimeMillis()))

    // The only lookup a driver's death can make: the pod informer knows the tag and nothing else,
    // and the instance that observes it need never have served this user.
    val found = jdbcMetadataStore.getSparkConnectSessionByEngineTag(sessionId)
    assert(found.map(_.userName).contains("tagged_user"))
    // Every batch and Thrift engine reaches the same lookup, so a miss must be ordinary.
    assert(jdbcMetadataStore.getSparkConnectSessionByEngineTag("some-batch-engine").isEmpty)

    jdbcMetadataStore.cleanupSparkConnectSessionByUserName("tagged_user")
  }

  test("recovery bookkeeping and driver post-mortems survive a round trip") {
    val sessionId = UUID.randomUUID().toString
    jdbcMetadataStore.insertSparkConnectSession(SparkConnectSessionInfo(
      userName = "recovering_user",
      sessionId = sessionId,
      engineTag = sessionId,
      engineToken = "an-engine-credential",
      createTime = System.currentTimeMillis()))

    val postMortem = SparkConnectDriverPostMortem(
      engineTag = sessionId,
      capturedTime = 1700000600000L,
      driverName = "spark-connect-driver-1",
      location = "analytics",
      finalState = "Failed",
      applicationState = "FAILED",
      reason = None,
      message = Some("the node was low on memory"),
      containers = Seq(SparkConnectDriverContainerExit(
        name = "spark-kubernetes-driver",
        reason = Some("OOMKilled"),
        message = None,
        exitCode = Some(137),
        signal = Some(9),
        oomKilled = true,
        restartCount = 0,
        finishedAt = Some("2026-08-27T02:11:04Z"))),
      events = Seq(SparkConnectDriverEventRecord(
        eventType = "Warning",
        reason = "Evicted",
        message = "The node was low on resource: memory",
        count = 1,
        firstTimestamp = Some("2026-08-27T02:11:03Z"),
        lastTimestamp = Some("2026-08-27T02:11:03Z"))))

    val newSessionId = UUID.randomUUID().toString
    jdbcMetadataStore.updateSparkConnectSessionRecovery(SparkConnectSessionInfo(
      userName = "recovering_user",
      sessionId = newSessionId,
      engineTag = newSessionId,
      engineToken = "a-fresh-credential",
      generation = 1,
      restartCount = 1,
      lastRestartTime = 1700000700000L,
      recoveryState = SparkConnectRecoveryState.ABANDONED,
      recoveryMessage = Some("the driver died 4 times"),
      engineConf = Map("spark.executor.memory" -> "8g"),
      driverPostMortems = Seq(postMortem)))

    val persisted = jdbcMetadataStore.getSparkConnectSessionByUserName("recovering_user")
      .getOrElse(fail("the binding was not persisted"))
    assert(persisted.generation == 1)
    assert(persisted.restartCount == 1)
    assert(persisted.lastRestartTime == 1700000700000L)
    assert(persisted.isRecoveryAbandoned)
    assert(persisted.recoveryMessage.contains("the driver died 4 times"))
    // The conf a relaunched engine has to come up with.
    assert(persisted.engineConf == Map("spark.executor.memory" -> "8g"))
    // The point of the whole feature: this outlives the pod, its events, and this process.
    assert(persisted.driverPostMortems == Seq(postMortem))
    assert(persisted.latestPostMortem.exists(_.oomKilled))
    assert(persisted.latestPostMortem.map(_.summary).contains("OOMKilled (exit 137)"))
    assert(persisted.latestPostMortem.exists(_.events.exists(_.reason == "Evicted")))

    jdbcMetadataStore.cleanupSparkConnectSessionByUserName("recovering_user")
  }
}
