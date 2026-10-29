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

package com.mongodb.spark.sql.connector.testfixture

import java.util

import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * DSv2 `Table` fixture whose fully-qualified class name lives under the same
 * package prefix (`com.mongodb.spark.sql.connector.`) that
 * `AccessResource.CATALOGLESS_TABLE_ALIASES` aliases under the friendly name
 * "mongodb". Used by `DataSourceV2RelationTableExtractorSuite` to exercise the
 * class-name alias skip path without adding the real MongoDB Spark Connector
 * as a test dependency.
 */
class MongoTableFixture extends Table {
  override def name(): String = "MongoTableFixture()"

  override def schema(): StructType =
    StructType(Seq(StructField("_id", StringType)))

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.BATCH_READ)
}
