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

package org.apache.kyuubi.plugin.spark.authz.ranger.datamasking

import java.sql.DriverManager

import scala.util.Try

import org.apache.commons.codec.digest.DigestUtils.md5Hex
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSessionExtensions}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.spark.authz.{SparkSessionProvider, V2JdbcTableCatalogPrivilegesBuilderSuite}
import org.apache.kyuubi.plugin.spark.authz.RangerTestNamespace._
import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.derbyJdbcDriverClass

/**
 * Masking and row filtering must be looked up against the catalog the table actually lives
 * in. Two tables named `default.src` exist here, one in the session catalog (mapped onto
 * the `iceberg` Ranger catalog, where `src` is both masked and row-filtered) and one in a
 * v2 catalog mapped onto a Ranger catalog that grants access but defines no masking or
 * row-filter policy. Everything but the catalog is identical, so a lookup that resolves
 * the catalog to the default instead of the table's own applies the wrong catalog's
 * policies -- silently masking what should be plain, or, the dangerous direction, handing
 * back unmasked rows for a table that is masked in its own catalog.
 */
class DataMaskingCatalogScopeSuite extends KyuubiFunSuite with SparkSessionProvider {
  private val maskFreeSparkCatalog = "mask_free_cat"
  private val maskFreeRangerCatalog = "catalog_without_masking"

  override protected val catalogImpl: String = "in-memory"
  override protected val supportPurge: Boolean = false
  override protected val extension: SparkSessionExtensions => Unit = new RangerSparkExtension

  override protected def extraSparkConf: SparkConf = {
    super.extraSparkConf
      // see DataMaskingTestBase: the masking rule is not ANSI-compatible yet
      .set("spark.sql.ansi.enabled", "false")
      .set(
        s"spark.sql.catalog.$maskFreeSparkCatalog",
        V2JdbcTableCatalogPrivilegesBuilderSuite.v2JdbcTableCatalogClassName)
      .set(
        s"spark.sql.catalog.$maskFreeSparkCatalog.url",
        s"jdbc:derby:memory:$maskFreeSparkCatalog;create=true")
      .set(s"spark.sql.catalog.$maskFreeSparkCatalog.driver", derbyJdbcDriverClass)
      // the session catalog keeps its usual mapping onto the policies' `iceberg` catalog
      .set(
        "ranger.plugin.spark.catalog.mapping",
        s"$sparkCatalog:iceberg,$maskFreeSparkCatalog:$maskFreeRangerCatalog")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    doAs(admin) {
      sql(s"CREATE TABLE IF NOT EXISTS $sparkCatalog.$defaultDb.src" +
        " (key int, value1 string) USING parquet")
      sql(s"INSERT INTO $sparkCatalog.$defaultDb.src SELECT 1, 'hello'")
      sql(s"INSERT INTO $sparkCatalog.$defaultDb.src SELECT 30, 'world'")

      sql(s"CREATE TABLE IF NOT EXISTS $maskFreeSparkCatalog.$defaultDb.src" +
        " (key int, value1 string)")
      sql(s"INSERT INTO $maskFreeSparkCatalog.$defaultDb.src SELECT 1, 'hello'")
      sql(s"INSERT INTO $maskFreeSparkCatalog.$defaultDb.src SELECT 30, 'world'")
    }
  }

  override def afterAll(): Unit = {
    doAs(admin) {
      sql(s"DROP TABLE IF EXISTS $sparkCatalog.$defaultDb.src")
      sql(s"DROP TABLE IF EXISTS $maskFreeSparkCatalog.$defaultDb.src")
    }
    spark.stop()
    Try(DriverManager.getConnection(s"jdbc:derby:memory:$maskFreeSparkCatalog;shutdown=true"))
    super.afterAll()
  }

  test("masking applies in the catalog that defines it") {
    checkAnswer(
      bob,
      s"SELECT value1 FROM $sparkCatalog.$defaultDb.src WHERE key = 1",
      Seq(Row(md5Hex("hello"))))
  }

  test("masking does not reach an identically named table in another catalog") {
    checkAnswer(
      bob,
      s"SELECT value1 FROM $maskFreeSparkCatalog.$defaultDb.src WHERE key = 1",
      Seq(Row("hello")))
  }

  test("row filtering applies in the catalog that defines it") {
    checkAnswer(
      bob,
      s"SELECT key FROM $sparkCatalog.$defaultDb.src ORDER BY key",
      Seq(Row(1)))
  }

  test("row filtering does not reach an identically named table in another catalog") {
    checkAnswer(
      bob,
      s"SELECT key FROM $maskFreeSparkCatalog.$defaultDb.src ORDER BY key",
      Seq(Row(1), Row(30)))
  }
}
