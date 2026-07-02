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

package org.apache.kyuubi.plugin.spark.authz

import java.nio.file.Files

// scalastyle:off
import org.apache.spark.SparkConf
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectType._
import org.apache.kyuubi.tags.IcebergTest

/**
 * Regression tests for authorization of Iceberg tables that live under a
 * multi-level (nested) namespace, e.g. `cat.a.b.tbl`, or under a single
 * namespace level that itself contains a dot, e.g. `` cat.`x.y`.tbl ``.
 *
 * The bug these guard against: the read/write path
 * ([[org.apache.kyuubi.plugin.spark.authz.serde.DataSourceV2RelationTableExtractor]])
 * used to derive the namespace by string-splitting the dot-flattened
 * `table.name()`, which
 *   - lost the distinction between a nested namespace and a dotted single
 *     level, and disagreed with the CREATE path, and
 *   - could yield an empty database for catalogs that do not encode the
 *     namespace in the table name - which then matched a broad/`default`
 *     policy (fail open).
 * The fix reads the namespace from the relation's `identifier`.
 */
@IcebergTest
class IcebergNestedNamespaceSuite extends AnyFunSuite with BeforeAndAfterAll
  with SparkSessionProvider {
// scalastyle:on

  override protected val catalogImpl: String = "in-memory"
  override protected val sqlExtensions: String =
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  private val cat = "local"
  override protected val extraSparkConf: SparkConf = new SparkConf()
    .set(s"spark.sql.catalog.$cat", "org.apache.iceberg.spark.SparkCatalog")
    .set(s"spark.sql.catalog.$cat.type", "hadoop")
    .set(
      s"spark.sql.catalog.$cat.warehouse",
      Files.createTempDirectory("iceberg-nested-ns").toString)

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  /** The single database string extracted for `sqlText`, from input or output. */
  private def dbOf(sqlText: String): String = {
    val plan = sql(sqlText).queryExecution.analyzed
    val (inputs, outputs, _) = PrivilegesBuilder.build(plan, spark)
    val pos = (inputs ++ outputs).filter(_.privilegeObjectType == TABLE_OR_VIEW)
    assert(pos.nonEmpty, s"fail-open: no table privilege object built for [$sqlText]")
    pos.head.dbname
  }

  test("two-level namespace is preserved across read and write") {
    sql(s"CREATE TABLE IF NOT EXISTS $cat.a.b.t (id int, name string) USING iceberg")
    assert(dbOf(s"CREATE TABLE $cat.a.b.t2 (id int) USING iceberg") === "a.b")
    assert(dbOf(s"SELECT id FROM $cat.a.b.t") === "a.b")
    assert(dbOf(s"INSERT INTO $cat.a.b.t VALUES (1, 'x')") === "a.b")
  }

  test("three-level namespace is preserved across read and write") {
    sql(s"CREATE TABLE IF NOT EXISTS $cat.a.b.c.deep (id int) USING iceberg")
    assert(dbOf(s"SELECT id FROM $cat.a.b.c.deep") === "a.b.c")
    assert(dbOf(s"INSERT INTO $cat.a.b.c.deep VALUES (1)") === "a.b.c")
  }

  test("a single namespace level containing a dot is quoted consistently") {
    sql(s"CREATE TABLE IF NOT EXISTS $cat.`x.y`.dotted (id int) USING iceberg")
    // read and write paths must agree with the CREATE path - all backtick-quoted
    val expected = "`x.y`"
    assert(dbOf(s"CREATE TABLE $cat.`x.y`.dotted2 (id int) USING iceberg") === expected)
    assert(dbOf(s"SELECT id FROM $cat.`x.y`.dotted") === expected)
    assert(dbOf(s"INSERT INTO $cat.`x.y`.dotted VALUES (1)") === expected)
  }
}
