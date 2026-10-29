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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSessionExtensions}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.spark.authz.{AccessControlException, SparkSessionProvider}
import org.apache.kyuubi.plugin.spark.authz.RangerTestNamespace._
import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension

/**
 * Masking a column must not widen the privilege check to the rest of the table.
 *
 * The masking rule rebuilds the scan as a projection over every column, masked or not. If
 * the privilege objects are taken from that projection, a user granted exactly the column
 * a mask covers -- the shape every tag-based grant has -- is denied over columns the query
 * never named, and the mask makes the query it protects impossible to run.
 */
class DataMaskingColumnScopeSuite extends KyuubiFunSuite with SparkSessionProvider {
  override protected val catalogImpl: String = "in-memory"
  override protected val extension: SparkSessionExtensions => Unit = new RangerSparkExtension
  override protected def extraSparkConf: SparkConf =
    super.extraSparkConf.set("spark.sql.ansi.enabled", "false")

  override def beforeAll(): Unit = {
    super.beforeAll()
    doAs(admin) {
      sql(s"CREATE TABLE IF NOT EXISTS $defaultDb.src" +
        " (key int, value1 string, value2 string) USING parquet")
      sql(s"INSERT INTO $defaultDb.src SELECT 1, 'hello', 'world'")
    }
  }

  override def afterAll(): Unit = {
    doAs(admin, sql(s"DROP TABLE IF EXISTS $defaultDb.src"))
    spark.stop()
    super.afterAll()
  }

  test("a user granted only the masked column can read it") {
    checkAnswer(
      maskedColumnOnlyUser,
      s"SELECT value1 FROM $defaultDb.src",
      Seq(Row(null)))
  }

  test("the columns the query did not name stay unauthorized") {
    val e = intercept[AccessControlException] {
      doAs(maskedColumnOnlyUser, sql(s"SELECT value2 FROM $defaultDb.src").collect())
    }
    assert(e.getMessage.contains("value2"))
  }
}
