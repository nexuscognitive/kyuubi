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

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{Row, SparkSessionExtensions}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.spark.authz.{ObjectType, OperationType, SparkSessionProvider}
import org.apache.kyuubi.plugin.spark.authz.RangerTestNamespace._
import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.ranger._

/**
 * Tag-based masking, end to end: `default.tag_src.secret` is tagged PII by
 * sparkSql_hive_jenkins_tags.json and the tag service carries a MASK_NULL policy for that
 * tag. Nothing about it is visible in the resource policies, so these tests are what keeps
 * the tag path -- Ranger's tag enricher, the tag policy repository, and the catalog-mode
 * resource the tagged service resource is matched against -- from breaking silently.
 */
class DataMaskingForTagsSuite extends KyuubiFunSuite with SparkSessionProvider {
  override protected val catalogImpl: String = "in-memory"
  override protected val extension: SparkSessionExtensions => Unit = new RangerSparkExtension

  private val taggedTable = s"$defaultDb.tag_src"

  override def beforeAll(): Unit = {
    super.beforeAll()
    doAs(admin) {
      sql(s"CREATE TABLE IF NOT EXISTS $taggedTable" +
        " (key int, secret string, plain string) USING parquet")
      sql(s"INSERT INTO $taggedTable SELECT 1, 'classified', 'public'")
    }
  }

  override def afterAll(): Unit = {
    doAs(admin, sql(s"DROP TABLE IF EXISTS $taggedTable"))
    spark.stop()
    super.afterAll()
  }

  test("tag-based masking nullifies the tagged column only") {
    checkAnswer(
      bob,
      s"SELECT secret, plain FROM $taggedTable",
      Seq(Row(null, "public")))
  }

  test("a user outside the tag policy reads the tagged column unmasked") {
    checkAnswer(
      admin,
      s"SELECT secret FROM $taggedTable",
      Seq(Row("classified")))
  }

  test("tag matching is catalog-scoped") {
    val bobUgi = UserGroupInformation.createRemoteUser(bob)
    def maskOf(catalog: Option[String]): Option[String] = {
      val resource =
        AccessResource(ObjectType.COLUMN, defaultDb, "tag_src", "secret", None, catalog)
      SparkRangerAdminPlugin.getMaskingExpr(
        AccessRequest(resource, bobUgi, OperationType.QUERY, AccessType.SELECT))
    }

    // the tagged service resource names the catalog the session catalog is mapped onto
    assert(maskOf(None).contains("NULL"))
    // a table of the same name in another catalog is a different resource, so it carries
    // neither the tag nor the tag policy's mask
    assert(maskOf(Some("catalog_without_tags")).isEmpty)
  }
}
