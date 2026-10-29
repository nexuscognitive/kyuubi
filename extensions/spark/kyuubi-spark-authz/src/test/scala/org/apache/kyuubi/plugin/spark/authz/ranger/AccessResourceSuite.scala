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

package org.apache.kyuubi.plugin.spark.authz.ranger

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.spark.authz.ObjectType._
import org.apache.kyuubi.plugin.spark.authz.SparkSessionProvider
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessResource.DEFAULT_SPARK_CATALOG

class AccessResourceSuite extends KyuubiFunSuite with SparkSessionProvider {
  override protected val catalogImpl: String = "in-memory"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // `spark` is lazy. AccessResource resolves catalog defaults and the Ranger catalog
    // mapping through SparkSession.active, so the session has to exist before the first
    // resource is built -- which is also how it works in production, on the driver.
    assert(spark.sparkContext != null)
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("generate spark ranger resources") {
    val resource = AccessResource(DATABASE, "my_db_name", None)
    // `catalog` is the raw input; getCatalog is what reaches Ranger, after the default
    // catalog is applied and mapped. Catalogs are always enforced, so it is never empty.
    assert(resource.catalog.isEmpty)
    assert(resource.getCatalog === AccessResource.DEFAULT_TARGET_CATALOG)
    assert(resource.getSchema === "my_db_name")
    assert(resource.getTable === null)
    assert(resource.getColumn === null)
    assert(resource.getColumns.isEmpty)

    val resource1 =
      AccessResource(DATABASE, null, "my_table_name", "my_col_1,my_col_2", Some("Bob"), None)
    assert(resource1.catalog.isEmpty)
    assert(resource1.getSchema === null)
    assert(resource1.getTable === null)
    assert(resource1.getColumn === null)
    assert(resource1.getColumns.isEmpty)
    assert(resource1.getOwnerUser === "Bob")

    val resource2 = AccessResource(FUNCTION, "my_db_name", "my_func_name", null, catalog = None)
    assert(resource2.catalog.isEmpty)
    assert(resource2.getSchema === "my_db_name")
    assert(resource2.getTable === null)
    assert(resource2.getValue("udf") === "my_func_name")
    assert(resource1.getColumn === null)
    assert(resource1.getColumns.isEmpty)

    val resource3 =
      AccessResource(TABLE, "my_db_name", "my_table_name", "my_col_1,my_col_2", catalog = None)
    assert(resource3.catalog.isEmpty)
    assert(resource3.getSchema === "my_db_name")
    assert(resource3.getTable === "my_table_name")
    assert(resource3.getColumn === null)
    assert(resource3.getColumns.isEmpty)

    val resource4 =
      AccessResource(COLUMN, "my_db_name", "my_table_name", "my_col_1,my_col_2", catalog = None)
    assert(resource4.catalog.isEmpty)
    assert(resource4.getSchema === "my_db_name")
    assert(resource4.getTable === "my_table_name")
    assert(resource4.getColumn === "my_col_1,my_col_2")
    assert(resource4.getColumns === Seq("my_col_1", "my_col_2"))
  }

  test("catalog settings are read from the Ranger plugin conf") {
    // spark-submit drops every `--conf` key that does not start with `spark.`, so
    // ranger-spark-security.xml is the only place a deployment can set these -- reading
    // them from the session conf alone left them silently stuck on the built-in defaults.
    val rangerConf = SparkRangerAdminPlugin.getRangerConf
    assert(spark.conf.getOption(AccessResource.RESOURCE_MODE_KEY).isEmpty)
    rangerConf.set(AccessResource.RESOURCE_MODE_KEY, AccessResource.MODE_HIVE)
    AccessResource.reset()
    try {
      val resource = AccessResource(TABLE, "my_db_name", "my_table_name", null, None, None)
      assert(resource.getCatalog === null, "hive mode has no catalog level")
      assert(resource.getDatabase === "my_db_name")
    } finally {
      rangerConf.unset(AccessResource.RESOURCE_MODE_KEY)
      AccessResource.reset()
    }
  }

  test("an explicit mapping overrides the default catalog's target") {
    // the session catalog's default pair is a fallback; before, it was applied last and
    // silently won, so `...catalog.mapping` could never redirect the session catalog
    val previous = spark.conf.getOption(AccessResource.CATALOG_MAPPING_KEY)
    spark.conf.set(AccessResource.CATALOG_MAPPING_KEY, s"$DEFAULT_SPARK_CATALOG:redirected")
    AccessResource.reset()
    try {
      val resource = AccessResource(TABLE, "my_db_name", "my_table_name", null, None, None)
      assert(resource.getCatalog === "redirected")
    } finally {
      previous.foreach(spark.conf.set(AccessResource.CATALOG_MAPPING_KEY, _))
      AccessResource.reset()
    }
  }

  test("KYUUBI #3605: generate spark ranger resources with catalog") {
    val catalog = Some("my_cat")

    val resource = AccessResource(DATABASE, "my_db_name", null, null, catalog = catalog)
    assert(resource.catalog.get === "my_cat")
    assert(resource.getSchema === "my_db_name")
    assert(resource.getTable === null)
    assert(resource.getColumn === null)
    assert(resource.getColumns.isEmpty)

    val resource1 =
      AccessResource(COLUMN, "my_db_name", "my_table_name", "my_col_1,my_col_2", catalog = catalog)
    assert(resource1.catalog.get === "my_cat")
    assert(resource1.getSchema === "my_db_name")
    assert(resource1.getTable === "my_table_name")
    assert(resource1.getColumn === "my_col_1,my_col_2")
    assert(resource1.getColumns === Seq("my_col_1", "my_col_2"))
  }

  test("excluded catalogs: unset in both sources excludes nothing") {
    val conf = SparkRangerAdminPlugin.getRangerConf
    conf.unset(AccessResource.EXCLUDED_CATALOGS_KEY)
    spark.conf.unset(AccessResource.SPARK_EXCLUDED_CATALOGS_KEY)
    try {
      assert(AccessResource.excludedCatalogs(spark).isEmpty)
      assert(!AccessResource.isExcludedCatalog(Some("iceberg"), Set.empty))
      assert(!AccessResource.isExcludedCatalog(None, Set.empty))
      assert(!AccessResource.isExcludedTableClass(
        "com.mongodb.spark.sql.connector.MongoTable",
        Set.empty))
    } finally {
      conf.unset(AccessResource.EXCLUDED_CATALOGS_KEY)
      spark.conf.unset(AccessResource.SPARK_EXCLUDED_CATALOGS_KEY)
    }
  }

  test("excluded catalogs: reads from Ranger XML when SparkConf is unset") {
    val conf = SparkRangerAdminPlugin.getRangerConf
    conf.set(AccessResource.EXCLUDED_CATALOGS_KEY, "mongodb,kafka")
    spark.conf.unset(AccessResource.SPARK_EXCLUDED_CATALOGS_KEY)
    try {
      assert(AccessResource.excludedCatalogs(spark) === Set("mongodb", "kafka"))
    } finally {
      conf.unset(AccessResource.EXCLUDED_CATALOGS_KEY)
    }
  }

  test("excluded catalogs: reads from SparkConf when Ranger XML is unset") {
    val conf = SparkRangerAdminPlugin.getRangerConf
    conf.unset(AccessResource.EXCLUDED_CATALOGS_KEY)
    spark.conf.set(AccessResource.SPARK_EXCLUDED_CATALOGS_KEY, "mongodb")
    try {
      assert(AccessResource.excludedCatalogs(spark) === Set("mongodb"))
    } finally {
      spark.conf.unset(AccessResource.SPARK_EXCLUDED_CATALOGS_KEY)
    }
  }

  test("excluded catalogs: SparkConf and Ranger XML values are unioned when both are set") {
    val conf = SparkRangerAdminPlugin.getRangerConf
    conf.set(AccessResource.EXCLUDED_CATALOGS_KEY, "mongodb,kafka")
    spark.conf.set(AccessResource.SPARK_EXCLUDED_CATALOGS_KEY, "elasticsearch")
    try {
      // Union semantics: both sources contribute, neither can turn off the other.
      // A session can extend a platform-shipped list but cannot un-exclude an entry
      // the platform explicitly listed.
      assert(AccessResource.excludedCatalogs(spark) === Set("mongodb", "kafka", "elasticsearch"))
    } finally {
      conf.unset(AccessResource.EXCLUDED_CATALOGS_KEY)
      spark.conf.unset(AccessResource.SPARK_EXCLUDED_CATALOGS_KEY)
    }
  }

  test("excluded catalogs: catalog-name match for connectors that register a catalog") {
    val excluded = Set("mongodb", "kafka")
    // A relation registered under a Spark catalog with a matching name is excluded.
    assert(AccessResource.isExcludedCatalog(Some("mongodb"), excluded))
    assert(AccessResource.isExcludedCatalog(Some("kafka"), excluded))
    // Non-listed catalogs and catalog-less relations are not matched by name.
    assert(!AccessResource.isExcludedCatalog(Some("iceberg"), excluded))
    assert(!AccessResource.isExcludedCatalog(None, excluded))
  }

  test("excluded catalogs: table-class match via alias for catalog-less connectors") {
    val excluded = Set("mongodb")
    // The MongoDB Spark Connector's DSv2 Table classes match the aliased prefix.
    assert(AccessResource.isExcludedTableClass(
      "com.mongodb.spark.sql.connector.MongoTable",
      excluded))
    assert(AccessResource.isExcludedTableClass(
      "com.mongodb.spark.sql.connector.internal.MongoTableV2",
      excluded))
    // A friendly name that has no alias entry matches no class.
    assert(!AccessResource.isExcludedTableClass(
      "com.mongodb.spark.sql.connector.MongoTable",
      Set("elasticsearch")))
    // A class outside every aliased prefix is not matched.
    assert(!AccessResource.isExcludedTableClass(
      "org.apache.iceberg.spark.source.SparkTable",
      excluded))
  }

  test("excluded catalogs: whitespace and empty entries are ignored") {
    val conf = SparkRangerAdminPlugin.getRangerConf
    conf.set(AccessResource.EXCLUDED_CATALOGS_KEY, "  mongodb , ,  kafka  ,")
    spark.conf.unset(AccessResource.SPARK_EXCLUDED_CATALOGS_KEY)
    try {
      assert(AccessResource.excludedCatalogs(spark) === Set("mongodb", "kafka"))
    } finally {
      conf.unset(AccessResource.EXCLUDED_CATALOGS_KEY)
    }
  }

  test("excluded catalogs: an all-whitespace value is treated as unset") {
    val conf = SparkRangerAdminPlugin.getRangerConf
    conf.set(AccessResource.EXCLUDED_CATALOGS_KEY, "   ,  , ")
    spark.conf.unset(AccessResource.SPARK_EXCLUDED_CATALOGS_KEY)
    try {
      assert(AccessResource.excludedCatalogs(spark).isEmpty)
    } finally {
      conf.unset(AccessResource.EXCLUDED_CATALOGS_KEY)
    }
  }
}
