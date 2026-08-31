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

import scala.collection.JavaConverters._

import com.mongodb.spark.sql.connector.MongoTableProvider
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.spark.authz.{OperationType, PrivilegeObject}
import org.apache.kyuubi.plugin.spark.authz.{PrivilegeObjectActionType, SparkSessionProvider}
import org.apache.kyuubi.plugin.spark.authz.ObjectType._
import org.apache.kyuubi.plugin.spark.authz.serde.DataSourceV2RelationTableExtractor

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
      AccessResource(DATABASE, null, "my_table_name", "my_col_1,my_col_2", Some("Bob"))
    assert(resource1.catalog.isEmpty)
    assert(resource1.getSchema === null)
    assert(resource1.getTable === null)
    assert(resource1.getColumn === null)
    assert(resource1.getColumns.isEmpty)
    assert(resource1.getOwnerUser === "Bob")

    val resource2 = AccessResource(FUNCTION, "my_db_name", "my_func_name", null)
    assert(resource2.catalog.isEmpty)
    assert(resource2.getSchema === "my_db_name")
    assert(resource2.getTable === null)
    assert(resource2.getValue("udf") === "my_func_name")
    assert(resource1.getColumn === null)
    assert(resource1.getColumns.isEmpty)

    val resource3 = AccessResource(TABLE, "my_db_name", "my_table_name", "my_col_1,my_col_2")
    assert(resource3.catalog.isEmpty)
    assert(resource3.getSchema === "my_db_name")
    assert(resource3.getTable === "my_table_name")
    assert(resource3.getColumn === null)
    assert(resource3.getColumns.isEmpty)

    val resource4 = AccessResource(COLUMN, "my_db_name", "my_table_name", "my_col_1,my_col_2")
    assert(resource4.catalog.isEmpty)
    assert(resource4.getSchema === "my_db_name")
    assert(resource4.getTable === "my_table_name")
    assert(resource4.getColumn === "my_col_1,my_col_2")
    assert(resource4.getColumns === Seq("my_col_1", "my_col_2"))
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

  test("KYUUBI #7230: a catalog-less MongoDB relation yields a namespace-scoped resource") {
    val options = Map(
      "connection.uri" -> "mongodb://localhost:27017",
      "database" -> "ciwat_servicenow_dev",
      "collection" -> "BUSINESS_APPLICATION_DIMENSION")
    val dsOptions = new CaseInsensitiveStringMap(options.asJava)
    val v2Table = new MongoTableProvider().getTable(
      StructType(Seq(StructField("_id", StringType))),
      Array.empty[Transform],
      dsOptions.asCaseSensitiveMap())
    val relation = DataSourceV2Relation.create(v2Table, None, None, dsOptions)

    val table = new DataSourceV2RelationTableExtractor().apply(spark, relation).get
    val resource = AccessResource(
      PrivilegeObject(table, Nil, PrivilegeObjectActionType.INSERT),
      OperationType.QUERY)

    // The schema level is what makes the request matchable at all. Ranger's hierarchy
    // matcher refuses a resource that is missing a parent level, so the pre-fix request
    // -- catalog=iceberg (the mapped session default), no schema, table=MongoTable() --
    // could not be authorized by any policy, and named neither the real database nor
    // the real collection.
    // Assert the element map rather than getAsString: the "/"-joined rendering is
    // ordered by the Ranger service def, which is only loaded from a live Ranger
    // service, so getAsString is null here. The map is what the policy matcher reads.
    assert(resource.getAsMap.asScala === Map(
      "catalog" -> "mongodb",
      "schema" -> "ciwat_servicenow_dev",
      "table" -> "BUSINESS_APPLICATION_DIMENSION"))
    assert(resource.getCatalog === "mongodb")
    assert(resource.getSchema === "ciwat_servicenow_dev")
    assert(resource.getTable === "BUSINESS_APPLICATION_DIMENSION")
  }
}
