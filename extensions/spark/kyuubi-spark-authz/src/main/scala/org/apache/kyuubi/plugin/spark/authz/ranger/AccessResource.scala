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

import scala.language.implicitConversions

import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.plugin.spark.authz.ObjectType
import org.apache.kyuubi.plugin.spark.authz.ObjectType._
import org.apache.kyuubi.plugin.spark.authz.serde.{Database, Table}

/**
 * Extended AccessResource that supports catalog-aware resource building.
 * 
 * This class builds Ranger access resources with either:
 * - Hive-style: database/table/column (legacy)
 * - Catalog-style: catalog/schema/table/column (for Trino/unified services)
 * 
 * The mode is controlled by spark.ranger.plugin.spark.resource.mode configuration.
 */
class AccessResource private (
    val objectType: ObjectType,
    val catalog: Option[String])
  extends RangerAccessResourceImpl {

  implicit def asString(obj: Object): String = if (obj != null) obj.asInstanceOf[String] else null

  def getDatabase: String = getValue("database")
  def getTable: String = getValue("table")
  def getColumn: String = getValue("column")
  def getSchema: String = getValue("schema")
  def getCatalog: String = getValue("catalog")
  def getUrl: String = getValue("url")

  def getColumns: Seq[String] = {
    val columnStr = getColumn
    if (columnStr == null) Nil else columnStr.split(",").filter(_.nonEmpty)
  }
}

object AccessResource {

  // Configuration keys
  val RESOURCE_MODE_KEY = "ranger.plugin.spark.resource.mode"
  val CATALOG_MAPPING_ENABLED_KEY = "ranger.plugin.spark.catalog.mapping.enabled"
  val DEFAULT_SPARK_CATALOG_KEY = "ranger.plugin.spark.catalog.default.spark"
  val DEFAULT_TARGET_CATALOG_KEY = "ranger.plugin.spark.catalog.default.target"
  val CATALOG_MAPPING_KEY = "ranger.plugin.spark.catalog.mapping"

  // Resource modes
  val MODE_HIVE = "hive"
  val MODE_CATALOG = "catalog"

  // Default values
  val DEFAULT_SPARK_CATALOG = "spark_catalog"
  val DEFAULT_TARGET_CATALOG = "iceberg"

  // Cached configuration
  @volatile private var initialized = false
  @volatile private var resourceMode = MODE_HIVE
  @volatile private var mappingEnabled = false
  @volatile private var sparkDefaultCatalog = DEFAULT_SPARK_CATALOG
  @volatile private var targetDefaultCatalog = DEFAULT_TARGET_CATALOG
  @volatile private var catalogMapping: Map[String, String] = Map.empty

  /**
   * Initialize configuration from SparkSession.
   */
  private def initialize(spark: SparkSession): Unit = synchronized {
    if (!initialized) {
      resourceMode = spark.conf.get(RESOURCE_MODE_KEY, MODE_HIVE)
      mappingEnabled = spark.conf.getOption(CATALOG_MAPPING_ENABLED_KEY)
        .exists(_.equalsIgnoreCase("true"))

      if (mappingEnabled) {
        sparkDefaultCatalog = spark.conf.get(DEFAULT_SPARK_CATALOG_KEY, DEFAULT_SPARK_CATALOG)
        targetDefaultCatalog = spark.conf.get(DEFAULT_TARGET_CATALOG_KEY, DEFAULT_TARGET_CATALOG)

        val mappingStr = spark.conf.get(CATALOG_MAPPING_KEY, "")
        catalogMapping = parseMappingString(mappingStr) + (sparkDefaultCatalog -> targetDefaultCatalog)
      }

      initialized = true
    }
  }

  /**
   * Parse catalog mapping string: "spark:target,other:mapped"
   */
  private def parseMappingString(mappingStr: String): Map[String, String] = {
    if (mappingStr == null || mappingStr.trim.isEmpty) {
      Map.empty
    } else {
      mappingStr.split(",").flatMap { pair =>
        val parts = pair.trim.split(":")
        if (parts.length == 2) Some(parts(0).trim -> parts(1).trim)
        else None
      }.toMap
    }
  }

  /**
   * Map a Spark catalog name to its Ranger policy equivalent.
   */
  def mapCatalog(catalog: String)(implicit spark: SparkSession): String = {
    if (!initialized) initialize(spark)

    if (!mappingEnabled || catalog == null || catalog.isEmpty) {
      catalog
    } else {
      catalogMapping.getOrElse(catalog, catalog)
    }
  }

  /**
   * Get the effective catalog name, applying mapping and defaults.
   */
  def getEffectiveCatalog(catalog: Option[String])(implicit spark: SparkSession): String = {
    if (!initialized) initialize(spark)

    val rawCatalog = catalog.getOrElse(sparkDefaultCatalog)
    if (mappingEnabled) {
      catalogMapping.getOrElse(rawCatalog, rawCatalog)
    } else {
      rawCatalog
    }
  }

  /**
   * Check if catalog-aware mode is enabled.
   */
  def isCatalogMode(implicit spark: SparkSession): Boolean = {
    if (!initialized) initialize(spark)
    resourceMode == MODE_CATALOG
  }

  /**
   * Build a table resource for Ranger authorization.
   * 
   * In catalog mode: catalog/schema/table/column
   * In hive mode: database/table/column
   */
  def buildTableResource(
      table: Table,
      columns: Seq[String] = Seq.empty)(implicit spark: SparkSession): AccessResource = {
    if (!initialized) initialize(spark)

    val resource = new AccessResource(TABLE, table.catalog)

    if (resourceMode == MODE_CATALOG) {
      // Catalog-aware mode (Trino-style)
      val effectiveCatalog = getEffectiveCatalog(table.catalog)
      resource.setValue("catalog", effectiveCatalog)
      table.database.foreach(db => resource.setValue("schema", db))
      resource.setValue("table", table.table)
      if (columns.nonEmpty) {
        resource.setValue("column", columns.mkString(","))
      }
    } else {
      // Legacy Hive mode
      table.database.foreach(db => resource.setValue("database", db))
      resource.setValue("table", table.table)
      if (columns.nonEmpty) {
        resource.setValue("column", columns.mkString(","))
      }
    }

    resource
  }

  /**
   * Build a database/schema resource for Ranger authorization.
   */
  def buildDatabaseResource(
      database: Database)(implicit spark: SparkSession): AccessResource = {
    if (!initialized) initialize(spark)

    val resource = new AccessResource(DATABASE, database.catalog)

    if (resourceMode == MODE_CATALOG) {
      val effectiveCatalog = getEffectiveCatalog(database.catalog)
      resource.setValue("catalog", effectiveCatalog)
      database.database.foreach(db => resource.setValue("schema", db))
    } else {
      database.database.foreach(db => resource.setValue("database", db))
    }

    resource
  }

  /**
   * Build a URL resource for path-based authorization.
   */
  def buildUrlResource(url: String): AccessResource = {
    val resource = new AccessResource(URI, None)
    resource.setValue("url", url)
    resource
  }

  /**
   * Build a function/UDF resource.
   */
  def buildFunctionResource(
      database: Option[String],
      functionName: String,
      catalog: Option[String] = None)(implicit spark: SparkSession): AccessResource = {
    if (!initialized) initialize(spark)

    val resource = new AccessResource(FUNCTION, catalog)

    if (resourceMode == MODE_CATALOG && catalog.isDefined) {
      val effectiveCatalog = getEffectiveCatalog(catalog)
      resource.setValue("catalog", effectiveCatalog)
      database.foreach(db => resource.setValue("schema", db))
    } else {
      database.foreach(db => resource.setValue("database", db))
    }
    resource.setValue("udf", functionName)

    resource
  }

  /**
   * Reset cached configuration (for testing).
   */
  def reset(): Unit = synchronized {
    initialized = false
    resourceMode = MODE_HIVE
    mappingEnabled = false
    sparkDefaultCatalog = DEFAULT_SPARK_CATALOG
    targetDefaultCatalog = DEFAULT_TARGET_CATALOG
    catalogMapping = Map.empty
  }
}