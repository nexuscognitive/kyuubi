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

import java.io.File
import java.util

import scala.language.implicitConversions

import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl

import org.apache.kyuubi.plugin.spark.authz.{ObjectType, PrivilegeObject}
import org.apache.kyuubi.plugin.spark.authz.ObjectType._
import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType

class AccessResource private (val objectType: ObjectType, val catalog: Option[String])
  extends RangerAccessResourceImpl {
  implicit def asString(obj: Object): String = if (obj != null) obj.asInstanceOf[String] else null
  def getDatabase: String = getValue("database")
  def getTable: String = getValue("table")
  def getColumn: String = getValue("column")
  def getColumns: Seq[String] = {
    val columnStr = getColumn
    if (columnStr == null) Nil else columnStr.split(",").filter(_.nonEmpty)
  }
  // New: support schema key for Trino-style service definitions
  def getSchema: String = getValue("schema")
  def getCatalog: String = getValue("catalog")
}

object AccessResource {

  /**
   * Configuration key to enable catalog-aware resource building.
   * When enabled, resources use catalog/schema/table/column hierarchy (Trino-style)
   * instead of database/table/column (Hive-style).
   * 
   * Set via ranger-spark-security.xml or spark conf:
   * ranger.plugin.spark.resource.catalog.enabled=true
   */
  val CATALOG_RESOURCE_ENABLED_KEY = "ranger.plugin.spark.resource.catalog.enabled"
  
  /**
   * Configuration key for default catalog mapping.
   * Maps Spark's default catalog name to the target catalog name in Ranger policies.
   * 
   * Example: spark_catalog -> iceberg
   * ranger.plugin.spark.catalog.default.spark=spark_catalog
   * ranger.plugin.spark.catalog.default.target=iceberg
   */
  val CATALOG_DEFAULT_SPARK_KEY = "ranger.plugin.spark.catalog.default.spark"
  val CATALOG_DEFAULT_TARGET_KEY = "ranger.plugin.spark.catalog.default.target"
  
  /**
   * Additional catalog mappings in format: spark_name:target_name,other:mapped
   * ranger.plugin.spark.catalog.mapping=hive_metastore:hive,delta:delta
   */
  val CATALOG_MAPPING_KEY = "ranger.plugin.spark.catalog.mapping"

  // Lazy initialization of configuration
  @volatile private var catalogResourceEnabled: Option[Boolean] = None
  @volatile private var catalogMapping: Map[String, String] = Map.empty
  @volatile private var defaultSparkCatalog: String = "spark_catalog"
  @volatile private var defaultTargetCatalog: String = "iceberg"

  private def initConfig(): Unit = synchronized {
    if (catalogResourceEnabled.isEmpty) {
      val conf = SparkRangerAdminPlugin.getRangerConf
      
      catalogResourceEnabled = Some(
        conf.getBoolean(CATALOG_RESOURCE_ENABLED_KEY, false)
      )
      
      if (catalogResourceEnabled.get) {
        defaultSparkCatalog = conf.get(CATALOG_DEFAULT_SPARK_KEY, "spark_catalog")
        defaultTargetCatalog = conf.get(CATALOG_DEFAULT_TARGET_KEY, "iceberg")
        
        val mappingStr = conf.get(CATALOG_MAPPING_KEY, "")
        val additionalMappings = if (mappingStr.nonEmpty) {
          mappingStr.split(",").flatMap { pair =>
            val parts = pair.trim.split(":")
            if (parts.length == 2) Some(parts(0).trim -> parts(1).trim) else None
          }.toMap
        } else Map.empty[String, String]
        
        // Build full mapping: explicit mappings + default catalog mapping
        catalogMapping = additionalMappings + (defaultSparkCatalog -> defaultTargetCatalog)
      }
    }
  }

  /**
   * Check if catalog-aware resource building is enabled.
   */
  def isCatalogResourceEnabled: Boolean = {
    initConfig()
    catalogResourceEnabled.getOrElse(false)
  }

  /**
   * Map a Spark catalog name to the target catalog name for Ranger policies.
   * Returns the original name if no mapping exists.
   */
  def mapCatalog(catalog: Option[String]): Option[String] = {
    initConfig()
    if (!isCatalogResourceEnabled) {
      catalog
    } else {
      catalog match {
        case Some(cat) => Some(catalogMapping.getOrElse(cat, cat))
        case None => Some(defaultTargetCatalog) // Use default when no catalog specified
      }
    }
  }

  def apply(
      objectType: ObjectType,
      firstLevelResource: String,
      secondLevelResource: String,
      thirdLevelResource: String,
      owner: Option[String] = None,
      catalog: Option[String] = None): AccessResource = {
    val resource = new AccessResource(objectType, catalog)

    // Determine if we should use catalog-aware (Trino-style) resource hierarchy
    val useCatalogResource = isCatalogResourceEnabled
    
    // Map the catalog name if catalog-aware mode is enabled
    val mappedCatalog = if (useCatalogResource) mapCatalog(catalog) else None

    resource.objectType match {
      case DATABASE =>
        if (useCatalogResource) {
          // Trino-style: catalog/schema
          mappedCatalog.foreach(c => resource.setValue("catalog", c))
          resource.setValue("schema", firstLevelResource)
        } else {
          // Hive-style: database
          resource.setValue("database", firstLevelResource)
        }
        
      case FUNCTION =>
        if (useCatalogResource) {
          // Trino-style: catalog/schema/function
          mappedCatalog.foreach(c => resource.setValue("catalog", c))
          resource.setValue("schema", Option(firstLevelResource).getOrElse(""))
          resource.setValue("function", secondLevelResource)
        } else {
          // Hive-style: database/udf
          resource.setValue("database", Option(firstLevelResource).getOrElse(""))
          resource.setValue("udf", secondLevelResource)
        }
        
      case COLUMN =>
        if (useCatalogResource) {
          // Trino-style: catalog/schema/table/column
          mappedCatalog.foreach(c => resource.setValue("catalog", c))
          resource.setValue("schema", firstLevelResource)
          resource.setValue("table", secondLevelResource)
          resource.setValue("column", thirdLevelResource)
        } else {
          // Hive-style: database/table/column
          resource.setValue("database", firstLevelResource)
          resource.setValue("table", secondLevelResource)
          resource.setValue("column", thirdLevelResource)
        }
        
      case TABLE | VIEW | INDEX =>
        if (useCatalogResource) {
          // Trino-style: catalog/schema/table
          mappedCatalog.foreach(c => resource.setValue("catalog", c))
          resource.setValue("schema", firstLevelResource)
          resource.setValue("table", secondLevelResource)
        } else {
          // Hive-style: database/table
          resource.setValue("database", firstLevelResource)
          resource.setValue("table", secondLevelResource)
        }
        
      case URI =>
        // URL resource is the same for both styles
        val objectList = new util.ArrayList[String]
        Option(firstLevelResource)
          .filter(_.nonEmpty)
          .foreach { path =>
            val s = path.stripSuffix(File.separator)
            objectList.add(s)
            objectList.add(s + File.separator)
          }
        resource.setValue("url", objectList)
    }
    resource.setServiceDef(SparkRangerAdminPlugin.getServiceDef)
    owner.foreach(resource.setOwnerUser)
    resource
  }

  def apply(
      objectType: ObjectType,
      firstLevelResource: String,
      catalog: Option[String]): AccessResource = {
    apply(objectType, firstLevelResource, null, null, catalog = catalog)
  }

  def apply(
      obj: PrivilegeObject,
      opType: OperationType): AccessResource = {
    apply(
      ObjectType(obj, opType),
      obj.dbname,
      obj.objectName,
      obj.columns.mkString(","),
      obj.owner,
      obj.catalog)
  }

  /**
   * Reset configuration cache (for testing purposes).
   */
  def resetConfig(): Unit = synchronized {
    catalogResourceEnabled = None
    catalogMapping = Map.empty
    defaultSparkCatalog = "spark_catalog"
    defaultTargetCatalog = "iceberg"
  }
}