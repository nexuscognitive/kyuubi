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
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import org.apache.kyuubi.plugin.spark.authz.{AccessControlException, ObjectType, PrivilegeObject}
import org.apache.kyuubi.plugin.spark.authz.ObjectType._
import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType
import org.apache.kyuubi.plugin.spark.authz.serde.{Database, Table}

/**
 * Extended AccessResource that supports catalog-aware resource building.
 *
 * This class builds Ranger access resources with either:
 * - Hive-style: database/table/column (legacy)
 * - Catalog-style: catalog/schema/table/column (for Trino/unified services)
 *
 * The mode is controlled by the resource.mode configuration.
 */
class AccessResource private (
    val objectType: ObjectType,
    val catalog: Option[String])
  extends RangerAccessResourceImpl {

  implicit def asString(obj: Object): String = if (obj != null) obj.asInstanceOf[String] else null

  def getDatabase: String = getValue("database")
  def getUdf: String = getValue("udf")
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

  private val LOG = LoggerFactory.getLogger(getClass)

  // Configuration keys
  val RESOURCE_MODE_KEY = "ranger.plugin.spark.resource.mode"
  val CATALOG_MAPPING_ENABLED_KEY = "ranger.plugin.spark.catalog.mapping.enabled"
  val DEFAULT_SPARK_CATALOG_KEY = "ranger.plugin.spark.catalog.default.spark"
  val DEFAULT_TARGET_CATALOG_KEY = "ranger.plugin.spark.catalog.default.target"
  val CATALOG_MAPPING_KEY = "ranger.plugin.spark.catalog.mapping"
  val CATALOG_ALLOWLIST_KEY = "ranger.plugin.spark.catalog.allowlist"

  // Resource modes
  val MODE_HIVE = "hive"
  val MODE_CATALOG = "catalog"

  // Default values
  val DEFAULT_SPARK_CATALOG = "spark_catalog"
  val DEFAULT_TARGET_CATALOG = "iceberg"

  // Cached configuration: catalog mode with spark_catalog->iceberg
  @volatile private var initialized = false
  @volatile private var resourceMode = MODE_CATALOG
  @volatile private var mappingEnabled = true
  @volatile private var sparkDefaultCatalog = DEFAULT_SPARK_CATALOG
  @volatile private var targetDefaultCatalog = DEFAULT_TARGET_CATALOG
  @volatile private var catalogMapping: Map[String, String] =
    Map(DEFAULT_SPARK_CATALOG -> DEFAULT_TARGET_CATALOG)

  /**
   * Initialize configuration from SparkSession.
   */
  private def initialize(spark: SparkSession): Unit = synchronized {
    if (!initialized) {
      resourceMode = spark.conf.get(RESOURCE_MODE_KEY, MODE_CATALOG)
      mappingEnabled = spark.conf.getOption(CATALOG_MAPPING_ENABLED_KEY)
        .forall(_.equalsIgnoreCase("true"))

      if (mappingEnabled) {
        sparkDefaultCatalog = spark.conf.get(DEFAULT_SPARK_CATALOG_KEY, DEFAULT_SPARK_CATALOG)
        targetDefaultCatalog = spark.conf.get(DEFAULT_TARGET_CATALOG_KEY, DEFAULT_TARGET_CATALOG)

        val mappingStr = spark.conf.get(CATALOG_MAPPING_KEY, "")
        catalogMapping = parseMappingString(mappingStr) +
          (sparkDefaultCatalog -> targetDefaultCatalog)

        LOG.info(
          "Catalog mapping enabled: defaultSparkCatalog={}, " +
            "defaultTargetCatalog={}, mappings={}",
          Array[Object](sparkDefaultCatalog, targetDefaultCatalog, catalogMapping.toString()): _*)
      }

      LOG.info(
        "AccessResource initialized: resourceMode={}, mappingEnabled={}",
        Array[Object](resourceMode, mappingEnabled.toString): _*)
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
      val mapped = catalogMapping.getOrElse(catalog, catalog)
      if (LOG.isDebugEnabled) {
        LOG.debug("mapCatalog: {} -> {}", Array[Object](catalog, mapped): _*)
      }
      mapped
    }
  }

  /**
   * Get the effective catalog name, applying mapping and defaults.
   */
  def getEffectiveCatalog(catalog: Option[String])(implicit spark: SparkSession): String = {
    if (!initialized) initialize(spark)

    val rawCatalog = catalog.getOrElse(sparkDefaultCatalog)
    val effective = if (mappingEnabled) {
      catalogMapping.getOrElse(rawCatalog, rawCatalog)
    } else {
      rawCatalog
    }
    if (LOG.isDebugEnabled) {
      LOG.debug(
        "getEffectiveCatalog: input={}, raw={}, effective={}",
        Array[Object](catalog.orNull, rawCatalog, effective): _*)
    }
    effective
  }

  /**
   * Check if catalog-aware mode is enabled.
   */
  def isCatalogMode(implicit spark: SparkSession): Boolean = {
    if (!initialized) initialize(spark)
    resourceMode == MODE_CATALOG
  }

  /**
   * Fail closed if a resource that must be catalog-scoped reached Ranger without a
   * catalog level.
   *
   * Our policies are catalog-scoped, and Ranger's hierarchy matcher silently refuses to
   * match a resource that is missing a parent level -- so a catalog-less resource would
   * be evaluated as "no policy applies" rather than as an error. Depending on the
   * surrounding call that reads either as a confusing deny or, for callers that treat an
   * empty privilege set as "nothing to check", as a bypass. [[getEffectiveCatalog]]
   * always yields a catalog, so this should be unreachable; it exists so that a future
   * extractor or upstream merge that reintroduces a catalog-less path fails loudly here
   * instead of degrading authorization silently.
   */
  private def requireCatalog(resource: AccessResource, objectType: ObjectType): Unit = {
    // URI/url policies are intentionally catalog-free -- a path has no catalog.
    if (resourceMode == MODE_CATALOG && objectType != URI) {
      val catalog = resource.getCatalog
      if (catalog == null || catalog.isEmpty) {
        throw new AccessControlException(
          s"Refusing to authorize a $objectType resource without a catalog while " +
            s"$RESOURCE_MODE_KEY=$MODE_CATALOG. This is a bug in privilege-object " +
            "extraction: the request cannot be matched against catalog-scoped policies.")
      }
    }
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
      if (LOG.isDebugEnabled) {
        LOG.debug(
          "buildTableResource [catalog]: " +
            "catalog={}, schema={}, table={}, columns={}",
          Array[Object](
            effectiveCatalog,
            table.database.orNull,
            table.table,
            columns.mkString(",")): _*)
      }
    } else {
      // Legacy Hive mode
      table.database.foreach(db => resource.setValue("database", db))
      resource.setValue("table", table.table)
      if (columns.nonEmpty) {
        resource.setValue("column", columns.mkString(","))
      }
      if (LOG.isDebugEnabled) {
        LOG.debug(
          "buildTableResource [hive mode]: database={}, table={}, columns={}",
          Array[Object](table.database.orNull, table.table, columns.mkString(",")): _*)
      }
    }

    requireCatalog(resource, TABLE)
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

    requireCatalog(resource, DATABASE)
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

    if (resourceMode == MODE_CATALOG) {
      // Functions rarely carry an explicit catalog; fall back to the effective
      // (default-mapped) catalog so the resource still has a `catalog` level.
      val effectiveCatalog = getEffectiveCatalog(catalog)
      resource.setValue("catalog", effectiveCatalog)
      database.foreach(db => resource.setValue("schema", db))
    } else {
      database.foreach(db => resource.setValue("database", db))
    }
    resource.setValue("udf", functionName)

    requireCatalog(resource, FUNCTION)
    resource
  }

  /**
   * Backward-compatible apply method used by RuleAuthorization and other callers.
   * Builds an AccessResource using positional string arguments, catalog-mode-aware.
   */
  def apply(
      objectType: ObjectType,
      firstLevelResource: String,
      secondLevelResource: String,
      thirdLevelResource: String,
      owner: Option[String] = None,
      catalog: Option[String] = None): AccessResource = {
    val spark = SparkSession.active
    if (!initialized) initialize(spark)

    val resource = new AccessResource(objectType, catalog)

    objectType match {
      case DATABASE =>
        if (resourceMode == MODE_CATALOG) {
          val effectiveCatalog = getEffectiveCatalog(catalog)(spark)
          resource.setValue("catalog", effectiveCatalog)
          Option(firstLevelResource).foreach(db => resource.setValue("schema", db))
        } else {
          resource.setValue("database", firstLevelResource)
        }
      case FUNCTION =>
        if (resourceMode == MODE_CATALOG) {
          val effectiveCatalog = getEffectiveCatalog(catalog)(spark)
          resource.setValue("catalog", effectiveCatalog)
          Option(firstLevelResource).filter(_.nonEmpty)
            .foreach(db => resource.setValue("schema", db))
        } else {
          resource.setValue("database", Option(firstLevelResource).getOrElse(""))
        }
        resource.setValue("udf", secondLevelResource)
      case COLUMN =>
        if (resourceMode == MODE_CATALOG) {
          val effectiveCatalog = getEffectiveCatalog(catalog)(spark)
          resource.setValue("catalog", effectiveCatalog)
          Option(firstLevelResource).foreach(db => resource.setValue("schema", db))
          resource.setValue("table", secondLevelResource)
          resource.setValue("column", thirdLevelResource)
        } else {
          resource.setValue("database", firstLevelResource)
          resource.setValue("table", secondLevelResource)
          resource.setValue("column", thirdLevelResource)
        }
      case TABLE | VIEW | INDEX =>
        if (resourceMode == MODE_CATALOG) {
          val effectiveCatalog = getEffectiveCatalog(catalog)(spark)
          resource.setValue("catalog", effectiveCatalog)
          Option(firstLevelResource).foreach(db => resource.setValue("schema", db))
          resource.setValue("table", secondLevelResource)
        } else {
          resource.setValue("database", firstLevelResource)
          resource.setValue("table", secondLevelResource)
        }
      case URI =>
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
    requireCatalog(resource, objectType)
    resource.setServiceDef(SparkRangerAdminPlugin.getServiceDef)
    owner.foreach(resource.setOwnerUser)
    if (LOG.isDebugEnabled) {
      LOG.debug(
        "apply: objectType={}, mode={}, " +
          "catalog={}, resource={}",
        Array[Object](
          objectType.toString,
          resourceMode,
          catalog.orNull,
          resource.getAsMap.toString): _*)
    }
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
   * The set of Spark catalog names that Ranger is authoritative for, read from
   * `ranger.plugin.spark.catalog.allowlist` in ranger-spark-security.xml. `None`
   * means the key is unset and the allowlist is not enforced -- callers should
   * treat every catalog as allowlisted.
   */
  def catalogAllowlist: Option[Set[String]] =
    Option(SparkRangerAdminPlugin.getRangerConf.get(CATALOG_ALLOWLIST_KEY))
      .map(_.split(",").iterator.map(_.trim).filter(_.nonEmpty).toSet)
      .filter(_.nonEmpty)

  /**
   * Whether a given catalog is inside the configured allowlist. A `None` catalog
   * -- e.g. an identifier-less DSv2 relation from a non-catalog TableProvider --
   * is never allowlisted when the allowlist is enforced; the caller is expected
   * to skip authorization for it. Returns `true` unconditionally when the
   * allowlist is unset, so unopted-in deployments see no behavior change.
   */
  def isAllowlisted(catalog: Option[String]): Boolean = catalogAllowlist match {
    case None => true
    case Some(allowed) => catalog.exists(allowed)
  }

  /**
   * Reset cached configuration (for testing).
   */
  def reset(): Unit = synchronized {
    initialized = false
    resourceMode = MODE_CATALOG
    mappingEnabled = true
    sparkDefaultCatalog = DEFAULT_SPARK_CATALOG
    targetDefaultCatalog = DEFAULT_TARGET_CATALOG
    catalogMapping = Map(DEFAULT_SPARK_CATALOG -> DEFAULT_TARGET_CATALOG)
  }
}
