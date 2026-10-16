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
import java.util.Collections.synchronizedMap

import scala.language.implicitConversions

import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import org.apache.kyuubi.plugin.spark.authz.{AccessControlException, ObjectType, PrivilegeObject}
import org.apache.kyuubi.plugin.spark.authz.ObjectType._
import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType

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

  // Resource modes
  val MODE_HIVE = "hive"
  val MODE_CATALOG = "catalog"

  // Default values
  val DEFAULT_SPARK_CATALOG = "spark_catalog"
  val DEFAULT_TARGET_CATALOG = "iceberg"

  /**
   * The resolved catalog-mode settings of one SparkSession.
   */
  private case class CatalogConf(
      resourceMode: String,
      mappingEnabled: Boolean,
      sparkDefaultCatalog: String,
      catalogMapping: Map[String, String]) {

    def isCatalogMode: Boolean = resourceMode == MODE_CATALOG

    def mapCatalog(catalog: String): String =
      if (!mappingEnabled || catalog == null || catalog.isEmpty) {
        catalog
      } else {
        catalogMapping.getOrElse(catalog, catalog)
      }
  }

  /**
   * Resolved settings are cached per session, not once per JVM: a driver that serves more
   * than one SparkSession (and every test JVM) must not inherit the resource mode and
   * catalog mapping of whichever session happened to build the first resource. Weak keys
   * so a stopped session is not retained.
   */
  private val catalogConfs =
    synchronizedMap(new util.WeakHashMap[SparkSession, CatalogConf]())

  private def catalogConf(spark: SparkSession): CatalogConf = {
    val cached = catalogConfs.get(spark)
    if (cached != null) {
      cached
    } else {
      val resolved = resolveCatalogConf(spark)
      catalogConfs.put(spark, resolved)
      resolved
    }
  }

  /**
   * These keys are named `ranger.plugin.<serviceType>.*`, so an operator reasonably puts
   * them in `ranger-spark-security.xml` next to every other key of that shape -- and they
   * cannot be passed as a Spark conf anyway, because spark-submit drops any `--conf` whose
   * key does not start with `spark.` ("Ignoring non-Spark config property"). The Ranger
   * plugin conf is therefore the primary source; a session conf still wins where one is
   * set, which is how the tests exercise mappings in-process.
   */
  private def catalogSetting(spark: SparkSession, key: String): Option[String] =
    spark.conf.getOption(key).orElse(Option(SparkRangerAdminPlugin.getRangerConf.get(key)))

  private def resolveCatalogConf(spark: SparkSession): CatalogConf = {
    val resourceMode = catalogSetting(spark, RESOURCE_MODE_KEY).getOrElse(MODE_CATALOG)
    val mappingEnabled = catalogSetting(spark, CATALOG_MAPPING_ENABLED_KEY)
      .forall(_.equalsIgnoreCase("true"))
    val sparkDefaultCatalog =
      catalogSetting(spark, DEFAULT_SPARK_CATALOG_KEY).getOrElse(DEFAULT_SPARK_CATALOG)
    val catalogMapping = if (mappingEnabled) {
      val targetDefaultCatalog =
        catalogSetting(spark, DEFAULT_TARGET_CATALOG_KEY).getOrElse(DEFAULT_TARGET_CATALOG)
      // the default-catalog pair is the fallback, so an explicit mapping entry for the
      // session catalog overrides it rather than the other way round
      Map(sparkDefaultCatalog -> targetDefaultCatalog) ++
        parseMappingString(catalogSetting(spark, CATALOG_MAPPING_KEY).getOrElse(""))
    } else {
      Map.empty[String, String]
    }

    LOG.info(
      "AccessResource initialized: resourceMode={}, mappingEnabled={}, " +
        "defaultSparkCatalog={}, mappings={}",
      Array[Object](
        resourceMode,
        mappingEnabled.toString,
        sparkDefaultCatalog,
        catalogMapping.toString()): _*)
    CatalogConf(resourceMode, mappingEnabled, sparkDefaultCatalog, catalogMapping)
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
   * Get the effective catalog name, applying mapping and defaults.
   *
   * `None` means "the session catalog": a v1 command that can only ever address
   * `spark_catalog`. It is NOT a stand-in for an unknown catalog -- passing it for a table
   * that lives elsewhere silently addresses the default catalog's policies instead, which
   * is how masks and row filters get lost. Callers that have a catalog must pass it.
   */
  def getEffectiveCatalog(catalog: Option[String])(implicit spark: SparkSession): String = {
    val conf = catalogConf(spark)
    val rawCatalog = catalog.getOrElse(conf.sparkDefaultCatalog)
    val effective = conf.mapCatalog(rawCatalog)
    if (LOG.isDebugEnabled) {
      LOG.debug(
        "getEffectiveCatalog: input={}, raw={}, effective={}",
        Array[Object](catalog.orNull, rawCatalog, effective): _*)
    }
    effective
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
    if (objectType != URI) {
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
   * Build an AccessResource from positional string arguments, catalog-mode-aware.
   *
   * `catalog` is deliberately without a default: the resource a mask or row-filter lookup
   * is built from has to name the catalog its table actually lives in, and omitting it
   * resolves to the default catalog instead of failing. Pass `None` only where the object
   * can only come from the session catalog.
   */
  def apply(
      objectType: ObjectType,
      firstLevelResource: String,
      secondLevelResource: String,
      thirdLevelResource: String,
      owner: Option[String] = None,
      catalog: Option[String]): AccessResource = {
    implicit val spark: SparkSession = SparkSession.active
    val conf = catalogConf(spark)
    val resource = new AccessResource(objectType, catalog)

    def setCatalogAndSchema(schema: Option[String]): Unit = {
      if (conf.isCatalogMode) {
        resource.setValue("catalog", getEffectiveCatalog(catalog))
        schema.foreach(db => resource.setValue("schema", db))
      } else {
        resource.setValue("database", schema.orNull)
      }
    }

    objectType match {
      case DATABASE =>
        setCatalogAndSchema(Option(firstLevelResource))
      case FUNCTION =>
        if (conf.isCatalogMode) {
          setCatalogAndSchema(Option(firstLevelResource).filter(_.nonEmpty))
        } else {
          resource.setValue("database", Option(firstLevelResource).getOrElse(""))
        }
        resource.setValue("udf", secondLevelResource)
      case COLUMN =>
        setCatalogAndSchema(Option(firstLevelResource))
        resource.setValue("table", secondLevelResource)
        resource.setValue("column", thirdLevelResource)
      case TABLE | VIEW | INDEX =>
        setCatalogAndSchema(Option(firstLevelResource))
        resource.setValue("table", secondLevelResource)
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
    if (conf.isCatalogMode) {
      requireCatalog(resource, objectType)
    }
    resource.setServiceDef(SparkRangerAdminPlugin.getServiceDef)
    owner.foreach(resource.setOwnerUser)
    if (LOG.isDebugEnabled) {
      LOG.debug(
        "apply: objectType={}, mode={}, catalog={}, resource={}",
        Array[Object](
          objectType.toString,
          conf.resourceMode,
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
   * Drop the cached per-session settings, so the next resource re-reads them.
   */
  def reset(): Unit = catalogConfs.clear()
}
