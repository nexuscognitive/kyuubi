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

package org.apache.kyuubi.plugin.spark.authz.serde

import java.util.{HashMap => JHashMap, LinkedHashMap, Map => JMap}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.plugin.spark.authz.util.PathIdentifier._
import org.apache.kyuubi.util.reflect.ReflectUtils._

/**
 * A trait for extracting database and table as string tuple
 * from the give object whose class type is define by `key`.
 */
trait TableExtractor extends ((SparkSession, AnyRef) => Option[Table]) with Extractor

object TableExtractor {
  val tableExtractors: Map[String, TableExtractor] = {
    loadExtractorsToMap[TableExtractor]
  }

  /**
   * Get table owner from table properties
   * @param v a object contains a org.apache.spark.sql.connector.catalog.Table
   * @return owner
   */
  def getOwner(v: AnyRef): Option[String] = {
    // org.apache.spark.sql.connector.catalog.Table
    val table = invokeAs[AnyRef](v, "table")
    val properties = invokeAs[JMap[String, String]](table, "properties").asScala
    properties.get("owner")
  }

  def getOwner(spark: SparkSession, catalogName: String, tableIdent: AnyRef): Option[String] = {
    try {
      val catalogManager = invokeAs[AnyRef](spark.sessionState, "catalogManager")
      val catalog = invokeAs[AnyRef](catalogManager, "catalog", (classOf[String], catalogName))
      val table = invokeAs[AnyRef](
        catalog,
        "loadTable",
        (Class.forName("org.apache.spark.sql.connector.catalog.Identifier"), tableIdent))
      getOwner(table)
    } catch {
      // Exception may occur due to invalid reflection or table not found
      case _: Exception => None
    }
  }

  /**
   * Get owner from a `org.apache.spark.sql.execution.datasources.LogicalRelation`
   * that wraps a Spark `CatalogTable`.
   */
  def getLogicalRelationOwner(v: AnyRef): Option[String] = {
    try {
      val maybeCatalogTable = invokeAs[Option[CatalogTable]](v, "catalogTable")
      maybeCatalogTable.flatMap(ct => Option(ct.owner).filter(_.nonEmpty))
    } catch {
      case _: Exception => None
    }
  }
}

/**
 * org.apache.spark.sql.catalyst.TableIdentifier
 */
class TableIdentifierTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val identifier = v1.asInstanceOf[TableIdentifier]
    if (isPathIdentifier(identifier.table, spark)) {
      None
    } else {
      val owner =
        try {
          val catalogTable = spark.sessionState.catalog.getTableMetadata(identifier)
          Option(catalogTable.owner).filter(_.nonEmpty)
        } catch {
          case _: Exception => None
        }
      Some(Table(None, identifier.database, identifier.table, owner))
    }
  }
}

/**
 * org.apache.spark.sql.catalyst.TableIdentifier Option
 */
class TableIdentifierOptionTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val tableIdentifier = v1.asInstanceOf[Option[TableIdentifier]]
    tableIdentifier.flatMap(lookupExtractor[TableIdentifierTableExtractor].apply(spark, _))
  }
}

/**
 * org.apache.spark.sql.catalyst.catalog.CatalogTable
 */
class CatalogTableTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    if (null == v1) {
      None
    } else {
      val catalogTable = v1.asInstanceOf[CatalogTable]
      val identifier = catalogTable.identifier
      val owner = Option(catalogTable.owner).filter(_.nonEmpty)
      Some(Table(None, identifier.database, identifier.table, owner))
    }
  }
}

/**
 * org.apache.spark.sql.catalyst.catalog.CatalogTable Option
 */
class CatalogTableOptionTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val catalogTable = v1.asInstanceOf[Option[CatalogTable]]
    catalogTable.flatMap(lookupExtractor[CatalogTableTableExtractor].apply(spark, _))
  }
}

/**
 * org.apache.spark.sql.catalyst.analysis.ResolvedTable
 */
class ResolvedTableTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val catalogVal = invokeAs[AnyRef](v1, "catalog")
    val catalog = lookupExtractor[CatalogPluginCatalogExtractor].apply(catalogVal)
    val identifier = invokeAs[AnyRef](v1, "identifier")
    val maybeTable = lookupExtractor[IdentifierTableExtractor].apply(spark, identifier)
    val maybeOwner = TableExtractor.getOwner(v1)
    maybeTable.map(_.copy(catalog = catalog, owner = maybeOwner))
  }
}

/**
 * org.apache.spark.sql.connector.catalog.Identifier
 */
class IdentifierTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = v1 match {
    case identifier: Identifier if !isPathIdentifier(identifier.name(), spark) =>
      Some(Table(None, Some(quote(identifier.namespace())), identifier.name(), None))
    case _ => None
  }
}

/**
 * java.lang.String
 * with concat parts by "."
 */
class StringTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val tableNameArr = v1.asInstanceOf[String].split("\\.")
    val maybeTable = tableNameArr.length match {
      case 1 => Table(None, None, tableNameArr(0), None)
      case 2 => Table(None, Some(tableNameArr(0)), tableNameArr(1), None)
      case 3 => Table(Some(tableNameArr(0)), Some(tableNameArr(1)), tableNameArr(2), None)
      // 4+ parts: catalog.<multi-level namespace>.table, where the namespace levels
      // are flattened into a single (dot-joined) database string.
      case _ =>
        Table(
          Some(tableNameArr.head),
          Some(quote(tableNameArr.slice(1, tableNameArr.length - 1))),
          tableNameArr.last,
          None)
    }
    Option(maybeTable)
  }
}

/**
 * Seq[org.apache.spark.sql.catalyst.expressions.Expression]
 */
class ExpressionSeqTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val expressions = v1.asInstanceOf[Seq[Expression]]
    // Iceberg will rearrange the parameters according to the parameter order
    // defined in the procedure, where the table parameters are currently always the first.
    lookupExtractor[StringTableExtractor].apply(spark, expressions.head.toString())
  }
}

/**
 * org.apache.spark.sql.catalyst.plans.logical.AddPartitionField
 */
class ArrayBufferTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    // Iceberg will transform table to ArrayBuffer[String]
    val maybeTable = v1.asInstanceOf[Seq[String]] match {
      case Seq(tblName) => Table(None, None, tblName, None)
      case Seq(dbName, tblName) => Table(None, Some(dbName), tblName, None)
      case Seq(catalogName, dbName, tblName) =>
        Table(Some(catalogName), Some(dbName), tblName, None)
      // 4+ parts: catalog.<multi-level namespace>.table, where the namespace levels
      // are flattened into a single (dot-joined) database string.
      case parts =>
        Table(
          Some(parts.head),
          Some(quote(parts.slice(1, parts.length - 1))),
          parts.last,
          None)
    }
    Option(maybeTable)
  }
}

/**
 * Resolves the namespace of a `DataSourceV2Relation` that carries neither a catalog
 * nor an identifier, for connectors that keep their namespace in the relation's
 * options instead of in a Spark identifier.
 *
 * A `TableProvider` that does not implement `SupportsCatalogOptions` -- the MongoDB
 * connector, the ClickHouse native connector -- goes through Spark's non-catalog
 * fallback, which passes `(catalog, identifier) = (None, None)`. The only identity
 * left on the relation is `table.name()`, which is connector-defined and may carry no
 * namespace at all. For `com.mongodb.spark.sql.connector.MongoTable` it carries none
 * by construction: `MongoTableProvider.getTable` always builds a `SimpleMongoConfig`,
 * which is neither a `ReadConfig` nor a `WriteConfig`, so `MongoTable.name()` falls to
 * its `else` branch and returns the constant string `MongoTable()`. Every MongoDB
 * relation in the cluster therefore reports the same name, so a resource built from it
 * is unscoped -- one grant would cover every database and collection -- and also
 * unmatchable, because it has no `schema` level and Ranger's hierarchy matcher
 * refuses to match a resource that is missing a parent level.
 */
private object ExternalDataSourceV2Namespace {

  final private val LOG = LoggerFactory.getLogger(getClass)

  final private val MONGO_TABLE_CLASS = "com.mongodb.spark.sql.connector.MongoTable"
  final private val MONGO_CONFIG_CLASS = "com.mongodb.spark.sql.connector.config.MongoConfig"
  final private val MONGO_CATALOG = "mongodb"

  /**
   * The connector's two usage namespaces, as (factory method, SparkConf key prefix).
   */
  final private val MONGO_USAGE_MODES = Seq(
    "writeConfig" -> "spark.mongodb.write.",
    "readConfig" -> "spark.mongodb.read.")

  /**
   * Catalog label for a catalog-less relation whose provider is not recognised here.
   * Such a relation is not in any Spark catalog, so leaving its catalog empty lets
   * `AccessResource` fold it into the session default catalog -- which maps onto the
   * Iceberg policy namespace and makes the audit record actively misleading. These
   * resources are denied either way (they carry no schema level), so this only changes
   * the label, not the outcome.
   */
  final val EXTERNAL_CATALOG = "external"

  /**
   * Deny-by-default namespace, used when the provider is recognised but its namespace
   * does not resolve to exactly one database and table.
   *
   * This has to be a resource rather than `None`, and it cannot be an exception.
   * Returning `None` would drop the privilege object, and `AppendData` carries both
   * uri and query descriptors, so `PrivilegesBuilder`'s command-level fail-closed guard
   * does not fire for it -- the write would be silently skipped, with no Ranger request
   * and no audit event. Throwing does not work either: `getTablePriv` swallows every
   * `Exception`, and `AccessControlException` is a `RuntimeException`. Emitting a
   * sentinel resource keeps the request, and with it the audit record and the deny.
   */
  final val UNRESOLVED = "__unresolved__"

  def apply(v2Relation: DataSourceV2Relation): Option[Table] =
    v2Relation.table.getClass.getName match {
      case MONGO_TABLE_CLASS => Some(mongoTable(v2Relation))
      case _ => None
    }

  private def mongoTable(v2Relation: DataSourceV2Relation): Table = {
    // The same map the connector itself received from DataFrameWriter/loadV2Source.
    val options = v2Relation.options.asCaseSensitiveMap()

    // Resolve through the connector's own config API rather than reading `database`
    // and `collection` out of the options here. The connector layers three sources --
    // explicit options, SparkConf under `spark.mongodb.{read,write}.`, and the
    // database/collection path of `connection.uri` -- and reimplementing that
    // precedence would silently drift from it on the next connector bump.
    //
    // The usage mode is not knowable from the relation alone, so both are tried.
    // Explicit options win inside the connector, so the mode only changes the answer
    // when the namespace comes from SparkConf, and then normally only one of the two
    // prefixes is set. If both resolve and disagree, the namespace is ambiguous and we
    // deny. If only the mode opposite to the actual operation resolves we authorize the
    // wrong namespace, but the connector builds its config the same way at execution
    // time, so that operation cannot succeed either.
    val namespaces = MONGO_USAGE_MODES.flatMap(resolveNamespace(options, _)).distinct

    namespaces match {
      case Seq((database, collection)) =>
        Table(Some(MONGO_CATALOG), Some(database), collection, None)
      case ambiguous =>
        LOG.warn(
          "Could not resolve a single MongoDB namespace for authorization " +
            "({} candidates); denying via {}/{}. A multi-collection spec " +
            "(collection=\"*\" or a comma-separated list) is not authorizable as a " +
            "single resource.",
          Array[Object](Int.box(ambiguous.size), MONGO_CATALOG, UNRESOLVED): _*)
        Table(Some(MONGO_CATALOG), Some(UNRESOLVED), UNRESOLVED, None)
    }
  }

  /**
   * Asks the connector to resolve `options` in one usage mode, or None if it will not.
   */
  private def resolveNamespace(
      options: JMap[String, String],
      usageMode: (String, String)): Option[(String, String)] = {
    val (configFactory, usagePrefix) = usageMode
    try {
      // The Read/WriteConfig factories keep only keys under `spark.mongodb.` and discard
      // everything else, so the relation's DataFrame `.option()` keys have to be lifted
      // into the usage namespace first. Verified against connector 10.6.1: an unprefixed
      // map produces an empty config whose getDatabaseName() reports "Missing
      // configuration for: database". Keys the caller already scoped (`spark.`-prefixed,
      // including `spark.mongodb.write.database`) pass through untouched.
      val usageOptions = new JHashMap[String, String]()
      options.asScala.foreach { case (key, value) =>
        usageOptions.put(if (key.startsWith("spark.")) key else usagePrefix + key, value)
      }
      val usageConfig = invokeAs[AnyRef](
        MONGO_CONFIG_CLASS,
        configFactory,
        (classOf[JMap[_, _]], usageOptions))
      // getCollectionName() throws unless CollectionsConfig.Type is SINGLE, so a
      // multi-collection spec -- collection="*", or a comma-separated list -- lands on
      // UNRESOLVED. Authorizing that as one resource would misstate what is accessed;
      // it needs one privilege object per collection, which a single-Table extractor
      // cannot express.
      //
      // Every member is resolved against MongoConfig, which is public. Read/WriteConfig
      // inherit these from the package-private AbstractMongoConfig, and resolving
      // through that class cannot link from outside its package.
      Some((
        invokeAs[String]((MONGO_CONFIG_CLASS, usageConfig), "getDatabaseName"),
        invokeAs[String]((MONGO_CONFIG_CLASS, usageConfig), "getCollectionName")))
    } catch {
      // The connector rejected these options -- no database, or a collection spec it
      // will not reduce to one name. Expected; the caller denies via UNRESOLVED.
      case NonFatal(e) =>
        LOG.debug(s"MongoDB namespace resolution via $configFactory did not apply", e)
        None
      // The connector's own classes are on the classpath but its dependencies (org.bson)
      // are not, so reflecting over its methods cannot link. This must be caught here:
      // LinkageError is not an Exception, so neither NonFatal above nor the catch in
      // PrivilegesBuilder.getTablePriv would stop it, and it would surface as a query
      // crash rather than an access decision.
      case e: LinkageError =>
        LOG.warn(
          "MongoDB connector classes are present but not fully linked, so its namespace " +
            "cannot be resolved for authorization; denying via {}/{}",
          Array[Object](MONGO_CATALOG, UNRESOLVED): _*)
        LOG.debug("MongoDB connector linkage failure", e)
        None
    }
  }
}

/**
 * org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
 */
class DataSourceV2RelationTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val plan = v1.asInstanceOf[LogicalPlan]
    plan.find(_.getClass.getSimpleName == "DataSourceV2Relation").get match {
      // NX1: upstream KYUUBI #7230 added an opt-out that returns None for a relation
      // carrying neither catalog nor identifier (a TableProvider without
      // SupportsCatalogOptions, e.g. spark.read.format("mongodb")). That is a
      // privilege-check bypass -- a skipped relation produces no privilege object,
      // no Ranger request, and no audit event. This deployment authorizes every
      // relation, so the skip is deliberately not wired up here: such a relation falls
      // through to the extraction below, which resolves the connector's real namespace
      // where it can (see ExternalDataSourceV2Namespace) and otherwise emits a
      // deny-by-default resource so the access is still requested and audited.
      case v2Relation: DataSourceV2Relation
          if v2Relation.identifier.isEmpty ||
            !isPathIdentifier(v2Relation.identifier.get.name(), spark) =>
        val maybeCatalog = v2Relation.catalog.flatMap(catalogPlugin =>
          lookupExtractor[CatalogPluginCatalogExtractor].apply(catalogPlugin))
        val maybeOwner = TableExtractor.getOwner(v2Relation)
        // Prefer the relation's `identifier`: its `namespace()` is a real
        // multi-level array, so nested namespaces (including a single level that
        // contains a dot, e.g. `cat`.`a.b`.`tbl`) are preserved exactly. The
        // `table.name()` string is dot-flattened and lossy - it cannot tell a
        // nested namespace from a dotted single level, and yields an empty
        // database for catalogs that don't encode the namespace in the name,
        // which would then be defaulted to the current database (fail-open).
        val identifierTable = invokeAs[Option[AnyRef]](v2Relation, "identifier")
          .flatMap(id => lookupExtractor[IdentifierTableExtractor].apply(spark, id))
        identifierTable match {
          case Some(table) =>
            Some(table.copy(catalog = maybeCatalog, owner = maybeOwner))
          // No identifier: a non-catalog TableProvider relation. `table.name()` is the
          // last resort and for some connectors carries no namespace at all, so ask the
          // connector-specific resolver for the real one first. It keeps whatever
          // catalog it assigns, since a relation-level catalog is absent by definition
          // on this path.
          case None =>
            ExternalDataSourceV2Namespace(v2Relation)
              .map(_.copy(owner = maybeOwner))
              .orElse(
                lookupExtractor[TableTableExtractor].apply(spark, v2Relation.table)
                  .map(_.copy(
                    catalog = maybeCatalog.orElse(
                      Some(ExternalDataSourceV2Namespace.EXTERNAL_CATALOG)),
                    owner = maybeOwner)))
        }
      case _ => None
    }
  }
}

/**
 * org.apache.spark.sql.execution.datasources.LogicalRelation
 */
class LogicalRelationTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val maybeCatalogTable = invokeAs[Option[AnyRef]](v1, "catalogTable")
    maybeCatalogTable.flatMap { ct =>
      lookupExtractor[CatalogTableTableExtractor].apply(spark, ct)
    }
  }
}

/**
 * org.apache.spark.sql.catalyst.analysis.ResolvedDbObjectName
 */
class ResolvedDbObjectNameTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val nameParts = invokeAs[Seq[String]](v1, "nameParts")
    val table = nameParts.last
    if (isPathIdentifier(table, spark)) {
      None
    } else {
      val catalogVal = invokeAs[AnyRef](v1, "catalog")
      val catalog = lookupExtractor[CatalogPluginCatalogExtractor].apply(catalogVal)
      val namespace = nameParts.init.toArray
      Some(Table(catalog, Some(quote(namespace)), table, None))
    }
  }
}

/**
 * org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
 */
class ResolvedIdentifierTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    v1.getClass.getName match {
      case "org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier" =>
        val catalogVal = invokeAs[AnyRef](v1, "catalog")
        val catalog = lookupExtractor[CatalogPluginCatalogExtractor].apply(catalogVal)
        val identifier = invokeAs[AnyRef](v1, "identifier")
        val maybeTable = lookupExtractor[IdentifierTableExtractor].apply(spark, identifier)
        val owner = catalog.flatMap(name => TableExtractor.getOwner(spark, name, identifier))
        maybeTable.map(_.copy(catalog = catalog, owner = owner))
      case _ => None
    }
  }
}

/**
 * org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
 */
class SubqueryAliasTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    v1.asInstanceOf[SubqueryAlias] match {
      case SubqueryAlias(_, SubqueryAlias(identifier, _)) =>
        if (isPathIdentifier(identifier.name, spark)) {
          None
        } else {
          lookupExtractor[StringTableExtractor].apply(spark, identifier.toString())
        }
      case SubqueryAlias(identifier, _) if !isPathIdentifier(identifier.name, spark) =>
        lookupExtractor[StringTableExtractor].apply(spark, identifier.toString())
      case _ => None
    }
  }
}

/**
 * org.apache.spark.sql.connector.catalog.Table
 */
class TableTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val tableName = invokeAs[String](v1, "name")
    lookupExtractor[StringTableExtractor].apply(spark, tableName)
  }
}

class HudiDataSourceV2RelationTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    invokeAs[LogicalPlan](v1, "table") match {
      // Match multipartIdentifier with tableAlias
      case SubqueryAlias(_, SubqueryAlias(identifier, relation)) =>
        lookupExtractor[StringTableExtractor].apply(spark, identifier.toString())
          .map(_.copy(owner = TableExtractor.getLogicalRelationOwner(relation)))
      // Match multipartIdentifier without tableAlias
      case SubqueryAlias(identifier, relation) =>
        lookupExtractor[StringTableExtractor].apply(spark, identifier.toString())
          .map(_.copy(owner = TableExtractor.getLogicalRelationOwner(relation)))
      case _ => None
    }
  }
}

/**
 * Extracts a [[Table]] from a Hudi `HoodieCatalogTable` via its `table` field
 * (a Spark `CatalogTable`). Used as a fallback for Hudi commands whose plan
 * field was renamed across versions (e.g. `DeleteHoodieTableCommand.dft` in
 * Hudi 1.0.x vs `query` in 1.2.0); the `catalogTable` field is stable.
 */
class HoodieCatalogTableTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val catalogTable = invokeAs[CatalogTable](v1, "table")
    lookupExtractor[CatalogTableTableExtractor].apply(spark, catalogTable)
  }
}

class HudiMergeIntoTargetTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    invokeAs[LogicalPlan](v1, "targetTable") match {
      // Match multipartIdentifier with tableAlias
      case SubqueryAlias(_, SubqueryAlias(identifier, relation)) =>
        lookupExtractor[StringTableExtractor].apply(spark, identifier.toString())
          .map(_.copy(owner = TableExtractor.getLogicalRelationOwner(relation)))
      // Match multipartIdentifier without tableAlias
      case SubqueryAlias(identifier, relation) =>
        lookupExtractor[StringTableExtractor].apply(spark, identifier.toString())
          .map(_.copy(owner = TableExtractor.getLogicalRelationOwner(relation)))
      case _ => None
    }
  }
}

trait HudiCallProcedureExtractor {

  protected def extractTableIdentifier(
      procedure: AnyRef,
      args: AnyRef,
      tableParameterKey: String): Option[String] = {
    val tableIdentifierParameter =
      invokeAs[Array[AnyRef]](procedure, "parameters")
        .find(invokeAs[String](_, "name").equals(tableParameterKey))
        .getOrElse(throw new IllegalArgumentException(s"Could not find param $tableParameterKey"))
    val tableIdentifierParameterIndex = invokeAs[LinkedHashMap[String, Int]](args, "map")
      .getOrDefault(tableParameterKey, INVALID_INDEX)
    tableIdentifierParameterIndex match {
      case INVALID_INDEX =>
        None
      case argsIndex =>
        val dataType = invokeAs[DataType](tableIdentifierParameter, "dataType")
        val row = invokeAs[InternalRow](args, "internalRow")
        val tableName = InternalRow.getAccessor(dataType, true)(row, argsIndex)
        Option(tableName.asInstanceOf[UTF8String].toString)
    }
  }

  case class ProcedureArgsInputOutputTuple(
      inputTable: Option[String] = None,
      outputTable: Option[String] = None,
      inputUri: Option[String] = None,
      outputUri: Option[String] = None)

  protected val PROCEDURE_CLASS_PATH = "org.apache.spark.sql.hudi.command.procedures"

  protected val INVALID_INDEX = -1

  // These pairs are used to get the procedure input/output args which user passed in call command.
  protected val procedureArgsInputOutputPairs: Map[String, ProcedureArgsInputOutputTuple] = Map(
    (
      s"$PROCEDURE_CLASS_PATH.ArchiveCommitsProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.CommitsCompareProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.CopyToTableProcedure",
      ProcedureArgsInputOutputTuple(
        inputTable = Some("table"),
        outputTable = Some("new_table"))),
    (
      s"$PROCEDURE_CLASS_PATH.CopyToTempViewProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.CreateMetadataTableProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.CreateSavepointProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.DeleteMarkerProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.DeleteMetadataTableProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.DeleteSavepointProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ExportInstantsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.HdfsParquetImportProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.HelpProcedure",
      ProcedureArgsInputOutputTuple()),
    (
      s"$PROCEDURE_CLASS_PATH.HiveSyncProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.InitMetadataTableProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RepairAddpartitionmetaProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RepairCorruptedCleanFilesProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RepairDeduplicateProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RepairMigratePartitionMetaProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RepairOverwriteHoodiePropsProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RollbackToInstantTimeProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RollbackToSavepointProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RunBootstrapProcedure",
      ProcedureArgsInputOutputTuple(
        inputTable = Some("table"),
        outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RunCleanProcedure",
      ProcedureArgsInputOutputTuple(
        inputTable = Some("table"),
        outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RunClusteringProcedure",
      ProcedureArgsInputOutputTuple(
        inputTable = Some("table"),
        outputTable = Some("table"),
        outputUri = Some("path"))),
    (
      s"$PROCEDURE_CLASS_PATH.RunCompactionProcedure",
      ProcedureArgsInputOutputTuple(
        inputTable = Some("table"),
        outputTable = Some("table"),
        outputUri = Some("path"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowArchivedCommitsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowBootstrapMappingProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowClusteringProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"), inputUri = Some("path"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowCommitsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowCommitExtraMetadataProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowCommitFilesProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowCommitPartitionsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowCommitWriteStatsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowCompactionProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"), inputUri = Some("path"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowFileSystemViewProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowFsPathDetailProcedure",
      ProcedureArgsInputOutputTuple()),
    (
      s"$PROCEDURE_CLASS_PATH.ShowHoodieLogFileMetadataProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowHoodieLogFileRecordsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowInvalidParquetProcedure",
      ProcedureArgsInputOutputTuple()),
    (
      s"$PROCEDURE_CLASS_PATH.ShowMetadataTableFilesProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowMetadataTablePartitionsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowMetadataTableStatsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowRollbacksProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowSavepointsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowTablePropertiesProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.StatsFileSizeProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.StatsWriteAmplificationProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.UpgradeOrDowngradeProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ValidateHoodieSyncProcedure",
      ProcedureArgsInputOutputTuple(
        inputTable = Some("src_table"),
        outputTable = Some("dst_table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ValidateMetadataTableFilesProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))))
}

class HudiCallProcedureOutputTableExtractor
  extends TableExtractor with HudiCallProcedureExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val procedure = invokeAs[AnyRef](v1, "procedure")
    val args = invokeAs[AnyRef](v1, "args")
    procedureArgsInputOutputPairs.get(procedure.getClass.getName)
      .filter(_.outputTable.isDefined)
      .map { argsPairs =>
        val tableIdentifier = extractTableIdentifier(procedure, args, argsPairs.outputTable.get)
        lookupExtractor[StringTableExtractor].apply(spark, tableIdentifier.get).orNull
      }
  }
}

class HudiCallProcedureInputTableExtractor
  extends TableExtractor with HudiCallProcedureExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val procedure = invokeAs[AnyRef](v1, "procedure")
    val args = invokeAs[AnyRef](v1, "args")
    procedureArgsInputOutputPairs.get(procedure.getClass.getName)
      .filter(_.inputTable.isDefined)
      .map { argsPairs =>
        val tableIdentifier = extractTableIdentifier(procedure, args, argsPairs.inputTable.get)
        lookupExtractor[StringTableExtractor].apply(spark, tableIdentifier.get).orNull
      }
  }
}

class HudiCallProcedureInputUriExtractor
  extends URIExtractor with HudiCallProcedureExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = {
    val procedure = invokeAs[AnyRef](v1, "procedure")
    val args = invokeAs[AnyRef](v1, "args")
    procedureArgsInputOutputPairs.get(procedure.getClass.getName)
      .filter(_.inputUri.isDefined)
      .map { argsPairs =>
        val tableIdentifier = extractTableIdentifier(procedure, args, argsPairs.inputUri.get)
        lookupExtractor[StringURIExtractor].apply(spark, tableIdentifier.get)
      }.getOrElse(Nil)
  }
}

class HudiCallProcedureOutputUriExtractor
  extends URIExtractor with HudiCallProcedureExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = {
    val procedure = invokeAs[AnyRef](v1, "procedure")
    val args = invokeAs[AnyRef](v1, "args")
    procedureArgsInputOutputPairs.get(procedure.getClass.getName)
      .filter(_.outputUri.isDefined)
      .map { argsPairs =>
        val tableIdentifier = extractTableIdentifier(procedure, args, argsPairs.outputUri.get)
        lookupExtractor[StringURIExtractor].apply(spark, tableIdentifier.get)
      }.getOrElse(Nil)
  }
}
