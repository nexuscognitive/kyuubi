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

import org.apache.kyuubi.util.reflect.ReflectUtils._

trait CatalogExtractor extends (AnyRef => Option[String]) with Extractor

object CatalogExtractor {
  val catalogExtractors: Map[String, CatalogExtractor] = {
    loadExtractorsToMap[CatalogExtractor]
  }
}

/**
 * org.apache.spark.sql.connector.catalog.CatalogPlugin
 */
class CatalogPluginCatalogExtractor extends CatalogExtractor {
  override def apply(v1: AnyRef): Option[String] = {
    Option(invokeAs[String](v1, "name"))
  }
}

/**
 * Option[org.apache.spark.sql.connector.catalog.CatalogPlugin]
 */
class CatalogPluginOptionCatalogExtractor extends CatalogExtractor {
  override def apply(v1: AnyRef): Option[String] = {
    v1 match {
      case Some(catalogPlugin: AnyRef) =>
        lookupExtractor[CatalogPluginCatalogExtractor].apply(catalogPlugin)
      case _ => None
    }
  }
}

/**
 * Option[String]
 */
class StringOptionCatalogExtractor extends CatalogExtractor {
  override def apply(v1: AnyRef): Option[String] = {
    v1.asInstanceOf[Option[String]]
  }
}

/**
 * Extracts catalog from org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
 *
 * DataSourceV2Relation has: catalog: Option[CatalogPlugin]
 */
class DataSourceV2RelationCatalogExtractor extends CatalogExtractor {
  override def apply(v1: AnyRef): Option[String] = {
    try {
      val catalogOption = invokeAs[Option[AnyRef]](v1, "catalog")
      catalogOption.flatMap { catalogPlugin =>
        Option(invokeAs[String](catalogPlugin, "name"))
      }
    } catch {
      case _: Exception => None
    }
  }
}

/**
 * Extracts catalog from org.apache.spark.sql.catalyst.analysis.ResolvedTable
 *
 * ResolvedTable has: catalog: TableCatalog (which extends CatalogPlugin)
 */
class ResolvedTableCatalogExtractor extends CatalogExtractor {
  override def apply(v1: AnyRef): Option[String] = {
    try {
      val catalog = invokeAs[AnyRef](v1, "catalog")
      if (catalog != null) {
        Option(invokeAs[String](catalog, "name"))
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }
}

/**
 * Extracts catalog from org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
 *
 * ResolvedIdentifier has: catalog: CatalogPlugin
 */
class ResolvedIdentifierCatalogExtractor extends CatalogExtractor {
  override def apply(v1: AnyRef): Option[String] = {
    try {
      val catalog = invokeAs[AnyRef](v1, "catalog")
      if (catalog != null) {
        Option(invokeAs[String](catalog, "name"))
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }
}

/**
 * Extracts catalog from org.apache.spark.sql.catalyst.analysis.ResolvedDBObjectName
 * (Spark 3.3+)
 *
 * ResolvedDBObjectName has: catalog: CatalogPlugin
 */
class ResolvedDbObjectNameCatalogExtractor extends CatalogExtractor {
  override def apply(v1: AnyRef): Option[String] = {
    try {
      val catalog = invokeAs[AnyRef](v1, "catalog")
      if (catalog != null) {
        Option(invokeAs[String](catalog, "name"))
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }
}

/**
 * Extracts catalog from CatalogTable
 * CatalogTable.identifier.catalog returns Option[String]
 */
class CatalogTableCatalogExtractor extends CatalogExtractor {
  override def apply(v1: AnyRef): Option[String] = {
    try {
      val identifier = invokeAs[AnyRef](v1, "identifier")
      if (identifier != null) {
        try {
          invokeAs[Option[String]](identifier, "catalog")
        } catch {
          case _: Exception => None
        }
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }
}

/**
 * Extracts catalog from Option[CatalogTable]
 */
class CatalogTableOptionCatalogExtractor extends CatalogExtractor {
  override def apply(v1: AnyRef): Option[String] = {
    v1 match {
      case Some(catalogTable: AnyRef) =>
        lookupExtractor[CatalogTableCatalogExtractor].apply(catalogTable)
      case _ => None
    }
  }
}

/**
 * Extracts catalog from TableIdentifier
 * TableIdentifier.catalog returns Option[String] (Spark 3.4+)
 */
class TableIdentifierCatalogExtractor extends CatalogExtractor {
  override def apply(v1: AnyRef): Option[String] = {
    try {
      invokeAs[Option[String]](v1, "catalog")
    } catch {
      case _: Exception => None
    }
  }
}

/**
 * Extracts catalog from Option[TableIdentifier]
 */
class TableIdentifierOptionCatalogExtractor extends CatalogExtractor {
  override def apply(v1: AnyRef): Option[String] = {
    v1 match {
      case Some(tableIdentifier: AnyRef) =>
        lookupExtractor[TableIdentifierCatalogExtractor].apply(tableIdentifier)
      case _ => None
    }
  }
}

/**
 * Extracts catalog from org.apache.spark.sql.catalyst.analysis.ResolvedNamespace
 *
 * ResolvedNamespace has: catalog: CatalogPlugin
 */
class ResolvedNamespaceCatalogExtractor extends CatalogExtractor {
  override def apply(v1: AnyRef): Option[String] = {
    try {
      val catalog = invokeAs[AnyRef](v1, "catalog")
      if (catalog != null) {
        Option(invokeAs[String](catalog, "name"))
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }
}
