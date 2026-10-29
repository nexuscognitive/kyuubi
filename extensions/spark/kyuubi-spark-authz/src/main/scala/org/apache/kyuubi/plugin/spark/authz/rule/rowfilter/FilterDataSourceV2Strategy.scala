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
package org.apache.kyuubi.plugin.spark.authz.rule.rowfilter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.SparkStrategy

import org.apache.kyuubi.plugin.spark.authz.serde.{lookupExtractor, ResolvedNamespaceCatalogExtractor}

case class FilterDataSourceV2Strategy(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ObjectFilterPlaceHolder(child)
        if child.nodeName == "ShowNamespaces" || child.nodeName == "ShowNamespacesCommand" =>
      spark.sessionState.planner.plan(child)
        .map(FilteredShowNamespaceExec(_, spark.sparkContext, catalogOf(child))).toSeq

    case ObjectFilterPlaceHolder(child) if child.nodeName == "ShowTables" =>
      spark.sessionState.planner.plan(child)
        .map(FilteredShowTablesExec(_, spark.sparkContext, catalogOf(child))).toSeq

    case _ => Nil
  }

  /**
   * The catalog the v2 SHOW command lists, taken from its resolved namespace. None means
   * the session catalog -- the filtered rows are checked against catalog-scoped
   * resources, so the catalog cannot be left to default.
   */
  private def catalogOf(plan: LogicalPlan): Option[String] = {
    plan.collectFirst { case p if p.nodeName == "ResolvedNamespace" => p }
      .flatMap(lookupExtractor[ResolvedNamespaceCatalogExtractor].apply)
  }
}
