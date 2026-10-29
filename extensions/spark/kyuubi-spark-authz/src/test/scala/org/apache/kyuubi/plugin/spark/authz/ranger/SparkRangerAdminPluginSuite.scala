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

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSessionExtensions

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.spark.authz.{ObjectType, OperationType, SparkSessionProvider}
import org.apache.kyuubi.plugin.spark.authz.RangerTestNamespace._
import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAdminPlugin._

class SparkRangerAdminPluginSuite extends KyuubiFunSuite with SparkSessionProvider {
  override protected val catalogImpl: String = "in-memory"
  // installing the extension is what initializes the Ranger plugin, and without it every
  // policy lookup below returns nothing
  override protected val extension: SparkSessionExtensions => Unit = new RangerSparkExtension

  override def beforeAll(): Unit = {
    super.beforeAll()
    // `spark` is lazy, and building a resource resolves the catalog mapping through
    // SparkSession.active, so the session has to exist before the first request.
    assert(spark.sparkContext != null)
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("get filter expression") {
    val bob = UserGroupInformation.createRemoteUser("bob")
    val are = AccessResource(ObjectType.TABLE, defaultDb, "src", null, catalog = None)
    def buildAccessRequest(ugi: UserGroupInformation): AccessRequest = {
      AccessRequest(are, ugi, OperationType.QUERY, AccessType.SELECT)
    }
    val maybeString = getFilterExpr(buildAccessRequest(bob))
    assert(maybeString.get === "key<20")
    Seq(admin, alice).foreach { user =>
      val ugi = UserGroupInformation.createRemoteUser(user)
      val maybeString = getFilterExpr(buildAccessRequest(ugi))
      assert(maybeString.isEmpty)
    }
  }

  test("get data masker") {
    val bob = UserGroupInformation.createRemoteUser("bob")
    def buildAccessRequest(ugi: UserGroupInformation, column: String): AccessRequest = {
      val are = AccessResource(ObjectType.COLUMN, defaultDb, "src", column, catalog = None)
      AccessRequest(are, ugi, OperationType.QUERY, AccessType.SELECT)
    }
    assert(getMaskingExpr(buildAccessRequest(bob, "value1")).get === "md5(cast(value1 as string))")
    assert(getMaskingExpr(buildAccessRequest(bob, "value2")).get ===
      "regexp_replace(regexp_replace(regexp_replace(regexp_replace(value2, '[A-Z]', 'X')," +
      " '[a-z]', 'x'), '[0-9]', 'n'), '[^A-Za-z0-9]', 'U')")
    assert(getMaskingExpr(buildAccessRequest(bob, "value3")).get contains "regexp_replace")
    assert(getMaskingExpr(buildAccessRequest(bob, "value4")).get === "date_trunc('YEAR', value4)")
    assert(getMaskingExpr(buildAccessRequest(bob, "value5")).get ===
      "concat(regexp_replace(regexp_replace(regexp_replace(regexp_replace(" +
      "left(value5, length(value5) - 4), '[A-Z]', 'X'), '[a-z]', 'x')," +
      " '[0-9]', 'n'), '[^A-Za-z0-9]', 'U'), right(value5, 4))")

    Seq(admin, alice).foreach { user =>
      val ugi = UserGroupInformation.createRemoteUser(user)
      val maybeString = getMaskingExpr(buildAccessRequest(ugi, "value1"))
      assert(maybeString.isEmpty)
    }
  }

  test("mask and row filter lookups are catalog-scoped") {
    val bob = UserGroupInformation.createRemoteUser("bob")
    // The test policies are rooted at the `iceberg` catalog, which the default catalog is
    // mapped onto. A table of the same name in a catalog without policies must not inherit
    // them -- so a mask or row-filter lookup that resolves the catalog to the default
    // instead of the table's own silently reads another catalog's policies.
    val unpoliced = Some("catalog_without_policies")

    val maskedColumn = AccessResource(ObjectType.COLUMN, defaultDb, "src", "value1", None, None)
    val sameColumnElsewhere =
      AccessResource(ObjectType.COLUMN, defaultDb, "src", "value1", None, unpoliced)
    assert(getMaskingExpr(request(maskedColumn, bob)).nonEmpty)
    assert(getMaskingExpr(request(sameColumnElsewhere, bob)).isEmpty)

    val filteredTable = AccessResource(ObjectType.TABLE, defaultDb, "src", null, None, None)
    val sameTableElsewhere =
      AccessResource(ObjectType.TABLE, defaultDb, "src", null, None, unpoliced)
    assert(getFilterExpr(request(filteredTable, bob)).nonEmpty)
    assert(getFilterExpr(request(sameTableElsewhere, bob)).isEmpty)
  }

  private def request(resource: AccessResource, ugi: UserGroupInformation): AccessRequest = {
    AccessRequest(resource, ugi, OperationType.QUERY, AccessType.SELECT)
  }
}
