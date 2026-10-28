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

import java.net.URI

import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat

import org.apache.kyuubi.KyuubiFunSuite

class CatalogStorageFormatURIExtractorSuite extends KyuubiFunSuite {

  private val extractor = new CatalogStorageFormatURIExtractor

  private def storageWith(location: Option[URI]): CatalogStorageFormat =
    CatalogStorageFormat(
      locationUri = location,
      inputFormat = None,
      outputFormat = None,
      serde = None,
      compressed = false,
      properties = Map.empty)

  test("preserves scheme and authority for s3a:// locations") {
    val storage = storageWith(Some(URI.create("s3a://bucket/warehouse/t1")))
    // Pre-fix returned Uri("/warehouse/t1"): URI.getPath drops scheme + authority
    // and Ranger policies written against the full URL silently miss.
    assert(extractor(null, storage) === Seq(Uri("s3a://bucket/warehouse/t1")))
  }

  test("preserves scheme for hdfs:// locations") {
    val storage = storageWith(Some(URI.create("hdfs://ns/foo/bar")))
    assert(extractor(null, storage) === Seq(Uri("hdfs://ns/foo/bar")))
  }

  test("passes plain paths through unchanged") {
    val storage = storageWith(Some(URI.create("/warehouse/t1")))
    assert(extractor(null, storage) === Seq(Uri("/warehouse/t1")))
  }

  test("yields no URI when location is unset") {
    assert(extractor(null, storageWith(None)) === Seq.empty)
  }
}
