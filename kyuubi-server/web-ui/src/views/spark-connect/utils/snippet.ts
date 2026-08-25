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

const USE_SSL_PARAM = 'use_ssl'
const TOKEN_PARAM = 'token'
const PARAM_SEPARATOR = ';'

/**
 * The connection string to hand to `SparkSession.builder.remote(...)`.
 *
 * Spark Connect parses everything after the authority as `/;key=value;key=value`, so the path
 * separator has to be present even though the path itself is always empty here. Getting that
 * single slash wrong is the difference between a working session and an opaque parse error, which
 * is why this lives in a tested function rather than in a template string in the view.
 *
 * `use_ssl=true` is unconditional. A Spark Connect client refuses a plaintext channel once a
 * bearer token is set for a non-loopback host, so a tokenised connection is TLS by construction;
 * and the one supported reason to turn TLS off on the frontend
 * (`kyuubi.frontend.spark.connect.ssl.enabled=false`) is a proxy in front of Kyuubi terminating
 * TLS, which leaves the hop the client actually makes encrypted all the same.
 *
 * The server's `connectUrl` is passed through untouched. Behind a misconfigured ingress it can
 * name an unreachable in-cluster host; that is a deployment problem to see, not one to paper over
 * by guessing a host here.
 */
export function buildRemoteUrl(connectUrl: string, token: string): string {
  const trimmed = (connectUrl || '').trim()
  if (!trimmed) return ''

  const segments = trimmed.split(PARAM_SEPARATOR)
  const authority = segments[0].replace(/\/+$/, '')

  // Preserve anything the server already appended, so a future parameter does not get dropped
  // here, while still letting the two we care about win.
  const params = new Map<string, string>()
  for (const segment of segments.slice(1)) {
    const separatorIndex = segment.indexOf('=')
    if (separatorIndex > 0) {
      params.set(
        segment.slice(0, separatorIndex),
        segment.slice(separatorIndex + 1)
      )
    }
  }
  params.set(USE_SSL_PARAM, 'true')
  if (token) params.set(TOKEN_PARAM, token)

  const rendered = Array.from(params)
    .map(([key, value]) => `${key}=${value}`)
    .join(PARAM_SEPARATOR)
  return `${authority}/${PARAM_SEPARATOR}${rendered}`
}

/**
 * A PySpark snippet the user can paste as-is.
 *
 * Users copy this blindly, so it is a complete, runnable program rather than a fragment: the
 * import, the builder call and one statement that proves the session works.
 */
export function buildPySparkSnippet(connectUrl: string, token: string): string {
  const remoteUrl = buildRemoteUrl(connectUrl, token)
  return [
    'from pyspark.sql import SparkSession',
    '',
    'spark = SparkSession.builder.remote(',
    `    "${remoteUrl}"`,
    ').getOrCreate()',
    '',
    'spark.sql("SELECT 1").show()'
  ].join('\n')
}
