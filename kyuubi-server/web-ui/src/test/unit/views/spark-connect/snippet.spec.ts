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

import { describe, it, expect } from 'vitest'
import {
  buildRemoteUrl,
  buildPySparkSnippet,
  CREDENTIAL_ENV_VAR
} from '@/views/spark-connect/utils/snippet'

const TOKEN = 'Zm9vYmFyLWJhei1xdXV4'

describe('buildRemoteUrl', () => {
  it('produces the exact connection string Spark Connect expects', () => {
    expect(buildRemoteUrl('sc://kyuubi.example.com:15002', TOKEN)).toBe(
      `sc://kyuubi.example.com:15002/;use_ssl=true;token=${TOKEN}`
    )
  })

  it('keeps exactly one slash when the server already sent a trailing one', () => {
    expect(buildRemoteUrl('sc://host:15002/', TOKEN)).toBe(
      `sc://host:15002/;use_ssl=true;token=${TOKEN}`
    )
    expect(buildRemoteUrl('sc://host:15002//', TOKEN)).toBe(
      `sc://host:15002/;use_ssl=true;token=${TOKEN}`
    )
  })

  it('trims surrounding whitespace', () => {
    expect(buildRemoteUrl('  sc://host:15002  ', TOKEN)).toBe(
      `sc://host:15002/;use_ssl=true;token=${TOKEN}`
    )
  })

  it('always asks for TLS, even if the server said otherwise', () => {
    expect(buildRemoteUrl('sc://host:15002/;use_ssl=false', TOKEN)).toBe(
      `sc://host:15002/;use_ssl=true;token=${TOKEN}`
    )
  })

  it('preserves other parameters the server appended', () => {
    expect(buildRemoteUrl('sc://host:15002/;user_id=alice', TOKEN)).toBe(
      `sc://host:15002/;user_id=alice;use_ssl=true;token=${TOKEN}`
    )
  })

  it('replaces a token the server already put in the URL', () => {
    expect(buildRemoteUrl('sc://host:15002/;token=stale', TOKEN)).toBe(
      `sc://host:15002/;token=${TOKEN};use_ssl=true`
    )
  })

  it('passes an IP-and-port host through untouched', () => {
    // The advertised host can be an in-cluster address. Surfacing it is the point; rewriting it
    // would hide a deployment misconfiguration.
    expect(buildRemoteUrl('sc://10.42.0.7:15002', TOKEN)).toBe(
      `sc://10.42.0.7:15002/;use_ssl=true;token=${TOKEN}`
    )
  })

  it('omits the token parameter when there is no token', () => {
    expect(buildRemoteUrl('sc://host:15002', '')).toBe(
      'sc://host:15002/;use_ssl=true'
    )
  })

  it('returns an empty string for an absent connect URL', () => {
    expect(buildRemoteUrl('', TOKEN)).toBe('')
    expect(buildRemoteUrl('   ', TOKEN)).toBe('')
  })
})

describe('buildPySparkSnippet', () => {
  it('is a complete, runnable program the user only has to supply a credential to', () => {
    expect(buildPySparkSnippet('sc://host:15002')).toBe(
      [
        'import os',
        '',
        'from pyspark.sql import SparkSession',
        '',
        '# The same credential you use for the Kyuubi REST API.',
        `token = os.environ["${CREDENTIAL_ENV_VAR}"]`,
        '',
        'spark = SparkSession.builder.remote(',
        '    f"sc://host:15002/;use_ssl=true;token={token}"',
        ').getOrCreate()',
        '',
        'spark.sql("SELECT 1").show()'
      ].join('\n')
    )
  })

  it('interpolates the credential rather than embedding one', () => {
    // The server issues no Spark Connect token, so there is nothing to bake in here. An f-string
    // keeps the credential in the user's environment and out of anything they paste around.
    const snippet = buildPySparkSnippet('sc://host:15002')

    expect(snippet).toContain('.remote(\n    f"')
    expect(snippet).toContain('token={token}')
    expect(snippet).not.toMatch(/token=[A-Za-z0-9_-]{20,}/)
  })

  it('quotes the remote URL so the shell-unfriendly semicolons survive a paste', () => {
    expect(buildPySparkSnippet('sc://host:15002')).toContain(
      '.remote(\n    f"sc://host:15002/;use_ssl=true;token={token}"\n)'
    )
  })
})
