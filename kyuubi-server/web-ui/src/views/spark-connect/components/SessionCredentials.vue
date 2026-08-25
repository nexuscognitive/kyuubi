<!--
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
-->

<template>
  <el-card class="credentials-card">
    <template #header>
      <div class="credentials-head">
        <span class="nx1-section-label">{{
          $t('spark_connect.credentials')
        }}</span>
        <el-button text @click="emit('dismiss')">
          {{ $t('spark_connect.dismiss') }}
        </el-button>
      </div>
    </template>

    <el-alert
      type="warning"
      :closable="false"
      show-icon
      :title="$t('spark_connect.token_warning_title')"
      :description="$t('spark_connect.token_warning_body')" />

    <dl class="field-list">
      <dt>{{ $t('session_id') }}</dt>
      <dd>
        <code class="field-value">{{ session.sessionId }}</code>
      </dd>

      <dt>{{ $t('spark_connect.connect_url') }}</dt>
      <dd>
        <code class="field-value">{{ session.connectUrl }}</code>
        <el-button size="small" @click="emit('copy', session.connectUrl)">
          {{ $t('spark_connect.copy') }}
        </el-button>
      </dd>

      <dt>{{ $t('spark_connect.token') }}</dt>
      <dd>
        <code class="field-value token-value">{{ session.token }}</code>
        <el-button
          size="small"
          type="primary"
          @click="emit('copy', session.token)">
          {{ $t('spark_connect.copy') }}
        </el-button>
      </dd>
    </dl>

    <p class="url-note">{{ $t('spark_connect.connect_url_note') }}</p>

    <div class="snippet-head">
      <span class="nx1-section-label">{{ $t('spark_connect.snippet') }}</span>
      <el-button size="small" @click="emit('copy', snippet)">
        {{ $t('spark_connect.copy_snippet') }}
      </el-button>
    </div>
    <pre class="snippet">{{ snippet }}</pre>
  </el-card>
</template>

<script lang="ts" setup>
  import { computed } from 'vue'
  import type { SparkConnectSession } from '@/api/spark-connect'
  import { buildPySparkSnippet } from '../utils/snippet'

  const props = defineProps<{ session: SparkConnectSession }>()
  const emit = defineEmits<{
    copy: [text: string]
    dismiss: []
  }>()

  const snippet = computed(() =>
    buildPySparkSnippet(props.session.connectUrl, props.session.token)
  )
</script>

<style lang="scss" scoped>
  .credentials-head,
  .snippet-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
  }
  .snippet-head {
    margin: 20px 0 8px;
  }
  .field-list {
    margin: 16px 0 0;
    display: grid;
    grid-template-columns: minmax(120px, max-content) 1fr;
    gap: 8px 16px;
    align-items: center;

    dt {
      font-size: 12px;
      font-weight: 600;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--el-text-color-secondary);
    }
    dd {
      margin: 0;
      display: flex;
      align-items: center;
      gap: 8px;
      min-width: 0;
    }
  }
  .field-value {
    font-family: var(--nx1-font-mono, monospace);
    font-size: 13px;
    // Tokens are long and must stay selectable, so they wrap rather than truncate.
    overflow-wrap: anywhere;
    min-width: 0;
  }
  .token-value {
    color: var(--el-color-danger);
  }
  .url-note {
    margin: 12px 0 0;
    font-size: 12px;
    color: var(--el-text-color-secondary);
  }
  .snippet {
    margin: 0;
    padding: 12px 14px;
    border-radius: 6px;
    background: var(--el-fill-color-light);
    font-family: var(--nx1-font-mono, monospace);
    font-size: 13px;
    line-height: 1.6;
    overflow-x: auto;
  }
</style>
