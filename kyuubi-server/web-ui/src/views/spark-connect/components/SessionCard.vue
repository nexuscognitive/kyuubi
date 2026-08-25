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
  <el-card class="session-card">
    <template #header>
      <div class="session-head">
        <span class="nx1-section-label">{{
          $t('spark_connect.your_session')
        }}</span>
        <div class="session-actions">
          <el-button icon="Refresh" text @click="emit('refresh')">
            {{ $t('refresh') }}
          </el-button>
          <el-popconfirm
            :title="$t('spark_connect.close_confirm')"
            @confirm="emit('close', session.sessionId)">
            <template #reference>
              <el-button type="danger" text>
                {{ $t('operation.close') }}
              </el-button>
            </template>
          </el-popconfirm>
        </div>
      </div>
    </template>

    <dl class="field-list">
      <dt>{{ $t('state') }}</dt>
      <dd>
        <el-tag :type="stateTagType" disable-transitions>
          {{ session.state }}
        </el-tag>
        <span v-if="session.state === 'PENDING'" class="state-note">
          {{ $t('spark_connect.pending_note') }}
        </span>
      </dd>

      <dt>{{ $t('session_id') }}</dt>
      <dd>
        <code class="field-value">{{ session.sessionId }}</code>
      </dd>

      <dt>{{ $t('engine_id') }}</dt>
      <dd>
        <code class="field-value">{{ session.engineId || '-' }}</code>
      </dd>

      <dt>{{ $t('create_time') }}</dt>
      <dd>{{ formattedCreateTime }}</dd>

      <dt>{{ $t('spark_connect.connect_url') }}</dt>
      <dd>
        <code class="field-value">{{ session.connectUrl }}</code>
        <el-button size="small" @click="emit('copy', session.connectUrl)">
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
    <p class="snippet-note">{{ $t('spark_connect.credential_note') }}</p>
    <pre class="snippet">{{ snippet }}</pre>
  </el-card>
</template>

<script lang="ts" setup>
  import { computed } from 'vue'
  import { format } from 'date-fns'
  import type { SparkConnectSessionData } from '@/api/spark-connect'
  import { buildPySparkSnippet } from '../utils/snippet'

  const props = defineProps<{ session: SparkConnectSessionData }>()
  const emit = defineEmits<{
    copy: [text: string]
    refresh: []
    close: [sessionId: string]
  }>()

  const STATE_TAG_TYPES: Record<string, string> = {
    RUNNING: 'success',
    PENDING: 'warning',
    FAILED: 'danger',
    CLOSED: 'info'
  }

  const stateTagType = computed(
    () => STATE_TAG_TYPES[props.session.state] ?? 'info'
  )

  const formattedCreateTime = computed(() => {
    const createTime = props.session.createTime
    if (createTime == null || createTime <= 0) return '-'
    return format(createTime, 'yyyy-MM-dd HH:mm:ss')
  })

  const snippet = computed(() => buildPySparkSnippet(props.session.connectUrl))
</script>

<style lang="scss" scoped>
  .session-head,
  .snippet-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
  }
  .session-actions {
    display: flex;
    align-items: center;
    gap: 4px;
  }
  .snippet-head {
    margin: 20px 0 8px;
  }
  .field-list {
    margin: 0;
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
    overflow-wrap: anywhere;
    min-width: 0;
  }
  .state-note,
  .url-note,
  .snippet-note {
    font-size: 12px;
    color: var(--el-text-color-secondary);
  }
  .url-note {
    margin: 12px 0 0;
  }
  .snippet-note {
    margin: 0 0 8px;
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
