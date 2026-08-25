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
  <el-card class="sessions-card">
    <template #header>
      <div class="sessions-head">
        <span class="nx1-section-label">{{
          $t('spark_connect.your_sessions')
        }}</span>
        <el-button icon="Refresh" text @click="emit('refresh')">
          {{ $t('refresh') }}
        </el-button>
      </div>
    </template>

    <el-table
      v-loading="loading"
      :data="sessions"
      style="width: 100%"
      :empty-text="$t('spark_connect.no_sessions')">
      <el-table-column
        prop="sessionId"
        :label="$t('session_id')"
        min-width="30%" />
      <el-table-column :label="$t('state')" min-width="15%">
        <template #default="scope">
          <el-tag :type="stateTagType(scope.row.state)" disable-transitions>
            {{ scope.row.state }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="user" :label="$t('user')" min-width="15%" />
      <el-table-column :label="$t('engine_id')" min-width="20%">
        <template #default="scope">
          {{ scope.row.engineId || '-' }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('create_time')" min-width="20%">
        <template #default="scope">
          {{ formatCreateTime(scope.row.createTime) }}
        </template>
      </el-table-column>
      <el-table-column fixed="right" :label="$t('operation.text')" width="100">
        <template #default="scope">
          <el-popconfirm
            :title="$t('spark_connect.close_confirm')"
            @confirm="emit('close', scope.row.sessionId)">
            <template #reference>
              <span>
                <el-tooltip
                  effect="dark"
                  :content="$t('operation.close')"
                  placement="top">
                  <template #default>
                    <el-button type="danger" icon="Delete" circle />
                  </template>
                </el-tooltip>
              </span>
            </template>
          </el-popconfirm>
        </template>
      </el-table-column>
    </el-table>
  </el-card>
</template>

<script lang="ts" setup>
  import { format } from 'date-fns'
  import type { SparkConnectSessionData } from '@/api/spark-connect'

  defineProps<{
    sessions: SparkConnectSessionData[]
    loading: boolean
  }>()
  const emit = defineEmits<{
    refresh: []
    close: [sessionId: string]
  }>()

  const STATE_TAG_TYPES: Record<string, string> = {
    RUNNING: 'success',
    PENDING: 'warning',
    FAILED: 'danger',
    CLOSED: 'info'
  }

  function stateTagType(state: string): string {
    return STATE_TAG_TYPES[state] ?? 'info'
  }

  function formatCreateTime(createTime: number | null | undefined): string {
    if (createTime == null || createTime <= 0) return '-'
    return format(createTime, 'yyyy-MM-dd HH:mm:ss')
  }
</script>

<style lang="scss" scoped>
  .sessions-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
  }
</style>
