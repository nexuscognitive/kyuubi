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
  <el-card class="recovery-card">
    <template #header>
      <span class="nx1-section-label">{{ $t('spark_connect.recovery') }}</span>
    </template>

    <!--
      Said before the numbers rather than after them: an operator reading a session that is never
      coming back needs the sentence that says so, not a restart count to infer it from.
    -->
    <el-alert
      v-if="session.recoveryMessage"
      class="recovery-alert"
      type="error"
      :closable="false"
      show-icon
      :title="$t('spark_connect.recovery_abandoned')"
      :description="session.recoveryMessage" />

    <el-alert
      v-else-if="session.stateLossWarning"
      class="recovery-alert"
      type="warning"
      :closable="false"
      show-icon
      :title="$t('spark_connect.state_loss_warning')"
      :description="session.stateLossWarning" />

    <dl class="field-list">
      <dt>{{ $t('spark_connect.restart_count') }}</dt>
      <dd>{{ session.restartCount }}</dd>

      <dt>{{ $t('spark_connect.generation') }}</dt>
      <dd>{{ session.generation }}</dd>

      <dt>{{ $t('spark_connect.last_restart') }}</dt>
      <dd>
        {{ formattedLastRestart ?? $t('spark_connect.never_restarted') }}
      </dd>
    </dl>

    <p class="recovery-note">{{ $t('spark_connect.recovery_note') }}</p>

    <div class="post-mortem-head">
      <span class="nx1-section-label">
        {{ $t('spark_connect.post_mortems') }}
      </span>
    </div>
    <p class="recovery-note">{{ $t('spark_connect.post_mortems_note') }}</p>

    <p v-if="postMortems.length === 0" class="recovery-note">
      {{ $t('spark_connect.no_post_mortems') }}
    </p>

    <!-- Newest first, because the most recent death is the one explaining the current state. -->
    <el-collapse v-else v-model="expanded">
      <el-collapse-item
        v-for="(postMortem, index) in postMortems"
        :key="`${postMortem.driverName}-${postMortem.capturedTime}`"
        :name="String(index)">
        <template #title>
          <span class="post-mortem-title">
            <el-tag
              :type="postMortem.oomKilled ? 'danger' : 'warning'"
              size="small"
              disable-transitions>
              {{
                postMortem.oomKilled
                  ? $t('spark_connect.post_mortem_oom')
                  : postMortem.finalState
              }}
            </el-tag>
            <code>{{ postMortem.driverName }}</code>
            <span class="post-mortem-summary">{{ postMortem.summary }}</span>
            <span class="post-mortem-when">
              {{ formatTimestamp(postMortem.capturedTime) }}
            </span>
          </span>
        </template>

        <dl class="field-list">
          <dt>{{ $t('spark_connect.post_mortem_when') }}</dt>
          <dd>{{ formatTimestamp(postMortem.capturedTime) }}</dd>

          <dt>{{ $t('spark_connect.post_mortem_driver') }}</dt>
          <dd>
            <code>{{ postMortem.driverName }}</code>
          </dd>

          <dt>{{ $t('spark_connect.driver_namespace') }}</dt>
          <dd>
            <code>{{ postMortem.location }}</code>
          </dd>

          <!--
            Both states, because they can disagree: the cluster's own phase is what happened, and
            Kyuubi's derived state is what Kyuubi acted on.
          -->
          <dt>{{ $t('spark_connect.post_mortem_state') }}</dt>
          <dd>{{ finalAndDerivedState(postMortem) }}</dd>

          <dt>{{ $t('spark_connect.post_mortem_cause') }}</dt>
          <dd>{{ postMortem.summary }}</dd>

          <template v-if="postMortem.message">
            <dt>{{ $t('spark_connect.event_message') }}</dt>
            <dd>{{ postMortem.message }}</dd>
          </template>
        </dl>

        <template v-if="postMortem.containers.length > 0">
          <span class="nx1-section-label">
            {{ $t('spark_connect.post_mortem_containers') }}
          </span>
          <el-table :data="postMortem.containers" size="small">
            <el-table-column
              prop="name"
              :label="$t('spark_connect.container_name')" />
            <el-table-column
              prop="reason"
              :label="$t('spark_connect.driver_reason')" />
            <el-table-column
              prop="exitCode"
              :label="$t('spark_connect.container_exit_code')"
              width="110" />
            <el-table-column
              prop="signal"
              :label="$t('spark_connect.container_signal')"
              width="90" />
            <el-table-column
              prop="restartCount"
              :label="$t('spark_connect.container_restarts')"
              width="100" />
            <el-table-column
              prop="finishedAt"
              :label="$t('spark_connect.container_finished_at')" />
          </el-table>
        </template>

        <template v-if="postMortem.events.length > 0">
          <span class="nx1-section-label">
            {{ $t('spark_connect.post_mortem_events') }}
          </span>
          <el-table
            :data="postMortem.events"
            size="small"
            :row-class-name="eventRowClass">
            <el-table-column
              prop="type"
              :label="$t('spark_connect.event_type')"
              width="100" />
            <el-table-column
              prop="reason"
              :label="$t('spark_connect.event_reason')"
              width="180" />
            <el-table-column
              prop="count"
              :label="$t('spark_connect.event_count')"
              width="80" />
            <el-table-column
              prop="lastTimestamp"
              :label="$t('spark_connect.event_last_seen')"
              width="200" />
            <el-table-column
              prop="message"
              :label="$t('spark_connect.event_message')" />
          </el-table>
        </template>
      </el-collapse-item>
    </el-collapse>
  </el-card>
</template>

<script lang="ts" setup>
  import { computed, ref } from 'vue'
  import { format } from 'date-fns'
  import type {
    SparkConnectDriverEvent,
    SparkConnectDriverPostMortem,
    SparkConnectSessionData
  } from '@/api/spark-connect'

  const props = defineProps<{ session: SparkConnectSessionData }>()

  // The newest death is expanded by default: it is the one that explains the state on screen,
  // and an operator should not have to click to reach it.
  const expanded = ref<string[]>(['0'])

  const postMortems = computed(() => props.session.driverPostMortems ?? [])

  const formattedLastRestart = computed(() =>
    props.session.lastRestartTime > 0
      ? formatTimestamp(props.session.lastRestartTime)
      : null
  )

  function formatTimestamp(timestamp: number): string {
    return format(timestamp, 'yyyy-MM-dd HH:mm:ss')
  }

  function finalAndDerivedState(
    postMortem: SparkConnectDriverPostMortem
  ): string {
    return `${postMortem.finalState} / ${postMortem.applicationState}`
  }

  function eventRowClass({ row }: { row: SparkConnectDriverEvent }): string {
    return row.type === 'Warning' ? 'event-warning' : ''
  }
</script>

<style lang="scss" scoped>
  .recovery-alert {
    margin-bottom: 12px;
  }
  .recovery-note {
    margin: 8px 0 12px;
    font-size: 12px;
    color: var(--el-text-color-secondary);
  }
  .post-mortem-head {
    margin-top: 20px;
  }
  .post-mortem-title {
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
    flex-wrap: wrap;
  }
  .post-mortem-summary {
    font-weight: 600;
  }
  .post-mortem-when {
    color: var(--el-text-color-secondary);
    font-size: 12px;
  }
  .field-list {
    margin: 0 0 16px;
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
      min-width: 0;
      overflow-wrap: anywhere;
    }
  }
  :deep(.event-warning) {
    color: var(--el-color-danger);
  }
</style>
