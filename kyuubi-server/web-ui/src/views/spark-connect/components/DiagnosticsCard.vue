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
  <el-card v-loading="loading" class="diagnostics-card">
    <template #header>
      <div class="diagnostics-head">
        <span class="nx1-section-label">{{
          $t('spark_connect.diagnostics')
        }}</span>
        <el-button icon="Refresh" text @click="load">
          {{ $t('refresh') }}
        </el-button>
      </div>
    </template>

    <p class="diagnostics-note">{{ $t('spark_connect.diagnostics_note') }}</p>

    <!-- Said once, at the top, rather than by leaving four panels to render blank. -->

    <el-alert
      v-if="driverMessage"
      class="driver-message"
      type="info"
      :closable="false"
      show-icon
      :title="noDriver ? $t('spark_connect.no_driver_yet') : ''"
      :description="driverMessage" />

    <el-tabs v-model="activeTab">
      <el-tab-pane :label="$t('spark_connect.tab_driver')" name="driver" lazy>
        <dl v-if="driver && driver.available" class="field-list">
          <dt>{{ $t('spark_connect.driver_pod') }}</dt>
          <dd>
            <code>{{ driver.podName }}</code>
          </dd>

          <dt>{{ $t('spark_connect.driver_namespace') }}</dt>
          <dd>
            <code>{{ driver.namespace }}</code>
          </dd>

          <dt>{{ $t('spark_connect.driver_node') }}</dt>
          <dd>
            <code>{{ driver.nodeName || '-' }}</code>
          </dd>

          <dt>{{ $t('spark_connect.driver_phase') }}</dt>
          <dd>
            <el-tag :type="phaseTagType" disable-transitions>
              {{ driver.phase }}
            </el-tag>
          </dd>

          <dt v-if="driver.reason">{{ $t('spark_connect.driver_reason') }}</dt>
          <dd v-if="driver.reason">{{ driver.reason }}</dd>

          <dt>{{ $t('spark_connect.driver_start_time') }}</dt>
          <dd>{{ driver.startTime || '-' }}</dd>

          <dt>{{ $t('spark_connect.driver_pod_ip') }}</dt>
          <dd>
            <code>{{ driver.podIp || '-' }}</code>
          </dd>

          <dt>{{ $t('engine_id') }}</dt>
          <dd>
            <code>{{ driver.engineId || '-' }}</code>
          </dd>
        </dl>

        <template v-if="driver && driver.containers.length > 0">
          <p v-if="restarted" class="restarted-note">
            {{ $t('spark_connect.restarted_note') }}
          </p>
          <span class="nx1-section-label">{{
            $t('spark_connect.driver_containers')
          }}</span>
          <el-table :data="driver.containers" size="small">
            <el-table-column
              prop="name"
              :label="$t('spark_connect.container_name')" />
            <el-table-column :label="$t('spark_connect.container_state')">
              <template #default="{ row }">
                {{
                  row.stateReason
                    ? `${row.state} (${row.stateReason})`
                    : row.state
                }}
              </template>
            </el-table-column>
            <el-table-column
              prop="ready"
              :label="$t('spark_connect.container_ready')" />
            <el-table-column
              prop="restartCount"
              :label="$t('spark_connect.container_restarts')" />
            <el-table-column
              :label="$t('spark_connect.container_last_termination')">
              <template #default="{ row }">
                {{ lastTermination(row) }}
              </template>
            </el-table-column>
            <el-table-column :label="$t('spark_connect.container_requests')">
              <template #default="{ row }">
                {{ resources(row.requests) }}
              </template>
            </el-table-column>
            <el-table-column :label="$t('spark_connect.container_limits')">
              <template #default="{ row }">
                {{ resources(row.limits) }}
              </template>
            </el-table-column>
          </el-table>
        </template>
      </el-tab-pane>

      <el-tab-pane :label="$t('spark_connect.tab_events')" name="events" lazy>
        <el-table
          v-if="events.length > 0"
          :data="events"
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
        <p v-else class="empty-note">{{ eventsMessage }}</p>
      </el-tab-pane>

      <el-tab-pane
        :label="$t('spark_connect.tab_driver_log')"
        name="driverLog"
        lazy>
        <pre class="log">{{ driverLogText }}</pre>
      </el-tab-pane>

      <el-tab-pane
        :label="$t('spark_connect.tab_submit_log')"
        name="submitLog"
        lazy>
        <pre class="log">{{ submitLogText }}</pre>
      </el-tab-pane>
    </el-tabs>
  </el-card>
</template>

<script lang="ts" setup>
  import { computed, ref, shallowRef, watch } from 'vue'
  import { useI18n } from 'vue-i18n'
  import { ElMessage } from 'element-plus'
  import {
    getSparkConnectDriverEvents,
    getSparkConnectDriverInfo,
    getSparkConnectDriverLog,
    getSparkConnectSubmitLog,
    type SparkConnectDriverContainer,
    type SparkConnectDriverEvent,
    type SparkConnectDriverInfo
  } from '@/api/spark-connect'

  const props = defineProps<{ sessionId: string }>()
  const { t } = useI18n()

  const PHASE_TAG_TYPES: Record<string, string> = {
    Running: 'success',
    Pending: 'warning',
    Succeeded: 'info',
    Failed: 'danger'
  }

  const loading = ref(false)
  const activeTab = ref('driver')
  const driver = shallowRef<SparkConnectDriverInfo | null>(null)
  const events = shallowRef<SparkConnectDriverEvent[]>([])
  const eventsMessage = ref('')
  const submitLogText = ref('')
  const driverLogText = ref('')

  const noDriver = computed(
    () => driver.value !== null && !driver.value.available
  )

  /** The server's own explanation, so the page never invents a reason of its own. */
  const driverMessage = computed(() =>
    noDriver.value ? driver.value?.message : null
  )

  const phaseTagType = computed(
    () => PHASE_TAG_TYPES[driver.value?.phase ?? ''] ?? 'info'
  )

  const restarted = computed(() =>
    (driver.value?.containers ?? []).some(
      (container) => container.restartCount > 0
    )
  )

  function lastTermination(container: SparkConnectDriverContainer): string {
    if (!container.lastTerminationReason) return '-'
    return container.lastTerminationExitCode === null
      ? container.lastTerminationReason
      : `${container.lastTerminationReason} (${container.lastTerminationExitCode})`
  }

  function resources(quantities: Record<string, string>): string {
    const entries = Object.entries(quantities ?? {})
    return entries.length === 0
      ? '-'
      : entries.map(([name, quantity]) => `${name}=${quantity}`).join(', ')
  }

  function eventRowClass({ row }: { row: SparkConnectDriverEvent }): string {
    return row.type === 'Warning' ? 'event-warning' : ''
  }

  function errorMessage(error: unknown): string {
    return error instanceof Error && error.message
      ? error.message
      : t('spark_connect.unknown_error')
  }

  async function load() {
    if (!props.sessionId) return
    loading.value = true
    try {
      const [driverInfo, driverEvents, submitLog, driverLog] =
        await Promise.all([
          getSparkConnectDriverInfo(props.sessionId),
          getSparkConnectDriverEvents(props.sessionId),
          getSparkConnectSubmitLog(props.sessionId),
          getSparkConnectDriverLog(props.sessionId)
        ])
      driver.value = driverInfo
      events.value = driverEvents.events ?? []
      eventsMessage.value = driverEvents.message ?? t('spark_connect.empty_log')
      submitLogText.value =
        (submitLog.logRowSet ?? []).join('\n') || t('spark_connect.empty_log')
      driverLogText.value =
        (driverLog.logRowSet ?? []).join('\n') || t('spark_connect.empty_log')
    } catch (error) {
      ElMessage.error(
        t('spark_connect.load_diagnostics_failed', {
          message: errorMessage(error)
        })
      )
    } finally {
      loading.value = false
    }
  }

  watch(() => props.sessionId, load, { immediate: true })

  defineExpose({ load })
</script>

<style lang="scss" scoped>
  .diagnostics-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
  }
  .diagnostics-note,
  .empty-note,
  .restarted-note {
    font-size: 12px;
    color: var(--el-text-color-secondary);
  }
  .diagnostics-note {
    margin: 0 0 12px;
  }
  .driver-message {
    margin-bottom: 12px;
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
  .log {
    margin: 0;
    padding: 12px 14px;
    border-radius: 6px;
    background: var(--el-fill-color-light);
    font-family: var(--nx1-font-mono, monospace);
    font-size: 12px;
    line-height: 1.6;
    max-height: 420px;
    overflow: auto;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
  }
  :deep(.event-warning) {
    color: var(--el-color-danger);
  }
</style>
