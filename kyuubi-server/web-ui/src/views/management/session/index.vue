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
  <el-card>
    <summary-bar :items="summary" />
    <el-input
      v-model="searchText"
      :placeholder="$t('search')"
      clearable
      class="search-input"
      @input="handleSearch" />
    <el-table
      v-loading="loading"
      :data="tableData"
      max-height="500px"
      style="width: 100%"
      @sort-change="handleSortChange">
      <el-table-column
        prop="user"
        :label="$t('user')"
        width="160px"
        sortable="custom" />
      <!-- TODO need jump to engine page -->
      <el-table-column
        prop="engineId"
        :label="$t('engine_id')"
        width="160px"
        sortable="custom" />
      <el-table-column
        prop="engineUrl"
        :label="$t('driver_pod')"
        width="260px"
        sortable="custom">
        <template #default="scope">
          {{ driverPod(scope.row.engineUrl) }}
        </template>
      </el-table-column>
      <el-table-column prop="ipAddr" :label="$t('client_ip')" width="160px" />
      <el-table-column
        prop="kyuubiInstance"
        :label="$t('kyuubi_instance')"
        width="180px"
        sortable="custom" />
      <el-table-column
        prop="sessionType"
        :label="$t('session_type')"
        width="120px"
        sortable="custom" />
      <el-table-column :label="$t('status')" width="130px">
        <template #default="scope">
          <el-tooltip
            v-if="isOwnerDown(scope.row)"
            effect="dark"
            :content="$t('owner_down_hint')"
            placement="top">
            <el-tag type="warning">{{ $t('reconnecting') }}</el-tag>
          </el-tooltip>
          <el-tooltip
            v-else-if="scope.row.exception"
            effect="dark"
            :content="scope.row.exception"
            placement="top">
            <el-tag type="danger">ERROR</el-tag>
          </el-tooltip>
          <el-tag
            v-else
            :type="sessionStatus(scope.row) === 'ACTIVE' ? 'success' : 'info'">
            {{ sessionStatus(scope.row) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column :label="$t('driver_state')" width="180px">
        <template #default="scope">
          <el-tag
            v-if="
              scope.row.sessionType === 'BATCH' && driverPodState(scope.row)
            "
            :type="driverStateType(driverPodState(scope.row))"
            size="small">
            {{ driverPodState(scope.row) }}
          </el-tag>
          <span v-else>-</span>
        </template>
      </el-table-column>
      <!-- TODO need jump to session page -->
      <el-table-column
        prop="identifier"
        :label="$t('session_id')"
        width="300px"
        sortable="custom">
        <template #default="scope">
          <el-link @click="handleSessionDetailJump(scope.row.identifier)">{{
            scope.row.identifier
          }}</el-link>
        </template>
      </el-table-column>
      <el-table-column
        prop="createTime"
        :label="$t('create_time')"
        width="200"
        sortable="custom">
        <template #default="scope">
          {{
            scope.row.createTime != null && scope.row.createTime > -1
              ? format(scope.row.createTime, 'yyyy-MM-dd HH:mm:ss')
              : '-'
          }}
        </template>
      </el-table-column>
      <el-table-column fixed="right" :label="$t('operation.text')" width="160">
        <template #default="scope">
          <el-space wrap>
            <el-tooltip
              v-if="scope.row.sessionType === 'BATCH'"
              effect="dark"
              :content="$t('driver_log')"
              placement="top">
              <el-button
                type="primary"
                icon="Document"
                circle
                @click="openDriverLog(scope.row)" />
            </el-tooltip>
            <el-tooltip
              v-if="scope.row.sessionType === 'BATCH'"
              effect="dark"
              :content="$t('driver_events')"
              placement="top">
              <el-button
                type="warning"
                icon="Bell"
                circle
                @click="openDriverEvents(scope.row)" />
            </el-tooltip>
            <el-tooltip
              v-if="isOwnerDown(scope.row)"
              effect="dark"
              :content="$t('owner_down_hint')"
              placement="top">
              <el-button type="info" icon="Delete" circle disabled />
            </el-tooltip>
            <el-popconfirm
              v-else
              :title="$t('operation.delete_confirm')"
              @confirm="
                handleDeleteSession(
                  scope.row.identifier,
                  scope.row.kyuubiInstance
                )
              ">
              <template #reference>
                <span>
                  <el-tooltip
                    effect="dark"
                    :content="$t('operation.delete')"
                    placement="top">
                    <template #default>
                      <el-button type="danger" icon="Delete" circle />
                    </template>
                  </el-tooltip>
                </span>
              </template>
            </el-popconfirm>
          </el-space>
        </template>
      </el-table-column>
    </el-table>
    <el-pagination
      v-model:current-page="currentPage"
      v-model:page-size="pageSize"
      :page-sizes="[10, 20, 50, 100]"
      :total="totalPage"
      layout="total, sizes, prev, pager, next, jumper"
      class="pagination"
      @size-change="handleSizeChange"
      @current-change="handleCurrentChange" />
  </el-card>

  <el-dialog
    v-model="driverLogVisible"
    :title="
      $t('driver_log') + (driverLogBatchId ? ': ' + driverLogBatchId : '')
    "
    width="72%"
    top="6vh">
    <div class="driver-log-toolbar">
      <el-button
        size="small"
        icon="Refresh"
        :loading="driverLogLoading"
        @click="refreshDriverLog">
        {{ $t('refresh') }}
      </el-button>
    </div>
    <pre v-loading="driverLogLoading" class="driver-log">{{
      driverLogText
    }}</pre>
  </el-dialog>

  <el-dialog
    v-model="driverEventsVisible"
    :title="
      $t('driver_events') +
      (driverEventsBatchId ? ': ' + driverEventsBatchId : '')
    "
    width="72%"
    top="6vh">
    <div class="driver-log-toolbar">
      <el-button
        size="small"
        icon="Refresh"
        :loading="driverEventsLoading"
        @click="refreshDriverEvents">
        {{ $t('refresh') }}
      </el-button>
    </div>
    <pre v-loading="driverEventsLoading" class="driver-log">{{
      driverEventsText
    }}</pre>
  </el-dialog>
</template>

<script lang="ts" setup>
  import { computed, ref } from 'vue'
  import { format } from 'date-fns'
  import {
    getAllSessions,
    deleteSession,
    getBatchDriverLog,
    getBatchDriverPodEvents
  } from '@/api/session'
  import { ElMessage } from 'element-plus'
  import { useI18n } from 'vue-i18n'
  import { useTable } from '@/utils/use-table'
  import SummaryBar, { SummaryItem } from '@/components/summary-bar/index.vue'
  import { Router, useRouter } from 'vue-router'
  const { t } = useI18n()
  const {
    list,
    tableData,
    loading,
    currentPage,
    pageSize,
    totalPage,
    searchText,
    handleSizeChange,
    handleCurrentChange,
    handleSortChange,
    handleSearch,
    getList: _getList
  } = useTable()
  // Derive the Spark driver pod name from the engine URL, e.g.
  // http://sample-2e49-driver-svc.rapid.svc:4040 -> sample-2e49-driver
  // (the driver-svc's first DNS label with the '-svc' suffix stripped).
  const driverPod = (engineUrl?: string): string => {
    if (!engineUrl) return '-'
    try {
      const label = new URL(engineUrl).hostname.split('.')[0]
      return label.replace(/-svc$/, '')
    } catch {
      return engineUrl
    }
  }
  // Coarse, derived connection-level status (no cross-instance join needed):
  // recently-active session with live operations => ACTIVE, otherwise IDLE.
  const sessionStatus = (row: {
    totalOperations?: number
    idleTime?: number
  }): string => {
    if (
      (row.totalOperations ?? 0) > 0 &&
      row.idleTime != null &&
      row.idleTime < 60000
    ) {
      return 'ACTIVE'
    }
    return 'IDLE'
  }
  // A batch merged in from the metadata store whose owning instance is currently unreachable
  // (dead/restarting pod). The job keeps running; it will reattach when the owner returns or a
  // peer takes it over.
  const isOwnerDown = (row: { conf?: Record<string, string> }): boolean =>
    row?.conf?.['kyuubi.session.owner.reachable'] === 'false'
  // Current Spark driver pod state for a batch row, surfaced by the backend via conf.
  const driverPodState = (row: { conf?: Record<string, string> }): string =>
    row?.conf?.['kyuubi.driver.pod.state'] || ''
  const driverStateType = (state: string): string => {
    if (state.startsWith('Running')) return 'success'
    if (state.startsWith('Failed')) return 'danger'
    if (state.startsWith('Pending')) return 'warning'
    return 'info' // Succeeded / Unknown / other
  }
  // Counts across the FULL result set (not just the current page). Owner-down rows are counted
  // separately; otherwise a session with an exception is ERROR, else ACTIVE or IDLE.
  const summary = computed(() => {
    const rows = list.value
    const ownerDown = rows.filter(isOwnerDown).length
    const error = rows.filter((r) => !isOwnerDown(r) && !!r.exception).length
    const active = rows.filter(
      (r) => !isOwnerDown(r) && !r.exception && sessionStatus(r) === 'ACTIVE'
    ).length
    const idle = rows.filter(
      (r) => !isOwnerDown(r) && !r.exception && sessionStatus(r) === 'IDLE'
    ).length
    const items: SummaryItem[] = [
      { label: t('summary.total'), value: rows.length, type: 'default' },
      { label: t('summary.active'), value: active, type: 'success' },
      { label: t('summary.idle'), value: idle, type: 'info' },
      { label: t('summary.error'), value: error, type: 'danger' }
    ]
    if (ownerDown > 0) {
      items.push({
        label: t('summary.reconnecting'),
        value: ownerDown,
        type: 'warning'
      })
    }
    return items
  })
  const handleDeleteSession = (sessionId: string, kyuubiInstance?: string) => {
    deleteSession(sessionId, kyuubiInstance)
      .then(() => {
        // need add delete success or failed logic after api support
        ElMessage({
          message: t('message.delete_succeeded', { name: 'session' }),
          type: 'success'
        })
      })
      .catch(() => {
        ElMessage({
          message: t('message.delete_failed', { name: 'session' }),
          type: 'error'
        })
      })
      .finally(() => {
        getList()
      })
  }
  const getList = () => {
    _getList(getAllSessions)
  }
  const router: Router = useRouter()

  // Spark driver pod log viewer (batch sessions). The batch id is the session identifier;
  // the endpoint reads the driver pod directly, so it works from any instance.
  const driverLogVisible = ref(false)
  const driverLogLoading = ref(false)
  const driverLogText = ref('')
  const driverLogBatchId = ref('')
  const openDriverLog = (row: { identifier: string }) => {
    driverLogBatchId.value = row.identifier
    driverLogVisible.value = true
    refreshDriverLog()
  }
  const refreshDriverLog = () => {
    if (!driverLogBatchId.value) return
    driverLogLoading.value = true
    getBatchDriverLog(driverLogBatchId.value, 500)
      .then((res: any) => {
        const lines = res?.logRowSet || []
        driverLogText.value = lines.length ? lines.join('\n') : t('no_log')
      })
      .catch(() => {
        driverLogText.value = t('no_log')
      })
      .finally(() => {
        driverLogLoading.value = false
      })
  }

  // Spark driver pod Kubernetes events viewer (batch sessions). Same direct-from-K8s model as the
  // driver log, so it works from any instance while the driver pod exists.
  const driverEventsVisible = ref(false)
  const driverEventsLoading = ref(false)
  const driverEventsText = ref('')
  const driverEventsBatchId = ref('')
  const openDriverEvents = (row: { identifier: string }) => {
    driverEventsBatchId.value = row.identifier
    driverEventsVisible.value = true
    refreshDriverEvents()
  }
  const refreshDriverEvents = () => {
    if (!driverEventsBatchId.value) return
    driverEventsLoading.value = true
    getBatchDriverPodEvents(driverEventsBatchId.value, 500)
      .then((res: any) => {
        const lines = res?.logRowSet || []
        driverEventsText.value = lines.length
          ? lines.join('\n')
          : t('no_events')
      })
      .catch(() => {
        driverEventsText.value = t('no_events')
      })
      .finally(() => {
        driverEventsLoading.value = false
      })
  }

  function handleSessionDetailJump(sessionId: string) {
    router.push({
      path: '/detail/session',
      query: {
        sessionId
      }
    })
  }
  getList()
</script>

<style lang="scss" scoped>
  .search-input {
    width: 260px;
    margin-bottom: 12px;
  }
  .pagination {
    margin-top: 12px;
    display: flex;
    justify-content: flex-end;
  }
  .driver-log-toolbar {
    margin-bottom: 8px;
  }
  .driver-log {
    margin: 0;
    max-height: 62vh;
    overflow: auto;
    padding: 12px;
    background: var(--el-fill-color-light);
    border-radius: 6px;
    font-family: monospace;
    font-size: 12px;
    line-height: 1.5;
    white-space: pre-wrap;
    word-break: break-all;
  }
</style>
