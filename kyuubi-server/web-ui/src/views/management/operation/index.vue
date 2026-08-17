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
  <el-card class="table-container">
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
      style="width: 100%"
      @sort-change="handleSortChange">
      <el-table-column
        prop="sessionUser"
        :label="$t('user')"
        width="160"
        sortable="custom" />
      <el-table-column
        prop="identifier"
        :label="$t('operation_id')"
        width="300"
        sortable="custom" />
      <el-table-column prop="statement" :label="$t('statement')" width="160" />
      <el-table-column
        prop="state"
        :label="$t('state')"
        width="160"
        sortable="custom" />
      <el-table-column
        prop="kyuubiInstance"
        :label="$t('kyuubi_instance')"
        width="180"
        sortable="custom" />
      <el-table-column
        prop="startTime"
        :label="$t('start_time')"
        width="160"
        sortable="custom">
        <template #default="scope">
          {{
            scope.row.startTime != null && scope.row.startTime > 0
              ? format(scope.row.startTime, 'yyyy-MM-dd HH:mm:ss')
              : '-'
          }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('complete_time')" width="160">
        <template #default="scope">
          {{
            scope.row.completeTime != null && scope.row.completeTime > 0
              ? format(scope.row.completeTime, 'yyyy-MM-dd HH:mm:ss')
              : '-'
          }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('duration')" width="140">
        <template #default="scope">{{
          scope.row.startTime != null &&
          scope.row.completeTime != null &&
          scope.row.startTime > 0 &&
          scope.row.completeTime > 0
            ? millTransfer(scope.row.completeTime - scope.row.startTime)
            : '-'
        }}</template>
      </el-table-column>
      <el-table-column fixed="right" :label="$t('operation.text')" width="110">
        <template #default="scope">
          <el-space wrap>
            <el-popconfirm
              v-if="!isTerminalState(scope.row.state)"
              :title="$t('operation.cancel_confirm')"
              @confirm="
                handleOperate(
                  scope.row.identifier,
                  scope.row.kyuubiInstance,
                  'CANCEL'
                )
              ">
              <template #reference>
                <span>
                  <el-tooltip
                    effect="dark"
                    :content="$t('operation.cancel')"
                    placement="top">
                    <template #default>
                      <el-button type="danger" icon="Remove" circle />
                    </template>
                  </el-tooltip>
                </span>
              </template>
            </el-popconfirm>
            <el-popconfirm
              :title="$t('operation.close_confirm')"
              @confirm="
                handleOperate(
                  scope.row.identifier,
                  scope.row.kyuubiInstance,
                  'CLOSE'
                )
              ">
              <template #reference>
                <span>
                  <el-tooltip
                    effect="dark"
                    :content="$t('operation.close')"
                    placement="top">
                    <template #default>
                      <el-button type="danger" icon="CircleClose" circle />
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
</template>

<script lang="ts" setup>
  import { computed } from 'vue'
  import { getAllOperations, deleteOperation } from '@/api/operation'
  import { millTransfer } from '@/utils/unit'
  import { format } from 'date-fns'
  import { useI18n } from 'vue-i18n'
  import { ElMessage } from 'element-plus'
  import { useTable } from '@/utils/use-table'
  import SummaryBar from '@/components/summary-bar/index.vue'

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
  // Route the close to the owning instance (admin endpoint). Closing a RUNNING
  // operation cancels the query; this is the cluster-wide "cancel any operation".
  const handleOperate = (
    operationId: string,
    kyuubiInstance: string,
    action: 'CANCEL' | 'CLOSE'
  ) => {
    deleteOperation(operationId, kyuubiInstance)
      .then(() => {
        ElMessage({
          message: t(`message.${action.toLowerCase()}_succeeded`, {
            name: 'operation'
          }),
          type: 'success'
        })
      })
      .catch(() => {
        ElMessage({
          message: t('message.delete_failed', { name: 'operation' }),
          type: 'error'
        })
      })
      .finally(() => {
        getList()
      })
  }
  const getList = () => {
    _getList(getAllOperations)
  }

  const terminalStates = new Set([
    'FINISHED_STATE',
    'CLOSED_STATE',
    'CANCELED_STATE',
    'TIMEOUT_STATE',
    'ERROR_STATE'
  ])

  function isTerminalState(state: string): Boolean {
    return terminalStates.has(state)
  }

  // Strip the "_STATE" suffix for display, e.g. RUNNING_STATE -> RUNNING.
  const prettyState = (state: string): string =>
    (state || 'UNKNOWN').replace(/_STATE$/, '')

  // Map an operation state to an Element Plus theme color.
  const stateType = (state: string): string => {
    if (!state) return 'info'
    if (/^(RUNNING|PENDING|INITIALIZED|COMPILED)/.test(state)) return 'primary'
    if (state.startsWith('FINISHED')) return 'success'
    if (state.startsWith('ERROR')) return 'danger'
    if (state.startsWith('CANCELED') || state.startsWith('TIMEOUT'))
      return 'warning'
    return 'info' // CLOSED and anything else
  }

  // Total + one tile per distinct operation state present, across the FULL result set.
  const summary = computed(() => {
    const rows = list.value
    const counts: Record<string, number> = {}
    rows.forEach((r) => {
      const s = r.state || 'UNKNOWN'
      counts[s] = (counts[s] || 0) + 1
    })
    const items: Array<{ label: string; value: number; type: string }> = [
      { label: t('summary.total'), value: rows.length, type: 'default' }
    ]
    Object.keys(counts)
      .sort()
      .forEach((s) => {
        items.push({ label: prettyState(s), value: counts[s], type: stateType(s) })
      })
    return items
  })
  getList()
</script>
<style lang="scss" scoped>
  header {
    display: flex;
    justify-content: flex-end;
  }
  .search-input {
    width: 260px;
    margin-bottom: 12px;
  }
  .pagination {
    margin-top: 12px;
    display: flex;
    justify-content: flex-end;
  }
</style>
