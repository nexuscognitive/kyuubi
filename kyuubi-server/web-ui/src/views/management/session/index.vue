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
  <!-- TODO we need search here -->
  <el-card>
    <el-table
      v-loading="loading"
      :data="tableData"
      max-height="500px"
      style="width: 100%">
      <el-table-column prop="user" :label="$t('user')" width="160px" />
      <!-- TODO need jump to engine page -->
      <el-table-column prop="engineId" :label="$t('engine_id')" width="160px" />
      <el-table-column prop="ipAddr" :label="$t('client_ip')" width="160px" />
      <el-table-column
        prop="kyuubiInstance"
        :label="$t('kyuubi_instance')"
        width="180px" />
      <el-table-column
        prop="sessionType"
        :label="$t('session_type')"
        width="120px" />
      <el-table-column :label="$t('status')" width="120px">
        <template #default="scope">
          <el-tooltip
            v-if="scope.row.exception"
            effect="dark"
            :content="scope.row.exception"
            placement="top">
            <el-tag type="danger">ERROR</el-tag>
          </el-tooltip>
          <el-tag v-else :type="sessionStatus(scope.row) === 'ACTIVE' ? 'success' : 'info'">
            {{ sessionStatus(scope.row) }}
          </el-tag>
        </template>
      </el-table-column>
      <!-- TODO need jump to session page -->
      <el-table-column :label="$t('session_id')" width="300px">
        <template #default="scope">
          <el-link @click="handleSessionDetailJump(scope.row.identifier)">{{
            scope.row.identifier
          }}</el-link>
        </template>
      </el-table-column>
      <el-table-column :label="$t('create_time')" width="200">
        <template #default="scope">
          {{
            scope.row.createTime != null && scope.row.createTime > -1
              ? format(scope.row.createTime, 'yyyy-MM-dd HH:mm:ss')
              : '-'
          }}
        </template>
      </el-table-column>
      <el-table-column fixed="right" :label="$t('operation.text')">
        <template #default="scope">
          <el-popconfirm
            :title="$t('operation.delete_confirm')"
            @confirm="
              handleDeleteSession(scope.row.identifier, scope.row.kyuubiInstance)
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
        </template>
      </el-table-column>
    </el-table>
  </el-card>
</template>

<script lang="ts" setup>
  import { format } from 'date-fns'
  import { getAllSessions, deleteSession } from '@/api/session'
  import { ElMessage } from 'element-plus'
  import { useI18n } from 'vue-i18n'
  import { useTable } from '@/utils/use-table'
  import { Router, useRouter } from 'vue-router'
  const { t } = useI18n()
  const { tableData, loading, getList: _getList } = useTable()
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
