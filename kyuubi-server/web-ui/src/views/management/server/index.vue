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
        prop="host"
        :label="$t('server_ip')"
        min-width="20%"
        sortable="custom" />
      <el-table-column
        prop="namespace"
        :label="$t('namespace')"
        min-width="20%"
        sortable="custom" />
      <el-table-column
        prop="instance"
        :label="$t('kyuubi_instance')"
        min-width="20%"
        sortable="custom" />
      <el-table-column prop="attributes.version" :label="$t('version')" />
      <el-table-column
        prop="status"
        :label="$t('state')"
        min-width="20%"
        sortable="custom" />
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
  import { getAllServer } from '@/api/server'
  import { useTable } from '@/utils/use-table'

  const {
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
  const getList = () => {
    _getList(getAllServer)
  }
  getList()
</script>

<style scoped lang="scss">
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
