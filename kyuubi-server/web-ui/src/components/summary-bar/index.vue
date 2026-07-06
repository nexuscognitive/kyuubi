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
  <div class="summary-bar">
    <div
      v-for="item in items"
      :key="item.label"
      class="summary-item">
      <div class="summary-value" :class="`is-${item.type || 'default'}`">
        {{ item.value }}
      </div>
      <div class="summary-label">{{ item.label }}</div>
    </div>
  </div>
</template>

<script lang="ts">
  // A compact row of count tiles rendered above a table. Each item is
  // { label, value, type }, where type maps to an Element Plus theme color.
  export interface SummaryItem {
    label: string
    value: string | number
    type?:
      | 'default'
      | 'primary'
      | 'success'
      | 'info'
      | 'warning'
      | 'danger'
  }

  export default {
    name: 'SummaryBar',
    props: {
      items: {
        type: Array as () => SummaryItem[],
        required: true
      }
    }
  }
</script>

<style scoped lang="scss">
  .summary-bar {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    margin-bottom: 14px;
  }
  .summary-item {
    flex: 0 0 auto;
    min-width: 92px;
    padding: 8px 18px;
    text-align: center;
    border: 1px solid var(--el-border-color-light);
    border-radius: 8px;
    background: var(--el-fill-color-blank);
  }
  .summary-value {
    font-size: 26px;
    line-height: 30px;
    font-weight: 600;
  }
  .summary-label {
    margin-top: 2px;
    font-size: 12px;
    color: var(--el-text-color-secondary);
  }
  .is-default {
    color: var(--el-text-color-primary);
  }
  .is-primary {
    color: var(--el-color-primary);
  }
  .is-success {
    color: var(--el-color-success);
  }
  .is-info {
    color: var(--el-color-info);
  }
  .is-warning {
    color: var(--el-color-warning);
  }
  .is-danger {
    color: var(--el-color-danger);
  }
</style>
