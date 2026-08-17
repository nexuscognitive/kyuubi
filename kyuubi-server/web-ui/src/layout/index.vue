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
  <div class="common-layout">
    <el-container>
      <el-aside>
        <Aside />
      </el-aside>
      <el-container>
        <el-header>
          <Header />
        </el-header>
        <el-main>
          <router-view v-slot="slotProps">
            <keep-alive :include="['DataAgent']">
              <component :is="slotProps && slotProps.Component" />
            </keep-alive>
          </router-view>
        </el-main>
      </el-container>
    </el-container>
  </div>
</template>

<script setup lang="ts">
  import { onMounted } from 'vue'
  import Aside from './components/aside/index.vue'
  import Header from './components/header/index.vue'
  import { useLocalesStore } from '@/pinia/locales/locales'
  import { useI18n } from 'vue-i18n'

  const { locale } = useI18n()
  const localesStore = useLocalesStore()

  onMounted(() => {
    locale.value = localesStore.getLocale
  })
</script>

<style lang="scss" scoped>
  .common-layout {
    height: 100%;

    .el-container {
      min-height: 100vh;

      ::v-deep(.el-aside) {
        width: auto;
        position: relative;
        background: var(--nx1-navy);
      }

      .el-header {
        display: flex;
        align-items: center;
        height: var(--nx1-header-height);
        padding: 0 20px 0 0;
        // A cream hairline carries the edge instead of a drop shadow -- on the
        // warm canvas a shadow here reads as a smudge.
        border-bottom: 1px solid var(--nx1-border);
        background: var(--nx1-card);
      }

      ::v-deep(.el-main) {
        background: var(--nx1-canvas);
        padding: 24px;
      }
    }
  }
</style>
