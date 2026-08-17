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
  <header>
    <!--
      The NX1 mark stays; the wordmark is the product name rather than the
      company one. Split like the Nexus One lockup - light first word, bold
      second - so it reads as the same family of marks.
    -->
    <div class="brand">
      <img
        class="brand-mark"
        src="@/assets/images/nx1-mark.svg"
        alt="Nexus One" />
      <span v-if="!isCollapse" class="brand-name">Spark<b>Engine</b></span>
    </div>
    <span v-if="!isCollapse" class="version">{{ version }}</span>
  </header>
  <c-menu :is-collapse="isCollapse" :active-path="activePath" :menus="menus" />
</template>

<script setup lang="ts">
  import { ref, reactive } from 'vue'
  import { useStore } from '@/pinia/layout'
  import { storeToRefs } from 'pinia'
  import { useRoute } from 'vue-router'
  import { MENUS } from './types'
  import cMenu from '@/components/menu/index.vue'

  const menus = reactive(MENUS)
  const store = useStore()
  const { isCollapse } = storeToRefs(store)
  const router = useRoute()
  const activePath = ref(router.path)
  const version = import.meta.env.VITE_APP_VERSION
</script>

<style lang="scss" scoped>
  header {
    width: 100%;
    position: absolute;
    top: 0;
    left: 0;
    height: var(--nx1-header-height);
    line-height: var(--nx1-header-height);
    padding: 0 16px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    box-sizing: border-box;
    // Aligns the navy rail with the content header's hairline.
    border-bottom: 1px solid rgba(255, 255, 255, 0.08);
    .brand {
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 0;
    }
    .brand-mark {
      width: 28px;
      height: 28px;
      flex: none;
    }
    .brand-name {
      font-family: var(--nx1-font-body);
      font-size: 19px;
      font-weight: 300;
      letter-spacing: -0.01em;
      line-height: 1;
      color: #fff;
      white-space: nowrap;

      b {
        font-weight: 600;
      }
    }
    // The version reads as a mono eyebrow rather than body text.
    .version {
      font-family: var(--nx1-font-mono);
      font-size: 10px;
      font-weight: 600;
      letter-spacing: 0.1em;
      text-transform: uppercase;
      color: rgba(255, 255, 255, 0.55);
      flex: none;
    }
  }
  .el-menu {
    margin-top: var(--nx1-header-height);
  }
</style>
