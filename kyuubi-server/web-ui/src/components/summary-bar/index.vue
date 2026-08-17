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
      class="summary-item"
      :class="`is-${item.type || 'default'}`">
      <div class="summary-value">{{ item.value }}</div>
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
    type?: 'default' | 'primary' | 'success' | 'info' | 'warning' | 'danger'
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
  /*
   * Accent-stripe stat tiles. Two rules drive this:
   *
   *  - The value stays in text ink; the left-edge stripe carries the status
   *    identity. Colouring the number itself makes the tile read as a status
   *    badge and drops legibility for anyone with a colour vision deficiency.
   *  - The stripe never encodes state on its own -- every tile is labelled, so
   *    colour is a second channel rather than the only one.
   */
  .summary-bar {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    margin-bottom: 18px;
  }
  .summary-item {
    position: relative;
    flex: 0 0 auto;
    min-width: 108px;
    padding: 12px 20px 12px 22px;
    overflow: hidden;
    border: 1px solid var(--nx1-border);
    border-radius: var(--nx1-radius);
    background: var(--nx1-card);
    box-shadow: var(--nx1-shadow-1);

    &::before {
      content: '';
      position: absolute;
      top: 0;
      left: 0;
      bottom: 0;
      width: 6px;
      background: var(--nx1-stripe, var(--nx1-border));
      transition: width 0.15s ease;
    }

    &:hover::before {
      width: 10px;
    }
  }
  .summary-value {
    font-family: var(--nx1-font-display);
    font-size: 28px;
    line-height: 34px;
    font-weight: 400;
    color: var(--nx1-text);
    font-variant-numeric: tabular-nums;
  }
  .summary-label {
    margin-top: 2px;
    font-family: var(--nx1-font-mono);
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--nx1-text-muted);
  }

  /*
   * Status identity lives entirely in the stripe hue. These are the NX1 brand
   * status values, used as a status palette rather than a categorical one: no two
   * stripes have to be told apart from each other, and every tile carries a
   * visible mono label plus a 12px gap. That labelling is what makes the moss
   * stripe (2.98:1 against the card) and the muted teal acceptable here -- do not
   * reuse this set as categorical series colours, where hue would be the only
   * channel and those two would not separate reliably under deuteranopia.
   */
  .is-default {
    --nx1-stripe: var(--nx1-border);
  }
  .is-primary {
    --nx1-stripe: var(--nx1-sky);
  }
  .is-success {
    --nx1-stripe: var(--nx1-moss);
  }
  .is-info {
    --nx1-stripe: var(--nx1-teal);
  }
  .is-warning {
    --nx1-stripe: var(--nx1-warning);
  }
  .is-danger {
    --nx1-stripe: var(--nx1-danger);
  }
</style>
