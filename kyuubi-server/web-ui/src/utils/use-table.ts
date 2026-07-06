/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ref, Ref } from 'vue'

export function useTable() {
  const list: Ref<any[]> = ref([]) // full, unfiltered result set
  const tableData: Ref<any[]> = ref([]) // current page after filter + sort
  const loading = ref(false)
  const currentPage = ref(1)
  const pageSize = ref(10)
  const totalPage = ref(0) // total rows AFTER filtering
  const searchText = ref('')
  const sortProp = ref('')
  const sortOrder = ref('') // 'ascending' | 'descending' | ''

  // Apply free-text filter -> sort -> pagination over the FULL list, so both act across all
  // pages rather than only the visible one.
  const setTableData = () => {
    let data = [...list.value]

    const q = searchText.value.trim().toLowerCase()
    if (q) {
      data = data.filter((row) =>
        Object.values(row || {}).some(
          (v) => v != null && String(v).toLowerCase().includes(q)
        )
      )
    }

    if (sortProp.value && sortOrder.value) {
      const prop = sortProp.value
      const dir = sortOrder.value === 'ascending' ? 1 : -1
      data.sort((a, b) => {
        const av = a?.[prop]
        const bv = b?.[prop]
        if (av == null && bv == null) return 0
        if (av == null) return 1
        if (bv == null) return -1
        if (typeof av === 'number' && typeof bv === 'number') {
          return (av - bv) * dir
        }
        return String(av).localeCompare(String(bv)) * dir
      })
    }

    totalPage.value = data.length
    tableData.value = data.slice(
      (currentPage.value - 1) * pageSize.value,
      currentPage.value * pageSize.value
    )
    loading.value = false
  }

  const handleSizeChange = () => {
    currentPage.value = 1
    setTableData()
  }

  const handleCurrentChange = () => {
    setTableData()
  }

  const handleSortChange = ({
    prop,
    order
  }: {
    prop: string | null
    order: string | null
  }) => {
    sortProp.value = prop || ''
    sortOrder.value = order || ''
    currentPage.value = 1
    setTableData()
  }

  const handleSearch = () => {
    currentPage.value = 1
    setTableData()
  }

  const getList = (func: Function, data?: any) => {
    loading.value = true
    func(data)
      .then((res: any[]) => (list.value = res || []))
      .catch(() => (list.value = []))
      .finally(() => {
        currentPage.value = 1
        setTableData()
      })
  }

  return {
    list, // full, unfiltered result set (for summaries/aggregates)
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
    getList
  }
}
