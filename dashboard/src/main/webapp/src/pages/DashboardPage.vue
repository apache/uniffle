<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

<template>
  <div class="page-wrap">
    <div class="page-section">
      <div class="page-section-header">
        <el-icon :size="16" class="page-section-icon"><Wallet /></el-icon>
        <span class="page-section-title">Dashboard</span>
      </div>
      <div class="page-section-body">
        <el-descriptions :column="3" :size="size" border>
          <el-descriptions-item>
            <template #label>
              <div class="cell-item"><el-icon :style="iconStyle"><Wallet /></el-icon> Version</div>
            </template>
            {{ pageData.dashboardInfo.version }}
          </el-descriptions-item>
          <el-descriptions-item>
            <template #label>
              <div class="cell-item"><el-icon :style="iconStyle"><Wallet /></el-icon> Start Time</div>
            </template>
            <template #default>
              {{ dateFormatter(null, null, pageData.dashboardInfo.startTime) }}
            </template>
          </el-descriptions-item>
        </el-descriptions>
      </div>
    </div>

    <div class="table-section flex-fill">
      <div class="table-section-bar">
        <el-icon :size="14" class="page-section-icon"><Setting /></el-icon>
        <span class="table-section-title">Dashboard Properties</span>
      </div>
      <el-table :data="filteredTableData" stripe height="100%">
        <el-table-column prop="argumentKey" label="Name" min-width="380" />
        <el-table-column prop="argumentValue" label="Value" min-width="380" :show-overflow-tooltip="true" />
        <el-table-column align="right">
          <template #header>
            <el-input v-model="searchKeyword" size="small" placeholder="Type to search" class="table-search-input" />
          </template>
        </el-table-column>
      </el-table>
    </div>
  </div>
</template>

<script>
import { ref, reactive, computed, onMounted } from 'vue'
import { getDashboardConf, getDashboardInfo } from '@/api/api'
import { dateFormatter } from '@/utils/common'

export default {
  setup() {
    const pageData = reactive({ tableData: [], dashboardInfo: {} })

    async function getDbConfPage() {
      try {
        const res = await getDashboardConf()
        pageData.tableData = res?.data?.data || []
      } catch (e) { console.warn('Failed to fetch dashboard conf:', e) }
    }
    async function getDbInfo() {
      try {
        const res = await getDashboardInfo()
        pageData.dashboardInfo = res?.data?.data || {}
      } catch (e) { console.warn('Failed to fetch dashboard info:', e) }
    }

    onMounted(() => { getDbConfPage(); getDbInfo() })

    const size = ref('')
    const iconStyle = computed(() => ({ marginRight: '6px' }))
    const searchKeyword = ref('')
    const filteredTableData = computed(() => {
      const keyword = searchKeyword.value.trim()
      if (!keyword) return pageData.tableData
      return pageData.tableData.filter((row) => row.argumentValue.includes(keyword) || row.argumentKey.includes(keyword))
    })

    return { pageData, iconStyle, size, filteredTableData, searchKeyword, dateFormatter }
  }
}
</script>
