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
  <div class="demo-collapse">
    <el-collapse v-model="pageData.activeNames" accordion:false>
      <el-collapse-item title="Coordinator Server" name="1">
        <div>
          <el-descriptions class="margin-top" :column="3" :size="size" border>
            <el-descriptions-item>
              <template #label>
                <div class="cell-item">
                  <el-icon :style="iconStyle">
                    <Platform />
                  </el-icon>
                  Coordinatro ID
                </div>
              </template>
              {{ pageData.serverInfo.coordinatorId }}
            </el-descriptions-item>
            <el-descriptions-item>
              <template #label>
                <div class="cell-item">
                  <el-icon :style="iconStyle">
                    <Link />
                  </el-icon>
                  IP Address
                </div>
              </template>
              {{ pageData.serverInfo.serverIp }}
            </el-descriptions-item>
            <el-descriptions-item>
              <template #label>
                <div class="cell-item">
                  <el-icon :style="iconStyle">
                    <Wallet />
                  </el-icon>
                  Coordinator Server Port
                </div>
              </template>
              {{ pageData.serverInfo.serverPort }}
            </el-descriptions-item>
            <el-descriptions-item>
              <template #label>
                <div class="cell-item">
                  <el-icon :style="iconStyle">
                    <Wallet />
                  </el-icon>
                  Coordinator Web Port
                </div>
              </template>
              {{ pageData.serverInfo.serverWebPort }}
            </el-descriptions-item>
            <el-descriptions-item>
              <template #label>
                <div class="cell-item">
                  <el-icon :style="iconStyle">
                    <Wallet />
                  </el-icon>
                  Monitoring Information
                </div>
              </template>
              <div class="mb-4">
                <el-button type="primary" @click="handlerMetrics">Metrics</el-button>
                <el-button type="success" @click="handlerPromMetrics">Prometheus Metrics</el-button>
                <el-button type="info" @click="handlerStacks">Stacks</el-button>
              </div>
            </el-descriptions-item>
            <el-descriptions-item>
              <template #label>
                <div class="cell-item">
                  <el-icon :style="iconStyle">
                    <Wallet />
                  </el-icon>
                  Version
                </div>
              </template>
              {{ pageData.serverInfo.version }}_{{ pageData.serverInfo.gitCommitId }}
            </el-descriptions-item>
            <el-descriptions-item>
              <template #label>
                <div class="cell-item">
                  <el-icon :style="iconStyle">
                    <Wallet />
                  </el-icon>
                  Start Time
                </div>
              </template>
              <template #default>
                {{ dateFormatter(null, null, pageData.serverInfo.startTime) }}
              </template>
            </el-descriptions-item>
          </el-descriptions>
        </div>
      </el-collapse-item>
      <el-collapse-item title="Coordinator Properties" name="2">
        <el-table :data="filteredTableData" stripe style="width: 100%">
          <el-table-column prop="argumentKey" label="Name" min-width="380" />
          <el-table-column prop="argumentValue" label="Value" min-width="380" :show-overflow-tooltip="true" />
          <el-table-column align="right">
            <template #header>
              <el-input v-model="searchKeyword" size="small" placeholder="Type to search" />
            </template>
          </el-table-column>
        </el-table>
      </el-collapse-item>
    </el-collapse>
  </div>
</template>

<script>
import { ref, reactive, computed, onMounted, watch } from 'vue'
import { ElMessage } from 'element-plus'
import {
  getCoordinatorConf,
  getCoordinatorMetrics,
  getCoordinatorPrometheusMetrics,
  getCoordinatorServerInfo,
  getCoordinatorStacks
} from '@/api/api'
import { useCurrentServerStore } from '@/store/useCurrentServerStore'
import { dateFormatter } from '@/utils/common'

export default {
  setup() {
    const pageData = reactive({
      activeNames: ['1', '2'],
      tableData: [],
      serverInfo: {}
    })
    const currentServerStore = useCurrentServerStore()

    async function getCoordinatorServerConfPage() {
      const res = await getCoordinatorConf()
      pageData.tableData = res.data.data
    }
    async function getCoorServerInfo() {
      const res = await getCoordinatorServerInfo()
      pageData.serverInfo = res.data.data
    }

    async function handlerMetrics() {
      try {
        const response = await getCoordinatorMetrics()
        if (response.status >= 200 && response.status < 300) {
          const newWindow = window.open('', '_blank')
          newWindow.document.write('<pre>' + JSON.stringify(response.data, null, 2) + '</pre>')
        } else {
          ElMessage.error('Request failed.')
        }
      } catch (err) {
        ElMessage.error('Internal error.')
      }
    }
    async function handlerPromMetrics() {
      try {
        const response = await getCoordinatorPrometheusMetrics()
        if (response.status >= 200 && response.status < 300) {
          const newWindow = window.open('', '_blank')
          newWindow.document.write('<pre>' + response.data + '</pre>')
        } else {
          ElMessage.error('Request failed.')
        }
      } catch (err) {
        ElMessage.error('Internal error.')
      }
    }
    async function handlerStacks() {
      try {
        const response = await getCoordinatorStacks()
        if (response.status >= 200 && response.status < 300) {
          const newWindow = window.open('', '_blank')
          newWindow.document.write('<pre>' + response.data + '</pre>')
        } else {
          ElMessage.error('Request failed.')
        }
      } catch (err) {
        ElMessage.error('Internal error.')
      }
    }

    /**
     * The system obtains data from global variables and requests the interface to obtain new data after data changes.
     */
    watch(() => currentServerStore.currentServer, () => {
      getCoordinatorServerConfPage()
      getCoorServerInfo()
    })

    onMounted(() => {
      // If the coordinator address to request is not found in the global variable, the request is not initiated.
      if (currentServerStore.currentServer) {
        getCoordinatorServerConfPage()
        getCoorServerInfo()
      }
    })

    const size = ref('')
    const iconStyle = computed(() => {
      const marginMap = {
        large: '8px',
        default: '6px',
        small: '4px'
      }
      return {
        marginRight: marginMap[size.value] || marginMap.default
      }
    })
    const blockMargin = computed(() => {
      const marginMap = {
        large: '32px',
        default: '28px',
        small: '24px'
      }
      return {
        marginTop: marginMap[size.value] || marginMap.default
      }
    })

    /**
     * The following describes how to handle blacklist select events.
     */
    const searchKeyword = ref('')
    const filteredTableData = computed(() => {
      const keyword = searchKeyword.value.trim()
      if (!keyword) {
        return pageData.tableData
      } else {
        return pageData.tableData.filter((row) => {
          return row.argumentValue.includes(keyword) || row.argumentKey.includes(keyword)
        })
      }
    })

    return {
      pageData,
      iconStyle,
      blockMargin,
      size,
      handlerMetrics,
      handlerPromMetrics,
      handlerStacks,
      filteredTableData,
      searchKeyword,
      dateFormatter
    }
  }
}
</script>
<style>
.cell-item {
  display: flex;
  align-items: center;
}

.margin-top {
  margin-top: 20px;
}
</style>
