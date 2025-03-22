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
      <el-collapse-item title="Dashboard" name="1">
        <div>
          <el-descriptions class="margin-top" :column="3" :size="size" border>
            <el-descriptions-item>
              <template #label>
                <div class="cell-item">
                  <el-icon :style="iconStyle">
                    <Wallet />
                  </el-icon>
                  Version
                </div>
              </template>
              {{ pageData.dashboardInfo.version}}
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
                {{ dateFormatter(null, null, pageData.dashboardInfo.startTime) }}
              </template>
            </el-descriptions-item>
          </el-descriptions>
        </div>
      </el-collapse-item>
      <el-collapse-item title="Dashboard Properties" name="2">
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
import { ref, reactive, computed, onMounted } from 'vue'
import {
  getDashboardConf,
  getDashboardInfo
} from '@/api/api'
import { dateFormatter } from '@/utils/common'

export default {
  setup() {
    const pageData = reactive({
      activeNames: ['1', '2'],
      tableData: [],
      dashboardInfo: {}
    })

    async function getDbConfPage() {
      const res = await getDashboardConf()
      pageData.tableData = res.data.data
    }
    async function getDbInfo() {
      const res = await getDashboardInfo()
      pageData.dashboardInfo = res.data.data
    }

    onMounted(() => {
      getDbConfPage()
      getDbInfo()
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
