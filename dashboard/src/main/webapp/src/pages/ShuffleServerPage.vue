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
  <div>
    <el-row :gutter="20">
      <el-col :span="4">
        <router-link
          class="router-link-active"
          :to="{ path: '/shuffleserverpage/activeNodeList', query: { isExcludedPage: false } }"
          @click.native="routerHandler"
        >
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Active</span>
              </div>
            </template>
            <div class="activenode">{{ dataList.allshuffleServerSize.ACTIVE ?? 0 }}</div>
          </el-card>
        </router-link>
      </el-col>
      <el-col :span="4">
        <router-link
          class="router-link-active"
          :to="{ path: '/shuffleserverpage/decommissioningNodeList', query: { isExcludedPage: false } }"
          @click.native="routerHandler"
        >
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Decommissioning</span>
              </div>
            </template>
            <div class="decommissioningnode">
              {{ dataList.allshuffleServerSize.DECOMMISSIONING ?? 0 }}
            </div>
          </el-card>
        </router-link>
      </el-col>
      <el-col :span="4">
        <router-link
          class="router-link-active"
          :to="{ path: '/shuffleserverpage/decommissionedNodeList', query: { isExcludedPage: false } }"
          @click.native="routerHandler"
        >
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Decommissioned</span>
              </div>
            </template>
            <div class="decommissionednode">
              {{ dataList.allshuffleServerSize.DECOMMISSIONED ?? 0 }}
            </div>
          </el-card>
        </router-link>
      </el-col>
      <el-col :span="4">
        <router-link
          class="router-link-active"
          :to="{ path: '/shuffleserverpage/lostNodeList', query: { isExcludedPage: false } }"
          @click.native="routerHandler"
          :updateTotalPage="updateTotalPage"
        >
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Lost</span>
              </div>
            </template>
            <div class="lostnode">{{ dataList.allshuffleServerSize.LOST ?? 0 }}</div>
          </el-card>
        </router-link>
      </el-col>
      <el-col :span="4">
        <router-link
          class="router-link-active"
          :to="{ path: '/shuffleserverpage/unhealthyNodeList', query: { isExcludedPage: false } }"
          @click.native="routerHandler"
        >
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Unhealthy</span>
              </div>
            </template>
            <div class="unhealthynode">{{ dataList.allshuffleServerSize.UNHEALTHY ?? 0 }}</div>
          </el-card>
        </router-link>
      </el-col>
      <el-col :span="4">
        <router-link
          class="router-link-active"
          :to="{ path: '/shuffleserverpage/excludeNodeList', query: { isExcludedPage: true } }"
          @click.native="routerHandler"
        >
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Excludes</span>
              </div>
            </template>
            <div class="excludesnode">{{ dataList.allshuffleServerSize.EXCLUDED ?? 0 }}</div>
          </el-card>
        </router-link>
      </el-col>
    </el-row>
    <el-divider />
    <el-row :gutter="24">
      <div style="width: 100%">
        <router-view></router-view>
      </div>
    </el-row>
  </div>
</template>

<script>
import { onMounted, reactive, provide } from 'vue'
import { getShufflegetStatusTotal } from '@/api/api'
import { useCurrentServerStore } from '@/store/useCurrentServerStore'

export default {
  setup() {
    const dataList = reactive({
      allshuffleServerSize: {
        ACTIVE: 0,
        DECOMMISSIONED: 0,
        DECOMMISSIONING: 0,
        EXCLUDED: 0,
        LOST: 0,
        UNHEALTHY: 0
      }
    })

    const currentServerStore = useCurrentServerStore()
    async function getShuffleStatusTotalPage() {
      const res = await getShufflegetStatusTotal()
      dataList.allshuffleServerSize = res.data.data
    }

    // The system obtains data from global variables and requests the interface to obtain new data after data changes.
    currentServerStore.$subscribe((mutable, state) => {
      if (state.currentServer) {
        getShuffleStatusTotalPage()
      }
    })

    onMounted(() => {
      // If the coordinator address to request is not found in the global variable, the request is not initiated.
      if (currentServerStore.currentServer) {
        getShuffleStatusTotalPage()
      }
    })

    const routerHandler = () => {
      getShuffleStatusTotalPage()
    }
    /**
     * After the missing server list is removed, the number of callbacks is reduced.
     */
    provide('updateTotalPage', () => {
      getShuffleStatusTotalPage()
    })

    return { dataList, routerHandler }
  }
}
</script>

<style scoped>
.cardtile {
  font-size: larger;
}

/* Remove the underscore from the route label. */
.router-link-active {
  text-decoration: none;
}

.activenode {
  font-family: 'Lantinghei SC';
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
  color: green;
}

.decommissioningnode {
  font-family: 'Lantinghei SC';
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
  color: #00c4ff;
}

.decommissionednode {
  font-family: 'Lantinghei SC';
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
  color: blue;
}

.lostnode {
  font-family: 'Lantinghei SC';
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
  color: red;
}

.unhealthynode {
  font-family: 'Lantinghei SC';
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
  color: #ff8800;
}

.excludesnode {
  font-family: 'Lantinghei SC';
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
}
</style>
