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

import Mock from 'mockjs'

Mock.mock(/\/app\/total/, 'get', function (options) {
  return { code: 0, data: { appTotality: 10, appCurrent: 10 }, errMsg: 'success' }
})

Mock.mock(/\/app\/appInfos/, 'get', function (options) {
  return {
    code: 0,
    data: [
      {
        userName: 'pro_sale',
        appId: 'application_1709707920318_70148215_192.168.2.15-3900494',
        updateTime: '2024-07-31 14:03:03'
      },
      {
        userName: 'pro_sale',
        appId: 'application_1709707920318_70149011_192.168.2.15-3900581',
        updateTime: '2024-07-31 14:03:07'
      },
      {
        userName: 'pro_sale',
        appId: 'application_1709707920318_70149008_192.168.2.15-3900578',
        updateTime: '2024-07-31 14:03:06'
      },
      {
        userName: 'pro_conpany',
        appId: 'application_1709707920318_70144711_192.168.2.15-3900288',
        updateTime: '2024-07-31 14:03:04'
      },
      {
        userName: 'pro_conpany',
        appId: 'application_1709707920318_70144710_192.168.2.15-3900312',
        updateTime: '2024-07-31 14:02:22'
      },
      {
        userName: 'pro_conpany',
        appId: 'application_1709707920318_70147234_192.168.2.15-3900424',
        updateTime: '2024-07-31 14:03:02'
      },
      {
        userName: 'pro_conpany',
        appId: 'application_1709707920318_70145262_192.168.2.15-3900291',
        updateTime: '2024-07-31 14:03:07'
      },
      {
        userName: 'pro_conpany',
        appId: 'application_1709707920318_70144709_192.168.2.15-3900287',
        updateTime: '2024-07-31 14:03:05'
      },
      {
        userName: 'pro_conpany',
        appId: 'application_1709707920318_70133543_192.168.2.15-3899697',
        updateTime: '2024-07-31 14:03:03'
      },
      {
        userName: 'pro_conpany',
        appId: 'application_1709707920318_70147213_192.168.2.15-3900423',
        updateTime: '2024-07-31 14:03:11'
      }
    ],
    errMsg: 'success'
  }
})

Mock.mock(/\/app\/userTotal/, 'get', function (options) {
  return {
    code: 0,
    data: [
      {
        userName: 'pro_conpany',
        appNum: 7
      },
      {
        userName: 'pro_sale',
        appNum: 3
      }
    ],
    errMsg: 'success'
  }
})
export default Mock
