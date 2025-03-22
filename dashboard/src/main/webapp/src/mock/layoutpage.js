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

Mock.mock(/\/web\/coordinator\/coordinatorServers/, 'get', function (options) {
  return {
    code: 0,
    data: {
      'sports.sina.com.cn': 'http://sports.sina.com.cn:29998',
      'sports.faceback.com.cn': 'http://sports.faceback.com.cn:29998',
      'sports.twiter.com.cn': 'http://sports.twiter.com.cn:29998'
    },
    errMsg: 'success'
  }
})
export default Mock
