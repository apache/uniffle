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

import moment from 'moment'
/**
 * Format the memory display value.
 * @param row
 * @param column
 * @param cellValue
 * @returns {string}
 */
const memFormatter = (row, column, cellValue) => {
  if (cellValue == null || isNaN(cellValue)) {
    return '-'
  }
  const arrUnit = ['B', 'K', 'M', 'G', 'T', 'P']
  const baseStep = 1024
  const unitCount = arrUnit.length
  let unitIndex = 0
  while (cellValue >= baseStep && unitIndex < unitCount - 1) {
    unitIndex++
    cellValue /= baseStep
  }
  cellValue = cellValue.toFixed(2)
  return cellValue + ' ' + arrUnit[unitIndex]
}
/**
 * Format the time display value.
 * @param row
 * @param column
 * @param cellValue
 * @returns {string}
 */
const dateFormatter = (row, column, cellValue) => {
  if (cellValue == null) {
    return '-'
  }
  return moment(cellValue).format('YYYY-MM-DD HH:mm:ss')
}

export { memFormatter, dateFormatter }
