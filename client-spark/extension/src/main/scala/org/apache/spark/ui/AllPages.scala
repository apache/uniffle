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

package org.apache.spark.ui

import org.apache.spark.internal.Logging

import javax.servlet.http.HttpServletRequest
import scala.xml.{Node, NodeSeq}

class AllPages(parent: MainTab) extends WebUIPage("") with Logging {
  private val runtimeStatusStore = parent.store

  private def propertyHeader = Seq("Name", "Value")

  private def propertyRow(kv: (String, String)) = <tr>
    <td>
      {kv._1}
    </td> <td>
      {kv._2}
    </td>
  </tr>
  override def render(request: HttpServletRequest): Seq[Node] = {
    val buildInfo = runtimeStatusStore.buildInfo()
    val buildInfoTableUI = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      buildInfo.info,
      fixedWidth = true
    )

    val summary: NodeSeq =
      <div>
        <div>
          <span class="collapse-sql-properties collapse-table"
                onClick="collapseTable('collapse-sql-properties', 'sql-properties')">
            <h4>
              <span class="collapse-table-arrow arrow-closed"></span>
              <a>Uniffle Build Information</a>
            </h4>
          </span>
          <div class="sql-properties collapsible-table collapsed">
            {buildInfoTableUI}
          </div>
        </div>
      </div>

    UIUtils.headerSparkPage(request, "Uniffle", summary, parent)
  }
}
