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

package org.apache.spark

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging

import java.util.Collections
import scala.collection.mutable

class UnifflePlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new UniffleDriverPlugin()

  override def executorPlugin(): ExecutorPlugin = null
}

private class UniffleDriverPlugin extends DriverPlugin with Logging {
  private var _sc: Option[SparkContext] = None

  override def init(sc: SparkContext, pluginContext: PluginContext): java.util.Map[String, String] = {
    _sc = Some(sc)
    val conf = pluginContext.conf()
    UniffleListener.register(sc)
    postBuildInfoEvent(sc)
    EventUtils.attachUI(sc)
    Collections.emptyMap()
  }

  private def postBuildInfoEvent(context: SparkContext): Unit = {
    val buildInfo = new mutable.LinkedHashMap[String, String]()
    buildInfo.put("Uniffle Version", "0.8.1")

    val event = UniffleBuildInfoEvent(buildInfo.toMap)
    EventUtils.post(context, event)
  }
}