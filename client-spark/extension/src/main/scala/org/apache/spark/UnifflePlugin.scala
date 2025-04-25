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
import org.apache.spark.shuffle.events.UniffleEvent
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.ui.ShuffleTab
import org.apache.uniffle.common.ProjectConstants

import java.util.Collections
import scala.collection.mutable

class UnifflePlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new UniffleDriverPlugin()

  override def executorPlugin(): ExecutorPlugin = null
}

private class UniffleDriverPlugin extends DriverPlugin with Logging {
  private var _sc: Option[SparkContext] = None

  override def init(sc: SparkContext, pluginContext: PluginContext): java.util.Map[String, String] = {
    logInfo("Initializing UniffleDriverPlugin...")
    _sc = Some(sc)
    UniffleListener.register(sc)
    postBuildInfoEvent(sc)
    attachUI(sc)
    Collections.emptyMap()
  }

  private def attachUI(context: SparkContext): Unit = {
    val kvStore = context.statusStore.store.asInstanceOf[ElementTrackingStore]
    val statusStore = new UniffleStatusStore(kvStore)
    context.ui.foreach(new ShuffleTab(statusStore, _))
  }

  private def postBuildInfoEvent(context: SparkContext): Unit = {
    val buildInfo = new mutable.LinkedHashMap[String, String]()
    buildInfo.put("Version", ProjectConstants.VERSION)
    buildInfo.put("Commit Id", ProjectConstants.getGitCommitId)
    buildInfo.put("Revision", ProjectConstants.REVISION)

    val event = BuildInfoEvent(buildInfo.toMap)
    context.listenerBus.post(event)
  }
}

case class BuildInfoEvent(info: Map[String, String]) extends UniffleEvent