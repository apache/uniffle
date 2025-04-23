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

import org.apache.spark.scheduler.SparkListenerEvent

sealed trait UniffleEvent extends SparkListenerEvent {}

case class BuildInfoEvent(info: Map[String, String]) extends UniffleEvent {}

// task shuffle relative events
sealed abstract case class ShuffleMetric(duration: Long, byteSize: Long)
case class TaskShuffleWriteInfoEvent(stageId: Int,
                                     shuffleId: Int,
                                     taskId: Long,
                                     shuffleServerWriteMetrics: java.util.Map[String, ShuffleWriteMetric]
                                    ) extends UniffleEvent {}
class ShuffleWriteMetric(override val duration: Long, override val byteSize: Long) extends ShuffleMetric(duration, byteSize)
case class TaskShuffleReadInfoEvent(stageId: Int,
                                    shuffleId: Int,
                                    taskId: Long,
                                    shuffleServerReadMetrics: java.util.Map[String, ShuffleReadMetric]
                                   ) extends UniffleEvent {}
// todo: add memory/localfile/hdfs metrics into read
class ShuffleReadMetric(override val duration: Long, override val byteSize: Long) extends ShuffleMetric(duration, byteSize)

// assignment relative events
case class ShuffleAssignmentInfoEvent(shuffleId: Int,
                                      assignedServers: java.util.List[String]) extends UniffleEvent {}

case class ShuffleReassignmentInfoEvent(stageId: Int,
                                        shuffleId: Int,
                                        taskAttemptId: Long,
                                        replacingServers: java.util.Map[String, java.util.List[String]]
                                       ) extends UniffleEvent {}
