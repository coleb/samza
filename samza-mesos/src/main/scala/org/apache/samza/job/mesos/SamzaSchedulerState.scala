/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.job.mesos

import java.util

import org.apache.mesos.Protos.{Offer, TaskState}
import org.apache.samza.config.Config
import org.apache.samza.config.MesosConfig.Config2Mesos
import org.apache.samza.container.{TaskName, TaskNamesToSystemStreamPartitions}
import org.apache.samza.job.ApplicationStatus
import org.apache.samza.job.ApplicationStatus._
import org.apache.samza.util.{Logging, Util}

class SamzaSchedulerState(config: Config) extends Logging {
  var currentStatus: ApplicationStatus = New
  var currentState: TaskState = TaskState.TASK_STARTING

  var initialTaskCount: Int = config.getTaskCount.getOrElse(1)
  var initialSamzaTaskIDs = (0 until initialTaskCount).toSet

  var samzaTaskIDToSSPTaskNames: Map[Int, TaskNamesToSystemStreamPartitions] = Util.assignContainerToSSPTaskNames(config, initialTaskCount)
  var taskNameToChangeLogPartitionMapping: Map[TaskName, Int] = Util.getTaskNameToChangeLogPartitionMapping(config, samzaTaskIDToSSPTaskNames)

  var runningTasks: Set[MesosTask] = Set()
  var finishedTasks: Set[MesosTask] = Set()
  var unclaimedTasks: Set[MesosTask] = initialSamzaTaskIDs.map(new MesosTask(config, this, _)).toSet

  var offerPool: Set[Offer] = Set()
}