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
import java.util.concurrent.TimeUnit

import org.apache.mesos.Protos.{TaskState, Offer}
import org.apache.mesos.state.State

import org.apache.mesos.state.ZooKeeperState
import org.apache.samza.container.{TaskNamesToSystemStreamPartitions, TaskName}
import org.apache.samza.job.ApplicationStatus
import org.apache.samza.job.ApplicationStatus._
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.{Util, Logging}

import org.apache.samza.config.Config
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config.MesosConfig
import org.apache.samza.config.MesosConfig.Config2Mesos

class SamzaSchedulerState(persistentStore: State, config: Config) extends Logging {
  var completedTasks: Int = 0
  var failedExecutors: Int = 0
  var releasedExecutors: Int = 0

  var finishedTasks: Set[Int] = Set()
  var runningTasks: Map[Int, Offer] = Map()
  var taskToTaskNames: Map[Int, util.Map[TaskName, util.Set[SystemStreamPartition]]] = Map()
  var status: ApplicationStatus = New

  var taskCount: Int = config.getTaskCount.getOrElse(1)
  var neededExecutors: Int = taskCount
  var unclaimedTasks: Set[Int] = (0 until taskCount).toSet
  var tasksToSSPTaskNames: Map[Int, TaskNamesToSystemStreamPartitions] = Util.assignContainerToSSPTaskNames(config, taskCount)
  var taskNameToChangeLogPartitionMapping: Map[TaskName, Int] = Util.getTaskNameToChangeLogPartitionMapping(config, tasksToSSPTaskNames)

  var currentState: TaskState = TaskState.TASK_STARTING

  def persist(): Boolean = {
    persist(persistentStore)
  }

  def restore(): Boolean = {
    restore(persistentStore)
  }

  def persist(persistentStore: State): Boolean = {
     false
  }

  def restore(persistentStore: State): Boolean = {
    false
  }
}