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

import org.apache.mesos.Protos._
import org.apache.mesos.{Scheduler, SchedulerDriver}
import org.apache.samza.config.MesosConfig
import org.apache.samza.container.TaskNamesToSystemStreamPartitions
import org.apache.samza.job.{CommandBuilder, ShellCommandBuilder}
import org.apache.samza.util.Util
import org.slf4j.Logger

class SamzaScheduler(config: MesosConfig) extends Scheduler {
  val log = Logger.getLogger(getClass.getName)
  var currentStatus = TaskState.TASK_STARTING
  var state: SamzaSchedulerState = null

  state.taskCount = config.getTaskCount match {
    case Some(count) => count
    case None =>
      info("No %s specified. Defaulting to one container." format YarnConfig.TASK_COUNT)
      1
  }

  val tasksToSSPTaskNames: Map[Int, TaskNamesToSystemStreamPartitions] = Util.assignContainerToSSPTaskNames(config, state.taskCount)
  val taskNameToChangeLogPartitionMapping = Util.getTaskNameToChangeLogPartitionMapping(config, tasksToSSPTaskNames)

  state.neededContainers = state.taskCount
  state.unclaimedTasks = (0 until state.taskCount).toSet

  info("Awaiting offers for %s containers" format state.taskCount)

  override def shouldShutdown = state.completedTasks == state.taskCount

  def registered(driver: SchedulerDriver, p2: FrameworkID, p3: MasterInfo) {}

  def reregistered(driver: SchedulerDriver, p2: MasterInfo) {}

  def offerRescinded(driver: SchedulerDriver, p2: OfferID) {}

  def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]) {
    for (offer <- offers) {
      log.info("Received offer " + offer)

      state.unclaimedTasks.headOption match {
        case Some(taskId) => {
          info("Got available task id (%d) for offer: %s" format(taskId, container))

          val sspTaskNames: TaskNamesToSystemStreamPartitions = tasksToSSPTaskNames.getOrElse(taskId, TaskNamesToSystemStreamPartitions())
          info("Claimed SSP taskNames %s for offer ID %s" format(sspTaskNames, taskId))

          val cmdBuilderClassName = config.getCommandClass.getOrElse(classOf[ShellCommandBuilder].getName)
          val cmdBuilder = Class.forName(cmdBuilderClassName).newInstance.asInstanceOf[CommandBuilder]
            .setConfig(config)
            .setName("samza-container-%s" format taskId)
            .setTaskNameToSystemStreamPartitionsMapping(sspTaskNames.getJavaFriendlyType)
            .setTaskNameToChangeLogPartitionMapping(taskNameToChangeLogPartitionMapping.map(kv => kv._1 -> Integer.valueOf(kv._2)).asJava)
          val command = cmdBuilder.buildCommand
          info("Task ID %s using command %s" format(taskId, command))

          val cpuResource = Resource.newBuilder
            .setName("cpus")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder().setValue(1))

          val commandInfo = CommandInfo.newBuilder
            .setValue(command)

          val task = TaskInfo.newBuilder
            .setName(taskId.getValue)
            .setTaskId(taskId)
            .setSlaveId(offer.getSlaveId)
            .addResources(cpuResource)
            .setCommand(commandInfo)
            .build

          /** FIXME: set package path somehow */

          log.info("Launching task " + taskId.getValue)
          driver.launchTasks(List(offer.getId), List(task))
          info("Started task ID %s" format taskId)

          state.neededContainers -= 1
          state.runningTasks += taskId -> taskId
          state.unclaimedTasks -= taskId
          state.taskToTaskNames += taskId -> sspTaskNames.getJavaFriendlyType
        }
        case _ => {
          // there are no more tasks to run, so decline the offer
          log.info("Declining offer")
          driver.declineOffer(offer.getId)
        }
      }
    }
  }

  def statusUpdate(driver: SchedulerDriver, status: TaskStatus) {
    log.info("Received status update " + status)
    currentStatus = status
  }

  def frameworkMessage(driver: SchedulerDriver, executor: ExecutorID, slave: SlaveID, p4: Array[Byte]) {}

  def disconnected(driver: SchedulerDriver) {}

  def slaveLost(driver: SchedulerDriver, slave: SlaveID) {}

  def executorLost(driver: SchedulerDriver, executor: ExecutorID, slave: SlaveID, p4: Int) {}

  def error(driver: SchedulerDriver, error: String) {}

  def getCurrentStatus = TaskStatus {
    currentStatus
  }
}
