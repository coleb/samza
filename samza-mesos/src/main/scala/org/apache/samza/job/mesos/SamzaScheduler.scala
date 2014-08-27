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

import org.apache.mesos.Protos.Environment._
import org.apache.mesos.Protos._
import org.apache.mesos.{Scheduler, SchedulerDriver}
import org.apache.samza.job.{CommandBuilder, ShellCommandBuilder}
import org.apache.samza.config.Config
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config.MesosConfig
import org.apache.samza.config.MesosConfig.Config2Mesos
import org.apache.samza.util.Util

import scala.collection.JavaConversions._

import org.apache.samza.util.Logging
import org.apache.samza.container.TaskNamesToSystemStreamPartitions

class SamzaScheduler(config: Config, state: SamzaSchedulerState) extends Scheduler with Logging {
  var currentState = TaskState.TASK_STARTING

  state.taskCount = config.getTaskCount match {
    case Some(count) => count
    case None =>
      info("No %s specified. Defaulting to one container." format MesosConfig.EXECUTOR_TASK_COUNT)
      1
  }

  val tasksToSSPTaskNames: Map[Int, TaskNamesToSystemStreamPartitions] = Util.assignContainerToSSPTaskNames(config, state.taskCount)
  val taskNameToChangeLogPartitionMapping = Util.getTaskNameToChangeLogPartitionMapping(config, tasksToSSPTaskNames)

  state.neededExecutors = state.taskCount
  state.unclaimedTasks = (0 until state.taskCount).toSet

  info("Awaiting offers for %s executors" format state.taskCount)

  def registered(driver: SchedulerDriver, framework: FrameworkID, master: MasterInfo) {
      info("Framework registered")
  }

  def reregistered(driver: SchedulerDriver, master: MasterInfo): Unit = {
    info("Framework re-registered")
  }

  def offerRescinded(driver: SchedulerDriver, offer: OfferID): Unit = {
    info("An offer was rescinded")
  }

  def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]) {
    for (offer <- offers) {
      info("Received offer " + offer)

      state.unclaimedTasks.headOption match {
        case Some(taskId) => {
          info("Got available task id (%d) for offer: %s" format(taskId, offer))

          val cpuResource = Resource.newBuilder
            .setName("cpus")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder().setValue(1))

          val diskResource = Resource.newBuilder
            .setName("disk")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder().setValue(4192))

          val memResource = Resource.newBuilder
            .setName("mem")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder().setValue(1024))

          val packagePath = config.getPackagePath.get
          info("Starting task ID %s using package path %s" format (taskId, packagePath))

          val uriCommandInfo = CommandInfo.URI.newBuilder()
            .setValue(packagePath)
            .setExtract(true)
            .build()

          val sspTaskNames: TaskNamesToSystemStreamPartitions = tasksToSSPTaskNames.getOrElse(taskId, TaskNamesToSystemStreamPartitions())
          info("Claimed SSP taskNames %s for offer ID %s" format(sspTaskNames, taskId))

          val cmdBuilderClassName = config.getCommandClass.getOrElse(classOf[ShellCommandBuilder].getName)
          val cmdBuilder = Class.forName(cmdBuilderClassName).newInstance.asInstanceOf[CommandBuilder]
            .setConfig(config)
            .setName("samza-executor-%s" format taskId.toString)
            .setTaskNameToSystemStreamPartitionsMapping(sspTaskNames.getJavaFriendlyType)
            .setTaskNameToChangeLogPartitionMapping(taskNameToChangeLogPartitionMapping.map(kv => kv._1 -> Integer.valueOf(kv._2)))

          val env = cmdBuilder.buildEnvironment.map { case (k, v) => (k, Util.envVarEscape(v)) }
          info("Task ID %s using env %s" format (taskId, env))

          val envInfo = {
            val builder = Environment.newBuilder()
            for ((key, value) <- env) {
              val variable = Variable.newBuilder().setName(key).setValue(value)
              builder.addVariables(variable)
            }
            builder.build()
          }

          val basename = "/home/jbringhu/samza-dev/hello-samza/deploy/samza"
          val command = "cd %s*; %s".format(basename, cmdBuilder.buildCommand)
          info("Task ID %s using command %s" format(taskId, command))

          val commandInfo = CommandInfo.newBuilder
            .setEnvironment(envInfo)
            .addUris(uriCommandInfo)
            .setValue(command)

          val task = TaskInfo.newBuilder
            .setName("samza-executor-%s" format taskId)
            .setTaskId(TaskID.newBuilder().setValue(taskId.toString).build())
            .setSlaveId(offer.getSlaveId)
            .addResources(cpuResource)
            .addResources(diskResource)
            .addResources(memResource)
            .setCommand(commandInfo)
            .build

          /** FIXME: set package path somehow? */

          info("Launching task " + taskId)
          driver.launchTasks(util.Arrays.asList(offer.getId), util.Arrays.asList(task))
          info("Started task ID %s" format taskId)

          state.neededExecutors -= 1
          state.runningTasks += taskId -> offer
          state.unclaimedTasks -= taskId
          state.taskToTaskNames += taskId -> sspTaskNames.getJavaFriendlyType
        }
        case _ => {
          // there are no more tasks to run, so decline the offer
          info("Declining offer")
          driver.declineOffer(offer.getId)
        }
      }
    }
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {
    info("Status update for Task %s: %s".format(status.getTaskId, status.getState))
    currentState = status.getState
  }

  def frameworkMessage(driver: SchedulerDriver, executor: ExecutorID, slave: SlaveID, data: Array[Byte]): Unit = {
    info("A framework message was received.")
  }

  def disconnected(driver: SchedulerDriver): Unit = {
    info("Framework has been disconnected")
  }

  def slaveLost(driver: SchedulerDriver, slave: SlaveID): Unit = {
    info("A slave has been lost")
  }

  def executorLost(driver: SchedulerDriver, executor: ExecutorID, slave: SlaveID, status: Int): Unit = {
    info("An executor has been lost.")
  }

  def error(driver: SchedulerDriver, error: String) {
    info("Error reported: %s" format error)
  }
}
