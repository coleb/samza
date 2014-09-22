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

import scala.collection.JavaConverters._

class SamzaScheduler(config: Config,
                     state: SamzaSchedulerState,
                     constraintManager: ConstraintManager) extends Scheduler with Logging {

  info("Mesos scheduler created.")

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
    state.offerPool ++= offers

    if (state.unclaimedTasks.size > 0) {
      if (constraintManager.satisfiesAll(state.offerPool, state.unclaimedTasks)) {
        info("Resource constraints have been satisfied.")

        info("Assigning tasks to offers.")
        val preparedTasks: util.List[TaskInfo] = new util.ArrayList[TaskInfo]
        for ((offer, task) <- (state.offerPool zip state.unclaimedTasks)) {
          preparedTasks.append(task.getBuiltMesosTaskInfo(offer.getSlaveId))
        }

        info("Launching Samza tasks on Mesos offers.")
        val status = driver.launchTasks(state.offerPool.map(_.getId), preparedTasks)

        state.offerPool = Set()
        state.runningTasks = state.unclaimedTasks
        state.unclaimedTasks = Set()

        info("Result of job launch is %s".format(status))

      } else {
        info("Resource constraints have not been satisfied, awaiting offers.")
      }
    } else {
      /* Decline offers we don't plan on using. */
      offers.foreach(offer => driver.declineOffer(offer.getId))
    }
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus) {
    info("Status update for Task %s: %s".format(status.getTaskId, status.getState))
  }

  def frameworkMessage(driver: SchedulerDriver,
                       executor: ExecutorID,
                       slave: SlaveID,
                       data: Array[Byte]): Unit = {
    info("A framework message was received.")
  }

  def disconnected(driver: SchedulerDriver): Unit = {
    info("Framework has been disconnected")
  }

  def slaveLost(driver: SchedulerDriver, slave: SlaveID): Unit = {
    info("A slave has been lost")
  }

  def executorLost(driver: SchedulerDriver,
                   executor: ExecutorID,
                   slave: SlaveID,
                   status: Int): Unit = {
    info("An executor has been lost.")
  }

  def error(driver: SchedulerDriver, error: String) {
    info("Error reported: %s" format error)
  }
}
