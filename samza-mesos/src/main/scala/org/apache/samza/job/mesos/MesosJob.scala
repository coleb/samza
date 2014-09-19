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

import java.util.concurrent.TimeUnit

import org.apache.mesos.Protos.{Status, TaskState, FrameworkInfo, FrameworkID}
import org.apache.mesos.MesosSchedulerDriver
import org.apache.mesos.state.{State, ZooKeeperState}
import org.apache.samza.job.ApplicationStatus._
import org.apache.samza.job.mesos.constraints.OfferQuantityConstraint
import org.apache.samza.job.{ApplicationStatus, StreamJob}
import org.apache.samza.config.Config
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config.MesosConfig
import org.apache.samza.config.MesosConfig.Config2Mesos

/* A MesosJob is a wrapper for a Mesos Scheduler. */
class MesosJob(config: Config) extends StreamJob {

  val state = new SamzaSchedulerState(config)
  val frameworkInfo = getFrameworkInfo
  val constraintManager = getConstraintManager
  val scheduler = new SamzaScheduler(config, state, constraintManager)
  val driver = new MesosSchedulerDriver(scheduler, frameworkInfo, "zk://localhost:2181/mesos")

  def getStatus: ApplicationStatus = {
    state.currentStatus
  }

  def getFrameworkInfo: FrameworkInfo = {
    val frameworkName = config.getName.get
    val frameworkId = FrameworkID.newBuilder
      .setValue(frameworkName)
      .build
    FrameworkInfo.newBuilder
      .setName(frameworkName)
      .setId(frameworkId)
      .setUser("") // Let Mesos assign the user
      .setFailoverTimeout(60.0) // Allow a 60 second window for failover
      .build
  }

  def getState: State = {
    new ZooKeeperState("localhost:2181", 10, TimeUnit.SECONDS, "/mesos")
  }

  def getConstraintManager: ConstraintManager = {
    /* TODO: generate list of constraints from config.
     *
     * For now, just make sure we have enough offers so we can launch the
     * entire job at once.
     */
    (new ConstraintManager).addConstraint(new OfferQuantityConstraint)
  }

  def kill: StreamJob = {
    driver.stop
    this
  }

  def submit: StreamJob = {
    driver.run
    this
  }

  def waitForFinish(timeoutMs: Long): ApplicationStatus = {
    val startTimeMs = System.currentTimeMillis()

    while (System.currentTimeMillis() - startTimeMs < timeoutMs) {
      Option(getStatus) match {
        case Some(s) => if (SuccessfulFinish.equals(s) || UnsuccessfulFinish.equals(s)) return s
        case None => null
      }

      Thread.sleep(1000)
    }

    Running
  }

  def waitForStatus(status: ApplicationStatus, timeoutMs: Long): ApplicationStatus = {
    val startTimeMs = System.currentTimeMillis()

    while (System.currentTimeMillis() - startTimeMs < timeoutMs) {
      Option(getStatus) match {
        case Some(s) => if (status.equals(s)) return status
        case None => null
      }

      Thread.sleep(1000)
    }

    Running
  }
}
