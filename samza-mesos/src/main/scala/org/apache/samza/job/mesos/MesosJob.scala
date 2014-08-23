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
import org.apache.samza.config.{JobConfig, MesosConfig, Config}
import org.apache.samza.job.ApplicationStatus._
import org.apache.samza.job.{ApplicationStatus, StreamJob}

/* A MesosJob is a wrapper for a Mesos Scheduler. */
class MesosJob(config: Config) extends StreamJob {

  val state = new SamzaSchedulerState()
  val frameworkInfo = getFrameworkInfo
  val scheduler = new SamzaScheduler(config, state)
  val driver = new MesosSchedulerDriver(scheduler, frameworkInfo, "zk://localhost:2181/mesos")

  def getStatus: ApplicationStatus = {
    scheduler.currentState match {
      case TaskState.TASK_FAILED => ApplicationStatus.UnsuccessfulFinish
      case TaskState.TASK_FINISHED => ApplicationStatus.SuccessfulFinish
      case TaskState.TASK_KILLED => ApplicationStatus.UnsuccessfulFinish
      case TaskState.TASK_LOST => ApplicationStatus.UnsuccessfulFinish
      case TaskState.TASK_RUNNING => ApplicationStatus.Running
      case TaskState.TASK_STAGING => ApplicationStatus.New
      case TaskState.TASK_STARTING => ApplicationStatus.New
    }
  }

  def getFrameworkInfo: FrameworkInfo = {
    val frameworkName = config.asInstanceOf[JobConfig].getName.get
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
    new ZooKeeperState("localhost:2181", 10, TimeUnit.SECONDS, "/samza-mesos-test")
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