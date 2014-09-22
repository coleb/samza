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

import org.apache.mesos.MesosSchedulerDriver
import org.apache.mesos.Protos.{FrameworkID, FrameworkInfo}
import org.apache.samza.config.Config
import org.apache.samza.config.MesosConfig.Config2Mesos
import org.apache.samza.job.ApplicationStatus._
import org.apache.samza.job.mesos.constraints.{NumericalConstraint, OfferQuantityConstraint}
import org.apache.samza.job.{ApplicationStatus, StreamJob}
import org.apache.samza.util.Logging

/* A MesosJob is a wrapper for a Mesos Scheduler. */
class MesosJob(config: Config) extends StreamJob with Logging {

  val state = new SamzaSchedulerState(config)
  val frameworkInfo = getFrameworkInfo
  val constraintManager = createConstraintManager
  val scheduler = new SamzaScheduler(config, state, constraintManager)
  val driver = new MesosSchedulerDriver(scheduler, frameworkInfo,
    config.getMasterConnect.getOrElse("zk://localhost:2181/mesos"))


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

  /*
  TODO: Either use ConfigStream (SAMZA-348) or store recovery state directly to ZK.
  def getState: State = {
    new ZooKeeperState("localhost:2181", 10, TimeUnit.SECONDS, "/mesos-scheduler-state")
  }
  */

  def createConstraintManager: ConstraintManager = {
    /* TODO: generate list of constraints from config.
     */
    (new ConstraintManager)
      .addConstraint(new OfferQuantityConstraint)
      .addConstraint(new NumericalConstraint("cpus", (offer) => {
        if(config.getExecutorMaxCpuCores <= offer) {
          info("Task is satisfied by offer cpu core resources.")
          true
        } else {
          info("Task is not satisfied by offer cpu core resources.")
          false
        }
      }))
      .addConstraint(new NumericalConstraint("mem", (offer) => {
        if(config.getExecutorMaxMemoryMb <= offer) {
          info("Task is satisfied by offer memory resources.")
          true
        } else {
          info("Task is not satisfied by offer memory resources.")
          false
        }
      }))
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
