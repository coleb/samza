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

import org.apache.mesos.Protos.Offer
import org.apache.mesos.state.ZooKeeperState
import org.apache.samza.container.TaskName
import org.apache.samza.job.ApplicationStatus._
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.Logging

class SamzaSchedulerState() extends Logging {

  var state = new ZooKeeperState("localhost:2181", 10, TimeUnit.SECONDS, "/samza-mesos-test")

  // controlled by the Scheduler
  var completedTasks = 0
  var neededExecutors = 0
  var failedExecutors = 0
  var releasedExecutors = 0
  var taskCount = 0
  var unclaimedTasks = Set[Int]()
  var finishedTasks = Set[Int]()
  var runningTasks = Map[Int, Offer]()
  var taskToTaskNames = Map[Int, util.Map[TaskName, util.Set[SystemStreamPartition]]]()
  var status = New
}