package org.apache.samza.job.mesos

import java.util
import java.util.concurrent.TimeUnit

import grizzled.slf4j.Logging
import org.apache.mesos.Protos.TaskID
import org.apache.mesos.state.{State, ZooKeeperState}
import org.apache.samza.container.TaskName
import org.apache.samza.system.SystemStreamPartition

class SamzaSchedulerState(val taskId: Int, val nodeHost: String, val nodePort: Int) extends Logging {

  var state = new ZooKeeperState("localhost:2181", 10, TimeUnit.SECONDS, "/samza-mesos-test")

  // controlled by the Scheduler
  var completedTasks = 0
  var neededContainers = 0
  var failedContainers = 0
  var releasedContainers = 0
  var taskCount = 0
  var unclaimedTasks = Set[Int]()
  var finishedTasks = Set[Int]()
  var runningTasks = Map[Int, TaskID]()
  var taskToTaskNames = Map[Int, util.Map[TaskName, util.Set[SystemStreamPartition]]]()
  var status = FinalApplicationStatus.UNDEFINED

  def save: Unit = {
    // TODO, save object to state
  }

  def restore: Unit = {
    // TODO, restore object from state
  }
}