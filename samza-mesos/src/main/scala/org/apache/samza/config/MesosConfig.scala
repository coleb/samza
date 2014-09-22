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

package org.apache.samza.config

object MesosConfig {
  // mesos scheduler config
  val PACKAGE_PATH = "mesos.package.path"
  val MASTER_CONNECT = "mesos.master.connect"

  val EXECUTOR_MAX_MEMORY_MB = "mesos.executor.memory.mb"
  val EXECUTOR_MAX_CPU_CORES = "mesos.executor.cpu.cores"
  val EXECUTOR_MAX_DISK_MB = "mesos.executor.disk.mb"
  val EXECUTOR_TASK_COUNT = "mesos.executor.count"

  val SCHEDULER_JMX_ENABLED = "mesos.scheduler.jmx.enabled"
  val SCHEDULER_FAILOVER_TIMEOUT = "mesos.scheduler.failover.timeout"

  implicit def Config2Mesos(config: Config) = new MesosConfig(config)
}

class MesosConfig(config: Config) extends JobConfig(config) {
  def getExecutorMaxMemoryMb: Int = getOption(MesosConfig.EXECUTOR_MAX_MEMORY_MB).map(_.toInt).getOrElse(1024)

  def getExecutorMaxCpuCores: Int = getOption(MesosConfig.EXECUTOR_MAX_CPU_CORES).map(_.toInt).getOrElse(1)

  def getPackagePath = getOption(MesosConfig.PACKAGE_PATH)

  def getTaskCount: Option[Int] = getOption(MesosConfig.EXECUTOR_TASK_COUNT).map(_.toInt)

  def getJmxServerEnabled = getBoolean(MesosConfig.SCHEDULER_JMX_ENABLED, true)

  def getMasterConnect = getOption(MesosConfig.MASTER_CONNECT)
}
