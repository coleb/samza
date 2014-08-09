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
  val CONTAINER_MAX_MEMORY_MB = "mesos.container.memory.mb"
  val CONTAINER_MAX_CPU_CORES = "mesos.container.cpu.cores"
  val CONTAINER_RETRY_COUNT = "mesos.container.retry.count"
  val CONTAINER_RETRY_WINDOW_MS = "mesos.container.retry.window.ms"
  val TASK_COUNT = "mesos.container.count"
  val AM_JVM_OPTIONS = "mesos.am.opts"
  val AM_JMX_ENABLED = "mesos.am.jmx.enabled"
  val AM_CONTAINER_MAX_MEMORY_MB = "mesos.am.container.memory.mb"
  val AM_POLL_INTERVAL_MS = "mesos.am.poll.interval.ms"

  implicit def Config2Mesos(config: Config) = new MesosConfig(config)
}

class MesosConfig(config: Config) extends ScalaMapConfig(config) {
  def getExecutorMaxMemoryMb: Option[Int] = getOption(MesosConfig.CONTAINER_MAX_MEMORY_MB).map(_.toInt)

  def getExecutorMaxCpuCores: Option[Int] = getOption(MesosConfig.CONTAINER_MAX_CPU_CORES).map(_.toInt)

  def getExecutorRetryCount: Option[Int] = getOption(MesosConfig.CONTAINER_RETRY_COUNT).map(_.toInt)

  def getExecutorRetryWindowMs: Option[Int] = getOption(MesosConfig.CONTAINER_RETRY_WINDOW_MS).map(_.toInt)

  def getPackagePath = getOption(MesosConfig.PACKAGE_PATH)

  def getTaskCount: Option[Int] = getOption(MesosConfig.TASK_COUNT).map(_.toInt)

  def getAmOpts = getOption(MesosConfig.AM_JVM_OPTIONS)

  def getSchedulerExecutorMaxMemoryMb: Option[Int] = getOption(MesosConfig.AM_CONTAINER_MAX_MEMORY_MB).map(_.toInt)

  def getSchedulerPollIntervalMs: Option[Int] = getOption(MesosConfig.AM_POLL_INTERVAL_MS).map(_.toInt)

  def getJmxServerEnabled = getBoolean(MesosConfig.AM_JMX_ENABLED, true)
}
