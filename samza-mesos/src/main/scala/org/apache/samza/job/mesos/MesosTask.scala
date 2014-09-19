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
import org.apache.samza.container.TaskNamesToSystemStreamPartitions
import org.apache.samza.job.{CommandBuilder, ShellCommandBuilder}

import org.apache.samza.config.Config
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config.MesosConfig
import org.apache.samza.config.MesosConfig.Config2Mesos
import org.apache.samza.job.CommandBuilder
import org.apache.samza.job.ShellCommandBuilder
import org.apache.samza.util.Util

import scala.collection.JavaConversions._

class MesosTask(config: Config,
                state: SamzaSchedulerState,
                val samzaTaskId: Int) {

  def getSamzaTaskName: String ={
    "samza-executor-%s" format samzaTaskId.toString
  }

  def getSamzaCommandBuilder: CommandBuilder = {
    val sspTaskNames: TaskNamesToSystemStreamPartitions = state.samzaTaskIDToSSPTaskNames
      .getOrElse(samzaTaskId, TaskNamesToSystemStreamPartitions())

    val cmdBuilderClassName = config.getCommandClass.getOrElse(classOf[ShellCommandBuilder].getName)
    Class.forName(cmdBuilderClassName).newInstance.asInstanceOf[CommandBuilder]
      .setConfig(config)
      .setName(getSamzaTaskName)
      .setTaskNameToSystemStreamPartitionsMapping(sspTaskNames.getJavaFriendlyType)
      .setTaskNameToChangeLogPartitionMapping(state.taskNameToChangeLogPartitionMapping.map(kv => kv._1 -> Integer.valueOf(kv._2)))
  }

  def getBuiltMesosCommandInfoURI: CommandInfo.URI = {
    val packagePath = config.getPackagePath.get
    CommandInfo.URI.newBuilder()
      .setValue(packagePath)
      .setExtract(true)
      .build()
  }

  def getBuiltMesosEnvironment(envMap: util.Map[String, String]): Environment = {
    val mesosEnvironmentBuilder: Environment.Builder = Environment.newBuilder()
    envMap.foreach(kv => {
      mesosEnvironmentBuilder.addVariables(
        Environment.Variable.newBuilder()
          .setName(kv._1)
          .setValue(kv._2)
          .build()
      )
    })
    mesosEnvironmentBuilder.build()
  }

  def getBuiltMesosTaskID: TaskID = {
    TaskID.newBuilder()
      .setValue(samzaTaskId.toString)
      .build()
  }

  def getBuiltMesosCommandInfo: CommandInfo = {
    val samzaCommandBuilder = getSamzaCommandBuilder
    CommandInfo.newBuilder()
      .addUris(getBuiltMesosCommandInfoURI)
      .setValue(samzaCommandBuilder.buildCommand())
      .setEnvironment(getBuiltMesosEnvironment(samzaCommandBuilder.buildEnvironment()))
      .build()
  }

  def getBuiltMesosTaskInfo(slaveId: SlaveID): TaskInfo = {
    TaskInfo.newBuilder()
      .setTaskId(getBuiltMesosTaskID)
      .setSlaveId(slaveId)
      .setName(getSamzaTaskName)
      .setCommand(getBuiltMesosCommandInfo)
      .addResources(Resource.newBuilder
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(1))
      .build())
      .addResources(Resource.newBuilder
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(1024))
      .build())
      .build()
  }
}

