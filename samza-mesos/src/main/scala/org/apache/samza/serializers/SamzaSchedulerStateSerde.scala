package org.apache.samza.serializers

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

import java.util

import org.apache.samza.job.mesos.SamzaSchedulerState
import org.apache.samza.util.Logging
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.JavaConversions._

/**
 * Write out the SamzaSchedulerState object in JSON.
 */
class SamzaSchedulerStateSerde extends Serde[SamzaSchedulerState] with Logging {
  val jsonMapper = new ObjectMapper()

  def fromBytes(bytes: Array[Byte]): SamzaSchedulerState = {
    try {
      val jMap = jsonMapper.readValue(bytes, classOf[util.HashMap[String, util.HashMap[String, String]]])
      return new SamzaSchedulerState
    } catch {
      case e: Exception =>
        warn("Exception while deserializing scheduler state: " + e)
        debug("Exception detail:", e)
        null
    }
  }

  def toBytes(schedulerState: SamzaSchedulerState): Array[Byte] = {
    val asMap = new util.HashMap[String, String]()
    jsonMapper.writeValueAsBytes(asMap)
  }
}
