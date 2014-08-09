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

import mesosphere.mesos.util.FrameworkInfo
import org.apache.mesos.MesosSchedulerDriver
import org.apache.samza.config.Config
import org.apache.samza.job.StreamJob

/* A MesosJob is a wrapper for a Mesos Scheduler. */
class MesosJob(config: Config) extends StreamJob {
  val framework = FrameworkInfo("SamzaMesos")
  val scheduler = new MesosSamzaScheduler
  val driver = new MesosSchedulerDriver(scheduler, framework.toProto, "zk://localhost:2181/mesos")

  driver.run()
}