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

import org.apache.mesos.Protos.{Offer, TaskInfo}
import org.apache.samza.job.mesos.constraints.SchedulingConstraint

class ConstraintManager {
  var constraintList: Option[List[SchedulingConstraint]]

  def satisifiesAll(offers: java.util.Collection[Offer],
                    tasks: java.util.Collection[TaskInfo]): Boolean = {
    this.satisifiesAll(constraintList.getOrElse(Nil), offers, tasks)
  }

  def satisifiesAll(constraints: List[SchedulingConstraint],
                    offers: java.util.Collection[Offer],
                    tasks: java.util.Collection[TaskInfo]): Boolean = {
    constraints.map(_.satisfied(offers, tasks)).reduce(and)
  }

  def addConstraints(constraint: SchedulingConstraint): SchedulingConstraint = {
    constraintList = Option(constraintList.getOrElse(Nil) ::: constraint)
    this
  }
}