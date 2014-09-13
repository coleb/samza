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

import org.apache.mesos.Protos.{TaskInfoOrBuilder, Offer, TaskInfo}
import org.apache.samza.job.mesos.constraints.SchedulingConstraint

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/** The constraint manager holds the state of all scheduling constraints, such
  * as when enough offers are available to launch the entire job, and then
  * calculates a response when necessary by computing the resource requirements
  * in parallel. Each scheduling constraint is responsible for its own timeout.
  */
class ConstraintManager {
  var constraintList = List[SchedulingConstraint]()

  def satisfiesAll(offers: java.util.Collection[Offer],
                   tasks: java.util.Collection[TaskInfoOrBuilder]): Boolean = {
    this.satisfiesAll(constraintList, offers, tasks)
  }

  def satisfiesAll(constraints: List[SchedulingConstraint],
                   offers: java.util.Collection[Offer],
                   tasks: java.util.Collection[TaskInfoOrBuilder]): Boolean = {
    var success: Boolean = false
    Future.reduce(constraints.map(_.satisfied(offers, tasks)))(_ && _).onComplete {
      case Success(value) => success = value
      case Failure(e) => {
        e.printStackTrace
        success = false
      }
    }
    success
  }

  def addConstraints(constraints: List[SchedulingConstraint]): ConstraintManager = {
    constraintList = constraintList ++ constraints
    this
  }

  def addConstraint(constraint: SchedulingConstraint): ConstraintManager = {
    constraintList = constraintList :+ constraint
    this
  }
}