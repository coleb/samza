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

package org.apache.samza.job.mesos.constraints

import org.apache.mesos.Protos.{Offer, OfferID, TaskInfo}

import scala.concurrent.Future

abstract class SchedulingConstraint() {
  val offers: java.util.Collection[Offer]
  val tasks: java.util.Collection[TaskInfo]

  /**
   * Determine if the specified set of offers satisfies the constraint requirements.
   */
  def satisfied(offers: java.util.Collection[Offer],
                tasks: java.util.Collection[TaskInfo]): Future[Boolean]

  /**
   * Determine this constraint's preference for mapping tasks to offers, if any.
   */
  def mappingPreference(): Future[Option[(List[OfferID], List[TaskInfo])]] = future {
    None
  }
}