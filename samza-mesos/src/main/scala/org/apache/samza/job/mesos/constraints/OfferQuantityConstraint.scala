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


import org.apache.mesos.Protos.{Offer, TaskInfoOrBuilder}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * The offer quantity constraint will hold offers until the specified number of offers have been reached. For simple
 * use cases, it can be used to wait until all tasks have a matching offer. In more complex cases, it may be useful
 * to hold a percentage of offers above the number need to launch all tasks. This allows custom chained optimizing
 * constraints to have a reasonable set of resources to optimize over.
 */

class OfferQuantityConstraint(offers: java.util.Collection[Offer],
                              tasks: java.util.Collection[TaskInfoOrBuilder]) extends SchedulingConstraint(offers, tasks) {

  /** Determine if all offers satisfy the constraint. . */
  def satisfied(offers: java.util.Collection[Offer],
                tasks: java.util.Collection[TaskInfoOrBuilder]): Future[Boolean] = Future {
    if (tasks.size >= offers.size()) true else false
  }
}