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

import org.apache.mesos.Protos.{Attribute, Offer, TaskInfo}

import scala.collection.JavaConversions._
import scala.concurrent.Future

class CategoricalConstraint() extends SchedulingConstraint {
  val name: Option[String]
  val value: Option[String]

  /** Determine if an offer satisfies the constraint. */
  def offerIsSatisfied(offer: Offer): Boolean = {
    offer.getAttributesList.forall((attr: Attribute) =>
      (name, value, Option(attr.getText)) match {
        case (Some(constraintName), Some(constraintValue), Some(attrText)) => {
          constraintName == attr.getName && attrText.getValue == constraintValue
        }
        case _ => false
      }
    )
  }

  /** Determine if all offers satisfy the constraint. . */
  def satisfied(offers: java.util.Collection[Offer],
                tasks: java.util.Collection[TaskInfo]): Future[Boolean] = future {
    offers.forall(offerIsSatisfied(_))
  }
}