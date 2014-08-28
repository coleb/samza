package org.apache.samza.job.mesos.constraints

import org.apache.mesos.Protos.{Attribute, TaskInfo, Offer}

import scala.collection.JavaConversions._
import scala.concurrent.Future

class CategoricalConstraint(offers: java.util.Collection[Offer],
                          tasks: java.util.Collection[TaskInfo]) extends SchedulingConstraint {
  val name: Option[String]
  val value: Option[Any]

  /** Determine if an offer satisfies the constraint. */
  def offerIsSatisfied(offer: Offer): Boolean = {
    offer.getAttributesList.forall((attr: Attribute) =>
      (name, value, Option(attr.getScalar)) match {
        case (Some(constraintName), Some(constraintValue), Some(attrScalar)) => {
          constraintName == attr.getName && attrScalar.getValue == constraintValue
        }
        case _ => false
      }
    )
  }

  /** Determine if all offers satisfy the constraint. . */
  def satisfied(): Future[Boolean] = future {
    offers.forall(offerIsSatisfied(_))
  }
}