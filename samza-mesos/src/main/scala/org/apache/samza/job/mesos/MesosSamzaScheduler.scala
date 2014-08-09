import java.util

import org.apache.mesos.Protos._
import org.apache.mesos.{Scheduler, SchedulerDriver}

import scala.collection.JavaConverters._

class MesosSamzaScheduler extends Scheduler {
  def disconnected(driver: SchedulerDriver) {}

  def error(driver: SchedulerDriver, message: String) {}

  def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {}

  def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]) {}

  def offerRescinded(driver: SchedulerDriver, offerId: OfferID) {}

  def registered(driver: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo) {}

  def reregistered(driver: SchedulerDriver, masterInfo: MasterInfo) {}

  def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]) {
    for (offer <- offers.asScala) {
    }
  }

  def slaveLost(driver: SchedulerDriver, slaveId: SlaveID) {}

  def statusUpdate(driver: SchedulerDriver, status: TaskStatus) {}
}
