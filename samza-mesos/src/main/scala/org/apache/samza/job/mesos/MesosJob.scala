import mesosphere.mesos.util.FrameworkInfo
import org.apache.mesos.MesosSchedulerDriver
import org.apache.samza.config.Config
import org.apache.samza.job.StreamJob


class MesosSamzaJob(config: Config) extends StreamJob {
  val framework = FrameworkInfo("SamzaMesos")
  val scheduler = new MesosSamzaScheduler
  val driver = new MesosSchedulerDriver(scheduler, framework.toProto, "zk://localhost:2181/mesos")

  driver.run()
}