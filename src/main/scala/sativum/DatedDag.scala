package sativum

import org.joda.time.LocalDate

abstract class DatedDag(_dt: String) extends Dag {
  val dt = new LocalDate(_dt)
  def runDated() {
    endpoints.map(sativum(_))
    while (!ready()) {
      Thread.sleep(waitTime)
    }
    endpoints.flatMap(_.parents).foreach {
      case d: DatedTask => d.delete()
      case _ =>
    }
    endpoints.par.map(sativum(_).get())
  }
}
