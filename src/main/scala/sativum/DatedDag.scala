package sativum

import org.joda.time.LocalDate
import peapod.Task

abstract class DatedDag(_dt: String) extends Dag {
  val dt = new LocalDate(_dt)
  def runDated() {
    while (!sativum.ready()) {
      Thread.sleep(60000)
    }
    endpoints.flatMap(_.children).foreach {
      case d: DatedTask => d.delete()
      case _ =>
    }
    endpoints.par.map(sativum.pea(_).get())
  }
}
