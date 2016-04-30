package sativum

import org.joda.time.LocalDate
import peapod.Task

abstract class Dag {
  lazy val name: String = this.getClass.getName

  protected val sativum: Sativum
  var endpoints: List[SativumPea[_]] = Nil
  def endpoint (t: Task[_]) = {
    endpoints = endpoints :+ sativum.pea(t)
  }
  def run() {
    while(! sativum.ready()) {
      Thread.sleep(60000)
    }
    endpoints.par.map(_.get())
  }
  def view(): String = {
    peapod.Util.teachingmachinesDotLink(sativum.dotFormatDiagram())
  }
}
