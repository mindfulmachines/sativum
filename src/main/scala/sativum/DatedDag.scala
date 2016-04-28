package sativum

import org.joda.time.LocalDate
import peapod.Task

abstract class DatedDag(dt: LocalDate) {
  lazy val name: String = this.getClass.getName

  protected val sativum: Sativum
  var endpoints: List[SativumPea[_]] = Nil
  def endpoint (t: Task[_] with DatedTask) = {
    endpoints = endpoints :+ sativum.pea(t)
  }
  def run() {
    endpoints.par.map(_.get())
  }
  def view(): String = {
    peapod.Util.teachingmachinesDotLink(sativum.dotFormatDiagram())
  }
}