package sativum

import org.joda.time.LocalDate
import peapod.Task

abstract class Dag(dt: LocalDate) {
  val sativum: Sativum
  var endpoints: List[Task[_]] = Nil
  def endpoint (t: Task[_]) = {
    endpoints = endpoints :+ t
  }
  def run() {
    endpoints.par.map(sativum(_).get())
  }
  def view(): String = {
    endpoints.par.map(sativum(_))
    peapod.Util.teachingmachinesDotLink(sativum.dotFormatDiagram())
  }
}
