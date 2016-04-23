package sativum

import peapod.Task

/**
  * Created by marcin.mejran on 4/20/16.
  */
abstract class Dag {
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
