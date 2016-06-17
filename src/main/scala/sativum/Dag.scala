package sativum

import peapod.Task

abstract class Dag {
  lazy val name: String = this.getClass.getName
  protected val sativum: Sativum
  var endpoints: List[Task[_]] = Nil

  def endpoint (t: Task[_]) = {
    endpoints = endpoints :+ t
  }

  def run() {
    while(! sativum.ready()) {
      Thread.sleep(60000)
    }
    endpoints.flatMap(_.children).foreach(_.delete())
    endpoints.par.map(sativum.pea(_).get())
  }

  def view(): String = {
    peapod.Util.teachingmachinesDotLink(sativum.dotFormatDiagram())
  }
}
