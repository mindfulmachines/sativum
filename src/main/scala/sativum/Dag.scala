package sativum

import peapod.Task

abstract class Dag {
  lazy val name: String = this.getClass.getName
  protected val sativum: Sativum
  var endpoints: List[Task[_]] = Nil
  protected val waitTime = 60000

  def endpoint (t: Task[_]) = {
    endpoints = endpoints :+ t
  }

  /**
    * Returns back if all peas in this Dag are ready, this would only be false in the case of Sensor Tasks
    *
    */
  def ready(): Boolean = {
    endpoints.map(sativum(_)).forall{
      case s: SativumPea[_] => s.ready()
      case _ => true
    }
  }

  def run() {
    endpoints.map(sativum(_))
    while(! ready()) {
      Thread.sleep(waitTime)
    }
    endpoints.flatMap(_.children).foreach(_.delete())
    endpoints.par.map(sativum(_).get())
  }

  def view(): String = {
    peapod.Util.teachingmachinesDotLink(sativum.dotFormatDiagram())
  }
}
