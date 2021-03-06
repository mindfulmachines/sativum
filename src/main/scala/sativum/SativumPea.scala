package sativum

import peapod.Logging
import peapod.{Task, Pea}

import scala.reflect.ClassTag

class SativumPea[+D: ClassTag](task: Task[D]) extends Pea[D](task) with Logging {
  def ready(): Boolean = {
    task match {
      case s: Sensor => s.ready()
      case _ => true
    }
  }

  override def buildCache(): Unit =  {
    task match {
      case s: Condition =>
        if(s.condition()) {
          super.buildCache()
        }
      case _ =>
        super.buildCache()
    }

  }
}
