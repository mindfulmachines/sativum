package sativum

import org.apache.spark.Logging
import peapod.{Task, Pea}

import scala.reflect.ClassTag
import scala.tools.nsc.interpreter.Logger

class SativumPea[+D: ClassTag](task: Task[D]) extends Pea[D](task) with Logging {
  def ready(): Boolean = {
    task match {
      case s: Sensor => s.ready()
      case _ => true
    }
  }
  def delete(): Unit = {
    task.delete()
  }
}
