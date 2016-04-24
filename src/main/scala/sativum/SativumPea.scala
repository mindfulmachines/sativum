package sativum

import org.apache.spark.Logging
import peapod.{Task, Pea}

import scala.reflect.ClassTag
import scala.tools.nsc.interpreter.Logger

class SativumPea[+D: ClassTag](task: Task[D]) extends Pea[D](task) with Logging {
  override lazy val recursiveVersion: List[String] = {
    Nil
  }
  override def recursiveVersionShort: String = {
    "latest"
  }

  def ready(): Boolean = {
    task match {
      case s: Sensor => s.ready()
      case _ => true
    }
  }
}
