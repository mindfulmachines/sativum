package sativum

import peapod.{Task, Pea}

import scala.reflect.ClassTag

/**
  * Created by marcin.mejran on 4/15/16.
  */
class SativumPea[+D: ClassTag](task: Task[D]) extends Pea[D](task) {
  override lazy val recursiveVersion: List[String] = {
    Nil
  }
  override def recursiveVersionShort: String = {
    ""
  }
}
