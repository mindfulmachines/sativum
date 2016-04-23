package sativum

import org.apache.spark.SparkContext
import peapod.{Pea, Task, Peapod}

import scala.reflect.ClassTag
import collection.JavaConversions._

/**
  * Created by marcin.mejran on 4/15/16.
  */
class Sativum(path: String, raw: String)(implicit sc: SparkContext) extends Peapod(path,raw) {
  override def pea[D: ClassTag](d: Task[D]): Pea[D] = this.synchronized {
    val f= peas.getOrElseUpdate(
      d.name,
      new SativumPea(d)
    ).asInstanceOf[Pea[D]]
    f
  }
}
