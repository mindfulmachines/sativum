package sativum

import org.apache.spark.SparkContext
import peapod.{Pea, Task, Peapod}

import scala.reflect.ClassTag
import collection.JavaConversions._

class Sativum(path: String, raw: String)(implicit sc: SparkContext) extends Peapod(path,raw) {

  /**
    * Returns back if all peas in this pod are ready, this would only be false in the case of Sensor Tasks
    *
    */
  def ready(): Boolean = {
    peas.values().forall{
      case s: SativumPea => s.ready()
      case _ => true
    }
  }
  override def pea[D: ClassTag](t: Task[D]): Pea[D] = this.synchronized {
    val f= peas.getOrElseUpdate(
      t.name,
      new SativumPea(t)
    ).asInstanceOf[Pea[D]]
    f
  }
}
