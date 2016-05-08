package sativum

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import peapod.{Pea, Peapod, Task}

import scala.reflect.ClassTag
import collection.JavaConversions._

class Sativum(path: String,
              raw: String,
              conf: Config = ConfigFactory.empty())
             (_sc: => SparkContext)
  extends Peapod(path,raw,conf)(_sc) {

  override val recursiveVersioning = false

  /**
    * Returns back if all peas in this pod are ready, this would only be false in the case of Sensor Tasks
    *
    */
  def ready(): Boolean = {
    peas.values().forall{
      case s: SativumPea[_] => s.ready()
      case _ => true
    }
  }
  override def pea[D: ClassTag](t: Task[D]): SativumPea[D] = this.synchronized {
    val f= peas.getOrElseUpdate(
      t.name,
      new SativumPea(t)
    ).asInstanceOf[SativumPea[D]]
    f
  }
  def pea(t: Task[_]): Pea[_] = this.synchronized {
    val f= peas.getOrElseUpdate(
      t.name,
      new SativumPea(t)
    )
    f
  }
}
