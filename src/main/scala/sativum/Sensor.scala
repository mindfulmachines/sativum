package sativum

import org.apache.hadoop.fs.Path
import peapod.Task

trait Sensor {
  def ready(): Boolean
}

trait FileSensor extends Task[Any] with Sensor {
  val source: String
  def ready(): Boolean = {
    val path = new Path(source)
    path.getFileSystem(p.sc.hadoopConfiguration).globStatus(path).nonEmpty
  }
}

