package sativum

import org.apache.hadoop.fs.Path
import peapod.Task

trait Sensor {
  def ready(): Boolean
}

trait FileSensor {
  self: Task =>
  def ready(): Boolean = {
    val path = new Path(source)
    path.getFileSystem(p.sc.hadoopConfiguration).globStatus(path).nonEmpty
  }
  def source: String
}

