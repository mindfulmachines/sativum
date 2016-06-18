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
    val result = path.getFileSystem(p.sc.hadoopConfiguration).globStatus(path)
    if(result != null && result.nonEmpty) {
      true
    } else {
      false
    }
  }
}

trait FilesSensor extends Task[Any] with Sensor {
  val sources: List[String]
  def ready(): Boolean = {
    sources.exists { source =>
      val path = new Path(source)
      val result = path.getFileSystem(p.sc.hadoopConfiguration).globStatus(path)
      if(result != null && result.nonEmpty) {
        true
      } else {
        false
      }
    }
  }
}