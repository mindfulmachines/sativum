package sativum

import peapod.{EphemeralTask, Peapod, Storable}

import scala.reflect.ClassTag

/**
  * Helper class for a Task that reads a remote file source, implements a Sensor and uses the StorableTask
  * serializers to read the file from disk. Useful for cross-dag data transfers and dependencies.
  */
class RemoteTask[D: ClassTag](val location: String)(implicit val p: Peapod, c: D => Storable[D])
  extends EphemeralTask[D] with FileSensor {
  override val source: String = location

  protected def read(): D = {
    c(null.asInstanceOf[D])
      .readStorable(p,location)
  }

  override protected def generate: D = {
    read()
  }
}
