package sativum

import peapod.{EphemeralTask, Peapod, Storable, Task}

import scala.reflect.ClassTag

/**
  * Created by marcin.mejran on 5/1/16.
  */
class RemoteTask[D: ClassTag](val location: String)(implicit val p: Peapod, c: D => Storable[D]) extends EphemeralTask[D] with FileSensor {
  override val source: String = location

  protected def read(): D = {
    c(null.asInstanceOf[D])
      .readStorable(p,location)
  }

  override protected def generate: D = {
    read()
  }
}
