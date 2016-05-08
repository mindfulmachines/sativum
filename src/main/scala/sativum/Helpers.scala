package sativum

/**
  * Created by marcin.mejran on 5/6/16.
  */
object Helpers {
  def isDated(a: Any) = {
    a.isInstanceOf[DatedTask]
  }
  def isSensor(a: Any) = {
    a.isInstanceOf[Sensor]
  }
}
