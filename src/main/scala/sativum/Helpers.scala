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

  def isCondition(a: Any) = {
    a.isInstanceOf[Condition]
  }

  def isConditional(a: Any) = {
    a.isInstanceOf[Conditional]
  }
}
