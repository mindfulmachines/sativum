package sativum
import peapod.Task
/**
  * Created by marcin.mejran on 4/15/16.
  */
trait SourceType[T] {
  self: Task[T] =>
  val source: String
}
