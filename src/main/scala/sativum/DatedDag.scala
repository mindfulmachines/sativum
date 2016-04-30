package sativum

import org.joda.time.LocalDate
import peapod.Task

abstract class DatedDag(_dt: String) extends Dag {
  val dt = new LocalDate(_dt)
}
