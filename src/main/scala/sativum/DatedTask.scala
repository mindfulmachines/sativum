package sativum

import org.joda.time.LocalDate
import peapod.Task

/**
  * Created by marcin.mejran on 4/1/16.
  */
trait DatedTask[D] extends Task[D] {
  self : Task[D] =>
  val partition: LocalDate
  override lazy val name: String = baseName+ "/" + partition.toString()
  override lazy val versionName = baseName
  override lazy val dir = p.path + "/" + baseName + "/" + recursiveVersionShort + "/" +
    partition.toString("yyyy/MM/dd") + "/"
}
