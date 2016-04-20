package sativum

import org.joda.time.{Days, LocalDate}

/**
  * Created by marcin.mejran on 4/19/16.
  */
object Implicits {
  implicit class RichLocalDate(val self: LocalDate) {
    def until(end: LocalDate): Seq[LocalDate] = {
      val daysCount = Days.daysBetween(self, end).getDays
      (0 until daysCount).map(self.plusDays)
    }
  }

  implicit class RichLocalDateString(self: String)
    extends RichLocalDate(LocalDate.parse(self)) {
    def until(end: String) = super.until(LocalDate.parse(end))

  }

}
