package sativum

import org.joda.time.LocalDate
import org.scalatest.FunSuite

class ImplicitsTest extends FunSuite {
  test("LocalDateImplicits") {
    import Implicits._
    assert(
      new LocalDate("2016-01-01").until(new LocalDate("2016-01-03")) ==
        new LocalDate("2016-01-01") :: new LocalDate("2016-01-02") :: Nil
    )
    assert(
      "2016-01-01".until("2016-01-03") ==
        new LocalDate("2016-01-01") :: new LocalDate("2016-01-02") :: Nil
    )
  }
}
