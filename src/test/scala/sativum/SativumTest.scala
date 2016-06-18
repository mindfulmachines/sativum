package sativum

import generic.PeapodGenerator
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite
import peapod.{EphemeralTask, Peapod}


class SativumTest extends FunSuite {
  class Test(implicit val p: Peapod) extends EphemeralTask[Double] {
    override protected def generate: Double = 1.0
  }

  test("NoSensor") {
    implicit val p = PeapodGenerator.sativum()
    val t = new Test()
    val pea = new SativumPea(t)
    assert(pea.ready())
  }

}
