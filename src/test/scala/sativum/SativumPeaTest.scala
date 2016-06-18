package sativum

import generic.PeapodGenerator
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite
import peapod.{EphemeralTask, Peapod}

class SativumPeaTest extends FunSuite {
  class Test(implicit val p: Peapod) extends EphemeralTask[Double] {
    override protected def generate: Double = 1.0
  }
  class TestFalse(implicit val p: Peapod) extends EphemeralTask[Double] with Condition {
    def condition() = false
    override protected def generate: Double = 1.0
  }
  class TestTrue(implicit val p: Peapod) extends EphemeralTask[Double] with Condition {
    def condition() = true
    override protected def generate: Double = 1.0
  }
  class TestSensor(implicit val p: Peapod) extends EphemeralTask[Double] with FileSensor {
    override val source = p.path + "/file.tmp"
    override protected def generate: Double = 1.0
  }

  test("Sensor") {
    implicit val p = PeapodGenerator.sativum()
    val t = new TestSensor()
    val pea = new SativumPea(t)
    assert(!pea.ready())
    val path = new Path(p.path + "/file.tmp")
    path.getFileSystem(p.sc.hadoopConfiguration).createNewFile(path)
    assert(pea.ready())
  }

  test("NoSensor") {
    implicit val p = PeapodGenerator.sativum()
    val t = new Test()
    val pea = new SativumPea(t)
    assert(pea.ready())
  }


  test("SativumPeaCondition") {
    implicit val p = PeapodGenerator.sativum()

    val peaFalse = new SativumPea(new TestFalse())
    assert(peaFalse.cache.isEmpty)
    peaFalse.buildCache()
    assert(peaFalse.cache.isEmpty)

    val peaTrue = new SativumPea(new TestTrue())
    assert(peaTrue.cache.isEmpty)
    peaTrue.buildCache()
    assert(peaTrue.cache.nonEmpty)

    val pea = new SativumPea(new Test())
    assert(pea.cache.isEmpty)
    pea.buildCache()
    assert(pea.cache.nonEmpty)
  }

}
