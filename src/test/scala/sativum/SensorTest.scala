package sativum

import java.io.{File, FileOutputStream}

import generic.PeapodGenerator
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite
import peapod.{EphemeralTask, Peapod}

class SensorTest extends FunSuite {
  class Test(implicit val p: Peapod) extends EphemeralTask[Double] with FileSensor {
    override val source = p.path + "/file.tmp"
    override protected def generate: Double = 1.0
  }

  class TestGlob(implicit val p: Peapod) extends EphemeralTask[Double] with FileSensor {
    override val source = p.path + "/fil*.tmp"
    override protected def generate: Double = 1.0
  }

  class TestMultiple(implicit val p: Peapod) extends EphemeralTask[Double] with FilesSensor {
    override val sources = (p.path + "/file.tmp") :: (p.path + "/file2.tmp") :: Nil
    override protected def generate: Double = 1.0
  }


  test("SingleFile") {
    implicit val p = PeapodGenerator.peapod()
    val t = new Test()
    assert(!t.ready())
    val path = new Path(p.path + "/file.tmp")
    path.getFileSystem(p.sc.hadoopConfiguration).createNewFile(path)
    assert(t.ready())
  }


  test("GlobFile") {
    implicit val p = PeapodGenerator.peapod()
    val t = new TestGlob()
    assert(!t.ready())
    val path = new Path(p.path + "/file.tmp")
    path.getFileSystem(p.sc.hadoopConfiguration).createNewFile(path)
    assert(t.ready())
  }


  test("MultipleFiles") {
    implicit val p = PeapodGenerator.peapod()
    val t = new TestMultiple()
    assert(!t.ready())
    val path = new Path(p.path + "/file2.tmp")
    path.getFileSystem(p.sc.hadoopConfiguration).createNewFile(path)
    assert(t.ready())
  }
}
