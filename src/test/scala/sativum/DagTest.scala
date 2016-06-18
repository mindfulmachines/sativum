package sativum

import generic.PeapodGenerator
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite
import peapod.{EphemeralTask, Peapod}

import scala.concurrent.Future


class DagTest extends FunSuite {

  class TestSensor(implicit val p: Peapod) extends EphemeralTask[Double] with FileSensor {
    override val source = p.path + "/file.tmp"
    override protected def generate: Double = 1.0
  }

  class TestDag(implicit val sativum: Sativum) extends Dag {
    override val waitTime = 500
    endpoint(new TestSensor())
  }

  test("SingleFile") {
    implicit val p = PeapodGenerator.sativum()
    val dag = new TestDag()(p)
    assert(!dag.ready())
    val path = new Path(p.path + "/file.tmp")
    path.getFileSystem(p.sc.hadoopConfiguration).createNewFile(path)
    assert(dag.ready())
  }

  test("View") {
    implicit val p = PeapodGenerator.sativum()
    val dag = new TestDag()(p)
    assert(dag.view() ==
      "http://graphvizserver-env.elasticbeanstalk.com/?" +
        "H4sIAAAAAAAAAEvJTC9KLMhQcFeozstPSVWILs5ILEi1TcqviK1WKErMy7YtTsxNta5F4dQCAD79o5Y2AAAA")
  }

  test("Name") {
    implicit val p = PeapodGenerator.sativum()
    val dag = new TestDag()(p)
    assert(dag.name == "sativum.DagTest$TestDag")
  }

  test("Run") {
    import scala.concurrent.ExecutionContext.Implicits.global
    implicit val p = PeapodGenerator.sativum()
    val dag = new TestDag()(p)
    val f = Future(dag.run())
    Thread.sleep(100)
    assert(!f.isCompleted)
    val path = new Path(p.path + "/file.tmp")
    path.getFileSystem(p.sc.hadoopConfiguration).createNewFile(path)
    Thread.sleep(100)
    assert(!f.isCompleted)
    Thread.sleep(1000)
    assert(f.isCompleted)

  }
}
