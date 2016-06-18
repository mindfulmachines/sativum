package sativum

import generic.PeapodGenerator
import org.joda.time.LocalDate
import org.scalatest.FunSuite
import peapod.{EphemeralTask, Peapod}

class DatedTaskTest  extends FunSuite {
  class TaskA1(val partition: LocalDate)(implicit val p: Peapod) extends EphemeralTask[Double] with DatedTask  {
    override lazy val baseName = "TaskA"
    override val version = "1"
    override val description = "Return 1 Always"
    def generate = 1
  }
  class TaskA2(val partition: LocalDate)(implicit val p: Peapod) extends EphemeralTask[Double] with DatedTask  {
    override lazy val baseName = "TaskA"
    override val version = "2"
    def generate = 1
  }
  class TaskB1(val partition: LocalDate)(implicit val p: Peapod) extends EphemeralTask[Double] with DatedTask  {
    override lazy val baseName = "TaskB"
    override val description = "Return 1 Always"
    pea(new TaskA1(partition))
    def generate = 1
  }
  class TaskB2(val partition: LocalDate)(implicit val p: Peapod) extends EphemeralTask[Double] with DatedTask  {
    override lazy val baseName = "TaskB"
    pea(new TaskA2(partition))
    def generate = 1
  }

  test("Name") {
    val p = PeapodGenerator.peapod()
    val t = new TaskB2(new LocalDate("2016-01-01"))(p)
    assert(t.name == "TaskB/2016-01-01")

  }

  test("RecursiveVersion") {
    val p1 = PeapodGenerator.peapod()
    val p2 = PeapodGenerator.peapod()
    val t1 = new TaskB1(new LocalDate("2016-01-01"))(p1)
    val t2 = new TaskB2(new LocalDate("2016-01-01"))(p2)
    assert(t1.recursiveVersion == "TaskB:1" :: "-TaskA:1" :: Nil)
    assert(t2.recursiveVersion == "TaskB:1" :: "-TaskA:2" :: Nil)
    assert(t1.recursiveVersionShort == "_vl0nfo5QL1AWZuHQUaotQ")
    assert(t2.recursiveVersionShort == "eSbl8xEbNGEvh7iKBnDChg")
    assert(t1.dir.endsWith("TaskB/_vl0nfo5QL1AWZuHQUaotQ/2016/01/01/"))
    assert(t2.dir.endsWith("TaskB/eSbl8xEbNGEvh7iKBnDChg/2016/01/01/"))
  }

  test("RecursiveVersionLatest") {
    val p1 = PeapodGenerator.sativum()
    val p2 = PeapodGenerator.sativum()
    val t1 = new TaskB1(new LocalDate("2016-01-01"))(p1)
    val t2 = new TaskB2(new LocalDate("2016-01-01"))(p2)
    assert(t1.recursiveVersion == "TaskB:1" :: "-TaskA:1" :: Nil)
    assert(t2.recursiveVersion == "TaskB:1" :: "-TaskA:2" :: Nil)
    assert(t1.recursiveVersionShort == "_vl0nfo5QL1AWZuHQUaotQ")
    assert(t2.recursiveVersionShort == "eSbl8xEbNGEvh7iKBnDChg")
    assert(t1.dir.endsWith("TaskB/latest/2016/01/01/"))
    assert(t2.dir.endsWith("TaskB/latest/2016/01/01/"))
  }
}