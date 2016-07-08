package sativum

import java.io.File
import java.net.URI
import java.sql.{DriverManager, SQLException}
import java.text.SimpleDateFormat
import java.util.Date

import com.google.common.io.Resources
import generic.{PeapodGenerator, Spark}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time.LocalDate
import org.scalatest.FunSuite
import peapod.{Peapod, StorableTask}
import sativum.HiveTest.{Parsed, Parsed2}

import collection.JavaConverters._
import peapod.StorableTask._

import scala.util.Random


object HiveTest {
  case class DependencyInput(label: Double, text: String)

  class Raw(implicit val p: Peapod) extends StorableTask[RDD[DependencyInput]] {
    override val version = "2"
    def generate = {
      p.sc.textFile("file://" + Resources.getResource("dependency.csv").getPath)
        .map(_.split(","))
        .map(l => new DependencyInput(l(0).toDouble, l(1)))
    }
  }

  class Parsed(val partition: LocalDate)(implicit val p: Peapod)
    extends StorableTask[DataFrame] with DatedTask {
    val raw = pea(new Raw)
    def generate = {
      import p.sqlCtx.implicits._
      raw.get().toDF()
    }
  }

  class Parsed2(val partition: LocalDate)(implicit val p: Peapod)
    extends StorableTask[DataFrame] with DatedTask {
    override lazy val baseName = "sativum.HiveTest$Parsed"
    override val version = "2"
    val raw = pea(new Raw)
    def generate = {
      import p.sqlCtx.implicits._
      raw.get().toDF()
    }
  }
}

class HiveTest extends FunSuite {
  val sdf = new SimpleDateFormat("ddMMyy-hhmmss")

  def peapod(rawPath: String
             = System.getProperty("java.io.tmpdir") + "workflow-" + sdf.format(new Date()) + Random.nextInt()) = {
    val path = new Path("file://",rawPath.replace("\\","/")).toString
    val fs = FileSystem.get(new URI(path), Spark.sc.hadoopConfiguration)
    fs.mkdirs(new Path(path))
    fs.deleteOnExit(new Path(path))

    generic.Spark.sc.hadoopConfiguration.set("javax.jdo.option.ConnectionURL",
      "jdbc:derby:;databaseName=" + rawPath.replace("\\","/") + "/derby/;create=true")

    (rawPath, new Peapod(
      path= path,
      raw="")(generic.Spark.sc) with Hive
      )
  }

  test("Hive") {
    implicit val (path, p) = peapod()
    val parsed = new Parsed(new LocalDate("2014-01-01"))
    p.pea(parsed).get()
    p.hive(parsed)

    val parsed2 = new Parsed(new LocalDate("2014-01-02"))
    p.pea(parsed2).get()
    p.hive(parsed2)

    val config = new HiveConf()
    p.sc.hadoopConfiguration.asScala.foreach(c => config.set(c.getKey,c.getValue))
    val client = new HiveMetaStoreClient(config)

    assert(client.tableExists("sativum","hivetest_parsed"))

    client.close()
    p.close()

    //This test is broken in Spark 1.6.x because the hadoopConf is not used to pre-populate the HiveConf
    //val hc = new HiveContext(p.sc)
    //assert(hc.sql("select count(*) as c from sativum.hivetest_parsed").head().getAs[Long]("c") == 24)
    try {
      DriverManager.getConnection("jdbc:derby:;shutdown=true")
    } catch {
      case e: SQLException =>
    }
  }

  test("HiveVersionChange") {
    val (path, p) = peapod()
    val parsed = new Parsed(new LocalDate("2014-01-01"))(p)
    p.pea(parsed).get()
    p.hive(parsed)

    val (path2, p2) = peapod(path)
    val parsed2 = new Parsed2(new LocalDate("2014-01-02"))(p2)
    p2.pea(parsed2).get()
    p2.hive(parsed2)

    val config = new HiveConf()
    p.sc.hadoopConfiguration.asScala.foreach(c => config.set(c.getKey,c.getValue))
    val client = new HiveMetaStoreClient(config)

    assert(client.tableExists("sativum","hivetest_parsed"))

    assert(client.listPartitionNames("sativum","hivetest_parsed",10).asScala.toList == List("dt=2014-01-02"))

    client.close()
    p.close()
    p2.close()

    try {
      DriverManager.getConnection("jdbc:derby:;shutdown=true")
    } catch {
      case e: SQLException =>
    }
  }
}
