package sativum

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.google.common.io.Resources
import generic.PeapodGenerator
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time.LocalDate
import org.scalatest.FunSuite
import peapod.{Peapod, StorableTask}
import sativum.HiveTest.Parsed
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
}

class HiveTest extends FunSuite {
  test("testHive") {

    val sdf = new SimpleDateFormat("ddMMyy-hhmmss")
    val path = System.getProperty("java.io.tmpdir") + "workflow-" + sdf.format(new Date()) + Random.nextInt()
    new File(path).mkdir()
    new File(path).deleteOnExit()

    generic.Spark.sc.hadoopConfiguration.set("javax.jdo.option.ConnectionURL",
      "jdbc:derby:;databaseName=" + path.replace("\\","/") + "/derby/;create=true")

    implicit val p = new Peapod(
      path= new Path("file://",path.replace("\\","/")).toString,
      raw="")(generic.Spark.sc) with Hive

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

  }
}
