package generic

import java.io.File
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.fs.{FileSystem, Path}
import peapod.Peapod
import sativum.{Hive, Sativum}

import scala.util.Random


object PeapodGenerator {
  def peapod() = {
    val sdf = new SimpleDateFormat("ddMMyy-hhmmss")
    val rawPath = System.getProperty("java.io.tmpdir") + "workflow-" + sdf.format(new Date()) + Random.nextInt()
    val path = new Path("file://",rawPath.replace("\\","/")).toString
    val fs = FileSystem.get(new URI(path), Spark.sc.hadoopConfiguration)
    fs.mkdirs(new Path(path))
    fs.deleteOnExit(new Path(path))
    val w = new Peapod(
      path=path,
      raw="")(generic.Spark.sc)
    w
  }
  def sativum() = {
    val sdf = new SimpleDateFormat("ddMMyy-hhmmss")
    val rawPath = System.getProperty("java.io.tmpdir") + "workflow-" + sdf.format(new Date()) + Random.nextInt()
    val path = new Path("file://",rawPath.replace("\\","/")).toString
    val fs = FileSystem.get(new URI(path), Spark.sc.hadoopConfiguration)
    fs.mkdirs(new Path(path))
    fs.deleteOnExit(new Path(path))
    val w = new Sativum(
      path= path,
      raw="")(generic.Spark.sc)
    w
  }
}
