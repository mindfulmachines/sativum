package sativum

import java.sql.{Connection, DriverManager}

import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import peapod.Task

abstract class RedshiftStorableTask extends Task[DataFrame] with DatedTask with Logging  {
  protected def generate: DataFrame

  Class.forName("com.amazon.redshift.jdbc41.Driver")

  val storable = true

  val table = if(p.recursiveVersioning) {
    baseName.replace(".","_") + "_" + recursiveVersionShort
  } else {
    baseName.replace(".","_")
  }

  val temps3 = p.conf.getString("peapod.redshift.temp")

  val url = s"jdbc:redshift://${p.conf.getString("peapod.redshift.url")}/" +
    s"${p.conf.getString("peapod.redshift.database")}?" +
    s"user=${p.conf.getString("peapod.redshift.user")}&" +
    s"password=${p.conf.getString("peapod.redshift.password")}"

  def build(): DataFrame = {
    write(generate)
    writeSuccess()
    read()
  }
  def load(): DataFrame = {
    read()
  }

  protected def read(): DataFrame = {
    val df = p.sqlCtx.read
      .format("com.databricks.spark.redshift")
      .option("url", url)
      .option("query", s"select * from $table where dt='${partition.toString}';")
      .option("tempdir", temps3)
      .load()
    val cols = df.columns.filter(_ != "dt").map(col(_))
    df.select(cols : _*)
  }

  protected def writeSuccess(): Unit = {
    val connection = DriverManager.getConnection(url)
    createPeapodTable(connection)
    connection.createStatement().executeQuery(s"insert into peapod(tbl, dt) values('$table','${partition.toString}');")
    connection.close()
  }

  protected def write(v: DataFrame): Unit = {
    val dt = partition.toString
    v.withColumn("dt",lit(dt))
      .write
      .format("com.databricks.spark.redshift")
      .option("url", url)
      .option("dbtable", table)
      .option("tempdir", temps3)
      .option("sortkeyspec", "SORTKEY(dt)")
      .mode("append")
      .save()
  }

  def delete(): Unit = {
    val connection = DriverManager.getConnection(url)
    createPeapodTable(connection)
    connection.createStatement().executeQuery(s"delete from peapod where tbl = '$table' and dt = '${partition.toString}';")
    if(tableExists()) {
      connection.createStatement().executeQuery(s"delete from $table where dt = '${partition.toString}';")
    }
    connection.close()
  }

  def exists(): Boolean = {
    val connection = DriverManager.getConnection(url)
    createPeapodTable(connection)
    val result =
      if (tableExists()) {
        val resultSet = connection
          .createStatement()
          .executeQuery(s"select * from peapod where tbl = '$table' and dt = '${partition.toString}';")
        resultSet.next()
      } else {
        false
      }
    connection.close()
    result
  }

  private def tableExists(): Boolean = {
    val connection = DriverManager.getConnection(url)
    try {
      connection
        .createStatement().executeQuery(s"SELECT * FROM $table LIMIT 1;")
      true;
    } catch {
      case e: Exception => false
    } finally {
      connection.close()
    }
  }

  private def createPeapodTable(conn: Connection) = {
    conn.createStatement().executeQuery(s"create table if not exists peapod (tbl varchar(256), dt char(10));")
  }
}
