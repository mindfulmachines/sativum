package sativum

import java.util

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api._
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.spark.sql.DataFrame
import peapod._

import scala.collection.JavaConversions._

/**
  * Created by marcin.mejran on 4/8/16.
  */
trait Hive {
  self: Peapod =>

  private val config = new HiveConf()
  sc.hadoopConfiguration.foreach(c => config.set(c.getKey,c.getValue))
  private val client = new HiveMetaStoreClient(config)

  def hive(task: StorableTask[DataFrame] with DatedTask): Unit = {
    val pea = this.pea(task)
    val df = pea.get()
    createTableAndPartition(task, pea, df)
  }

  private def createTableAndPartition(task: Task[DataFrame] with DatedTask, pea: Pea[DataFrame], df: DataFrame) {
    val (db, table) = pea.task.versionName.splitAt(pea.task.versionName.indexOf("."))
    val cleanTable = table.replace(".","_").replace("$","_").stripPrefix("_")

    val sd = createSd(task, StructTypeToHiveConverter.convertToHive(df.schema))

    createTableIfNeeded(task,sd, db, cleanTable)

    createPartition(task,
      sd,
      db,
      cleanTable)

    client.close()
  }


  private def createPartition(task: Task[DataFrame] with DatedTask,
                      sd: StorageDescriptor,
                      db: String,
                      table: String) {
    val prt = new Partition()
    prt.setDbName(db)
    prt.setTableName(table)
    prt.setSd(sd)
    prt.addToValues(task.partition.toString("yyyy-MM-dd"))

    client.add_partition(prt)
  }

  private def createSd(task: Task[DataFrame], cols: List[FieldSchema]) = {
    // Values for the StorageDescriptor
    val location = task.dir
    val inputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    val outputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    val props = new util.HashMap[String, String]()
    val serDeInfo = new SerDeInfo(classOf[ParquetHiveSerDe].getSimpleName,
      classOf[ParquetHiveSerDe].getName, props)

    // Build the StorageDescriptor
    val sd = new StorageDescriptor()
    sd.setCols(cols)
    sd.setLocation(location)
    sd.setInputFormat(inputFormat)
    sd.setOutputFormat(outputFormat)
    sd.setSerdeInfo(serDeInfo)
    sd.setParameters(new util.HashMap[String, String]())
    sd
  }

  private def createTable(task: Task[DataFrame],
                  sd: StorageDescriptor,
                  db: String,
                  table: String
                 ): Unit = {

    // Define the table
    val tbl = new Table()
    tbl.setDbName(db)
    tbl.setTableName(table)
    tbl.setSd(sd)
    tbl.setParameters(new util.HashMap[String, String]())
    tbl.setViewOriginalText("")
    tbl.setViewExpandedText("")
    tbl.setTableType("EXTERNAL_TABLE")
    tbl.putToParameters("EXTERNAL","TRUE")
    tbl.putToParameters("peapod.version", task.recursiveVersionShort)
    val partitions = new util.ArrayList[FieldSchema]()
    partitions.add(new FieldSchema("dt", serdeConstants.STRING_TYPE_NAME, ""))
    tbl.setPartitionKeys(partitions)

    client.createTable(tbl)
  }

  private def createTableIfNeeded(task: Task[DataFrame],
                           sd: StorageDescriptor,
                          db: String,
                          cleanTable: String) {
    if(! client.getAllDatabases.contains(db)) {
      client.createDatabase(new Database(db,"",path,new util.HashMap[String,String]()))
    }

    if(client.tableExists(db, cleanTable)) {
      if(client.getTable(db, cleanTable).getParameters.get("peapod.version") == task.recursiveVersionShort) {

      } else {
        client.dropTable(db, cleanTable, false, true)
        createTable(task, sd, db, cleanTable)
      }
    } else {
      createTable(task, sd, db, cleanTable)
    }
  }
}