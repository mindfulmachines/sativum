package sativum

import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.spark.sql.types._

/**
  * Created by marcin.mejran on 4/8/16.
  */
object StructTypeToHiveConverter {
  def convert(s : StructType): Array[(String, String)] = {
    s.fields.map{
      f => (f.name,
        convertDataType(f.dataType)
        )
    }
  }

  def convertDataType(dt: DataType): String = {
    dt match {
      case d: IntegerType => serdeConstants.INT_TYPE_NAME
      case d: LongType => serdeConstants.BIGINT_TYPE_NAME
      case d: StringType => serdeConstants.STRING_TYPE_NAME
      case d: DoubleType => serdeConstants.DOUBLE_TYPE_NAME
      case d: StructType => serdeConstants.STRUCT_TYPE_NAME +
        "<" + convert(d).map(e => e._1 + ":" + e._2 ).mkString(",") + ">"
      case d: ArrayType => serdeConstants.LIST_TYPE_NAME + "<" + convertDataType(d.elementType) + ">"
    }
  }
  def convertToHive(s: StructType) = {
    convert(s).map(f => new FieldSchema(f._1, f._2, "")).toList
  }
}
