/**
 * HBase Spark Connector - HFile Bulk Load (bulkLoad)
 *
 * Reads /data/dengue.csv and generates HFiles in a staging directory
 * using HBaseContext.bulkLoad, then loads them into HBase table "dengue"
 * via LoadIncrementalHFiles.doBulkLoad.
 *
 * Unlike bulkPut (which writes directly via Put RPCs), bulkLoad generates
 * HFiles on disk and loads them in bulk — much faster for large datasets.
 *
 * Run: docker exec -it spark-worker bash /scripts/run-bulkload-hfiles-dengue.sh
 */

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.spark.{HBaseContext, KeyFamilyQualifier}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}

val streamPath       = "/data/stream/input"
val userTableName = spark.conf.get("spark.hbase.streaming.tableName", "dengue")
val tableName     = TableName.valueOf(userTableName)
val checkpointDir    = "/data/stream/checkpoint"

println(s"\n=== Target table: $userTableName ===")

val catalog = s"""
{
  "table":{"namespace":"default", "name":"$userTableName"},
  "rowkey":"rowkey",
  "columns":{
    "rowkey":{"cf":"rowkey", "col":"rowkey", "type":"string"},
    "age":{"cf":"cf", "col":"age", "type":"string"},
    "gender":{"cf":"cf", "col":"gender", "type":"string"},
    "hemoglobin_g_dl":{"cf":"cf", "col":"hemoglobin_g_dl", "type":"string"},
    "wbc_count":{"cf":"cf", "col":"wbc_count", "type":"string"},
    "differential_count":{"cf":"cf", "col":"differential_count", "type":"string"},
    "rbc_count":{"cf":"cf", "col":"rbc_count", "type":"string"},
    "platelet_count":{"cf":"cf", "col":"platelet_count", "type":"string"},
    "platelet_distribution_width":{"cf":"cf", "col":"platelet_distribution_width", "type":"string"},
    "dengue_label":{"cf":"cf", "col":"dengue_label", "type":"string"}
  }
}
"""

val schema = StructType(Seq(
  StructField("age", StringType, true),
  StructField("gender", StringType, true),
  StructField("hemoglobin_g_dl", StringType, true),
  StructField("wbc_count", StringType, true),
  StructField("differential_count", StringType, true),
  StructField("rbc_count", StringType, true),
  StructField("platelet_count", StringType, true),
  StructField("platelet_distribution_width", StringType, true),
  StructField("dengue_label", StringType, true)
))


val reader = (spark.readStream
  .option("header", "true")
  .schema(schema)
  .format("csv")
  .load(streamPath)
  .withColumn("rowkey", expr("uuid()")))


val query = (reader.writeStream
  .format("hbase")
  .option("catalog", catalog)
  .option("checkpointLocation", checkpointDir)
  .start())

query.awaitTermination()

System.exit(0)
