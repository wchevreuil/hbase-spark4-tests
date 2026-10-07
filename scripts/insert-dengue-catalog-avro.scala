/**
 * Spark SQL via HBase JSON catalog (DataSource API).
 *
 * Same query as query-dengue.scala, but loads the table through
 * format("org.apache.hadoop.hbase.spark4") so the connector can apply
 * column pruning and predicate pushdown (no manual hbaseRDD mapping).
 *
 * RegionServers must have scala-library + hbase-spark JARs on HBASE_CLASSPATH
 * (configured in Dockerfile → hbase-env.sh). Rebuild images after changing that.
 *
 * Prerequisite: table loaded (run /scripts/run-bulkload.sh first).
 * Run: bash /scripts/run-query-catalog.sh
 */

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.apache.avro.Schema

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}




val csvPath       = "/data/dengue.csv"

val userTableName = spark.conf.get("spark.hbase.bulkload.tableName", "dengue")
val tableName     = TableName.valueOf(userTableName)

println(s"\n=== Target table: $userTableName ===")

println("\n=== Defining avro schema ===")
val avroSchemaDef: String =
  s"""{"namespace": "dengue.avro",
     |   "type": "record", "name": "DengueRecord",
     |   "fields": [
     |    {"name": "age", "type": ["string", "null"]},
     |    {"name": "gender", "type": ["string", "null"]},
     |    {"name": "hemoglobin_g_dl", "type": ["string", "null"]},
     |    {"name": "wbc_count", "type": ["string", "null"]},
     |    {"name": "differential_count", "type": ["string", "null"]},
     |    {"name": "rbc_count", "type": ["string", "null"]},
     |    {"name": "platelet_count", "type": ["string", "null"]},
     |    {"name": "platelet_distribution_width", "type": ["string", "null"]},
     |    {"name": "dengue_label", "type": ["string", "null"]}
     |   ]
     |}""".stripMargin

val avroSchema: Schema = new Schema.Parser().parse(avroSchemaDef)

println("\n=== Defining avro catalog ===")
val catalog: String = s"""{
  |"table":{"namespace":"default", "name":"$tableName"},
  |"rowkey":"rowkey",
  |"columns":{
  |  "rowkey":{"cf":"rowkey", "col":"rowkey", "type":"string"},
  |  "col1":{"cf":"cf", "col":"dengue_data", "avro":"avroSchema"}
  |}
  |}""".stripMargin

println("\n=== Reading CSV ===")

val csvDF = spark.read.option("header", "true").option("inferSchema", "false").csv(csvPath)

println("\n=== Transforming CSV rows into avro records and saving as a DF ===")

val writerDF = (csvDF.withColumn("rowkey", concat(lit("row"), monotonically_increasing_id()))
  .withColumn("col1", struct(
    col("age"),
    col("gender"),
    col("hemoglobin_g_dl"),
    col("wbc_count"),
    col("differential_count"),
    col("rbc_count"),
    col("platelet_count"),
    col("platelet_distribution_width"),
    col("dengue_label")
  )).select("rowkey", "col1"))

println("\n=== writing to HBase via DS V2 ===")

(writerDF.write
  .format("org.apache.hadoop.hbase.spark.datasources.HBaseTableProvider")
  .option("catalog", catalog)
  .option("avroSchema", avroSchemaDef)
  .option("newtable", "5")
  .mode("append")
  .save())

println("\n=== reading results ===")

val readDF = (spark.read
  .format("org.apache.hadoop.hbase.spark.datasources.HBaseTableProvider")
  .option("catalog", catalog)
  .option("avroSchema", avroSchemaDef)
  .load())

readDF.show(10, false)

readDF.filter(col("col1.dengue_label") === "1").select("rowkey", "col1.age", "col1.gender", "col1.hemoglobin_g_dl", "col1.wbc_count",
    "col1.differential_count", "col1.rbc_count", "col1.platelet_count",
    "col1.platelet_distribution_width", "col1.dengue_label").show(10, false)

System.exit(0)
