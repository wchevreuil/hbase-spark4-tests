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

// Maps Spark SQL column names → HBase cf "cf" + qualifier (as written by bulkload.scala).
// Row key column uses the special cf "rowkey" required by the connector.
val catalog = """
{
  "table":{"namespace":"default", "name":"dengue"},
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

println("\n=== Defining write schema ===")
val writeSchema = StructType(Seq(
  StructField("rowkey", StringType),
  StructField("age", StringType),
  StructField("gender", StringType),
  StructField("hemoglobin_g_dl", StringType),
  StructField("wbc_count", StringType),
  StructField("differential_count", StringType),
  StructField("rbc_count", StringType),
  StructField("platelet_count", StringType),
  StructField("platelet_distribution_width", StringType),
  StructField("dengue_label", StringType)))


val data = Seq(
  Row("row20260918-1", "44", "Male", "12.6", "2200","1","1","6210","11","1"),
  Row("row20260918-2", "41", "Male", "11", "2100","1","1","6000","11","1"),
  Row("row20260918-3", "22", "Female", "10","2000","1","1","6100","10","1"))

println("\n=== Creating write DataFrame ===")
val df = spark.createDataFrame(spark.sparkContext.parallelize(data), writeSchema)

println("\n=== Writing DataFrame to table ===")
df.write
  .format("org.apache.hadoop.hbase.spark.datasources.HBaseTableProvider")
  .option("catalog", catalog).mode("append").save()

val dengue = spark.read.format("org.apache.hadoop.hbase.spark.datasources.HBaseTableProvider").option("catalog", catalog).load()

dengue.createOrReplaceTempView("dengue")

val sql = """SELECT dengue_label FROM dengue WHERE rowkey = 'row20260918-1'"""

val result = spark.sql(sql).collect()(0).getAs[String]("dengue_label")

println(f"\n=== Dengue result for row20260918-1: $result ===")

System.exit(0)
