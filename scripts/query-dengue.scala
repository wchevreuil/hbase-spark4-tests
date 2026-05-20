/**
 * Spark SQL: count rows in HBase table "dengue"
 * where gender = 'Male' and dengue_label = '1'.
 *
 * Prerequisite: table loaded (run /scripts/run-bulkload.sh first).
 * Run: spark-hbase -i /scripts/query-dengue.scala
 */

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.types._

val columns = Seq(
  "age", "gender", "hemoglobin_g_dl", "wbc_count", "differential_count",
  "rbc_count", "platelet_count", "platelet_distribution_width", "dengue_label"
)

val schema = StructType(columns.map(c => StructField(c, StringType, nullable = true)))

val hbaseConf    = HBaseConfiguration.create()
val hbaseContext = new HBaseContext(sc, hbaseConf)
val scan         = new Scan()

val rows = hbaseContext.hbaseRDDAsRows("dengue", scan, columns)
val df = spark.createDataFrame(rows, schema)
df.createOrReplaceTempView("dengue")

println("\n=== Row count: gender = 'Male' AND dengue_label = '1' ===")
spark.sql("""
  SELECT COUNT(*) AS male_dengue_positive_count
  FROM dengue
  WHERE gender = 'Male'
    AND dengue_label = '1'
""").show(false)

System.exit(0)
