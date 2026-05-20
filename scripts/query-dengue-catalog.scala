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

println("\n=== Loading dengue via HBase catalog (DataSource V2 API) ===")
val dengue = spark.read.format("org.apache.hadoop.hbase.spark.datasources.HBaseTableProvider").option("catalog", catalog).load()
dengue.createOrReplaceTempView("dengue")

val sql = """SELECT COUNT(*) AS male_dengue_positive_count FROM dengue WHERE gender = 'Male' AND dengue_label = '1'"""

println("\n=== Catalyst physical plan (look for PushedFilters / Scan) ===")
spark.sql(sql).explain("formatted")

println("\n=== Row count: gender = 'Male' AND dengue_label = '1' ===")
spark.sql(sql).show(false)

System.exit(0)
