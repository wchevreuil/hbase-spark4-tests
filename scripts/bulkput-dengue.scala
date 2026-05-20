/**
 * HBase Spark Connector - bulkPut via RDD implicit
 *
 * Reads /data/dengue.csv and loads it into HBase table "dengue"
 * using the hbaseBulkPut implicit method on RDD (from HBaseRDDFunctions).
 *
 * Prerequisite: table created (run create-table.hbase first).
 * Run: docker exec -it spark-worker bash /scripts/run-bulkput-dengue.sh
 */

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.util.Bytes

val csvPath = "/data/dengue.csv"
val tableName = TableName.valueOf("dengue")
val cf = Bytes.toBytes("cf")

val df = spark.read.option("header", "true").option("inferSchema", "false").csv(csvPath)

println("\n=== Schema ===")
df.printSchema()
println("\n=== First 5 rows ===")
df.show(5, false)
println(s"Total rows to load: ${df.count()}\n")

val hbaseConf = HBaseConfiguration.create()
val hbaseContext = new HBaseContext(sc, hbaseConf)

val columns = df.columns.toList

val rdd = df.rdd.zipWithIndex.map { case (row, idx) =>
  val rowKey = f"row_${idx}%05d"
  val put = new Put(Bytes.toBytes(rowKey))
  columns.foreach { col =>
    val v = row.getAs[String](col)
    if (v != null) put.addColumn(cf, Bytes.toBytes(col), Bytes.toBytes(v))
  }
  put
}

println("=== Running hbaseBulkPut (RDD implicit) ===")
rdd.hbaseBulkPut(hbaseContext, tableName, (p: Put) => p)
println(s"bulkPut complete!\n")

println("=== Verifying: sample rows from HBase ===")
val scan = new Scan()
val verifyColumns = Seq("gender", "dengue_label", "age")
val rows = hbaseContext.hbaseRDDAsRows("dengue", scan, verifyColumns)
rows.take(5).foreach { row =>
  println(s"  gender=${row.getString(0)}, dengue_label=${row.getString(1)}, age=${row.getString(2)}")
}
println(s"\nTotal rows in HBase: ${rows.count()}")

System.exit(0)
