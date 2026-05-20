/**
 * HBase Spark Connector - Bulk Load
 *
 * Loads /data/dengue.csv into HBase table "dengue", column family "cf".
 * Each CSV column becomes a separate HBase column qualifier.
 * Row key: zero-padded sequential index (e.g. row_00001).
 *
 * Run via: spark-hbase -i /scripts/bulkload.scala
 */

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes

// ── Config ──────────────────────────────────────────────────────────────────
val csvPath   = "/data/dengue.csv"
val tableName = TableName.valueOf("dengue")
val cf        = Bytes.toBytes("cf")

// ── Read CSV ─────────────────────────────────────────────────────────────────
val df = spark.read
  .option("header", "true")
  .option("inferSchema", "false")   // keep everything as String for HBase
  .csv(csvPath)

println(s"\n=== Schema ===")
df.printSchema()
println(s"\n=== First 5 rows ===")
df.show(5, truncate = false)
println(s"Total rows to load: ${df.count()}\n")

// ── Build HBase Puts ─────────────────────────────────────────────────────────
val hbaseConf    = HBaseConfiguration.create()
val hbaseContext = new HBaseContext(sc, hbaseConf)

// Capture column list on the driver before shipping the closure to executors
val columns = df.columns.toList

val puts = df.rdd.zipWithIndex.map { case (row, idx) =>
  val rowKey = f"row_${idx}%05d"
  val put    = new Put(Bytes.toBytes(rowKey))
  columns.foreach { col =>
    val v = row.getAs[String](col)
    if (v != null) put.addColumn(cf, Bytes.toBytes(col), Bytes.toBytes(v))
  }
  put
}

// ── Write to HBase ───────────────────────────────────────────────────────────
hbaseContext.bulkPut(puts, tableName, (p: Put) => p)
println("✓ Bulk load complete!\n")

// ── Quick verification ───────────────────────────────────────────────────────
// Convert to plain strings on the executor before collecting to the driver —
// ImmutableBytesWritable is not Java-serializable so .take() would fail.
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.CellUtil

println("=== Sample rows from HBase ===")
val scan = new Scan()
hbaseContext.hbaseRDD(tableName, scan)
  .map { case (immutableKey, result) =>
    val rowKey = Bytes.toString(immutableKey.copyBytes())
    val cols = result.listCells().toArray.map { c =>
      val cell = c.asInstanceOf[org.apache.hadoop.hbase.Cell]
      s"${Bytes.toString(CellUtil.cloneQualifier(cell))}=${Bytes.toString(CellUtil.cloneValue(cell))}"
    }.mkString(", ")
    s"  $rowKey -> $cols"
  }
  .take(5)
  .foreach(println)

System.exit(0)
