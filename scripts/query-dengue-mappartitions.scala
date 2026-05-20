/**
 * HBase Spark Connector - RDD aggregation via hbaseRDDAsRows
 *
 * Scans the "dengue" table using hbaseRDDAsRows and aggregates
 * counts by (gender, dengue_label), showing how many male/female
 * got positive/negative results.
 *
 * Prerequisite: table loaded (run /scripts/run-bulkload.sh first).
 * Run: docker exec -it spark-worker bash /scripts/run-query-mappartitions.sh
 */

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.spark.HBaseContext

val hbaseConf = HBaseConfiguration.create()
val hbaseContext = new HBaseContext(sc, hbaseConf)
val scan = new Scan()

val columns = Seq("gender", "dengue_label")

val rowsRdd = hbaseContext.hbaseRDDAsRows("dengue", scan, columns)

// mapPartitions on the plain Row RDD — no serialization issues
val pairs = rowsRdd.mapPartitions { iter =>
  iter.map { row =>
    val gender = Option(row.getString(0)).getOrElse("Unknown")
    val label = Option(row.getString(1)).getOrElse("Unknown")
    (gender, label)
  }
}

val counts = pairs.countByValue()

println("\n=== Dengue Results by Gender ===")
println(f"${"Gender"}%-10s ${"Dengue Label"}%-15s ${"Count"}%s")
println("-" * 40)

counts.toSeq
  .sortBy { case ((gender, label), _) => (gender, label) }
  .foreach { case ((gender, label), count) =>
    val desc = if (label == "1") "Positive" else if (label == "0") "Negative" else label
    println(f"${gender}%-10s ${desc}%-15s ${count}%d")
  }

println()
System.exit(0)
