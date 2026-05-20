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

val csvPath       = "/data/dengue.csv"
val userTableName = spark.conf.get("spark.hbase.bulkload.tableName", "dengue")
val tableName     = TableName.valueOf(userTableName)
val cf            = Bytes.toBytes("cf")
val stagingDir    = "/tmp/hbase-bulkload-staging"

println(s"\n=== Target table: $userTableName ===")

val df = spark.read.option("header", "true").option("inferSchema", "false").csv(csvPath)

println("\n=== Schema ===")
df.printSchema()
println("\n=== First 5 rows ===")
df.show(5, false)
val rowCount = df.count()
println(s"Total rows to load: $rowCount\n")

val hbaseConf    = HBaseConfiguration.create()
val hbaseContext = new HBaseContext(sc, hbaseConf)

val columns = df.columns.toList

val cfBytes = cf.clone()
val colList = columns

val kfqRdd = df.rdd.zipWithIndex.flatMap { case (row, idx) => val rowKey = Bytes.toBytes(f"row_${idx}%05d"); colList.iterator.flatMap { col => val v = row.getAs[String](col); if (v != null) Some((new KeyFamilyQualifier(rowKey, cfBytes, Bytes.toBytes(col)), Bytes.toBytes(v))) else None } }

println("=== Generating HFiles via bulkLoad ===")
hbaseContext.bulkLoad[(KeyFamilyQualifier, Array[Byte])](kfqRdd, tableName, t => Iterator(t), stagingDir)
println(s"HFiles generated in $stagingDir\n")

println("=== Loading HFiles into HBase ===")
val conn  = ConnectionFactory.createConnection(hbaseConf)
val admin = conn.getAdmin
val table = conn.getTable(tableName)
val regionLocator = conn.getRegionLocator(tableName)

try { val load = new LoadIncrementalHFiles(hbaseConf); load.doBulkLoad(new Path(stagingDir), admin, table, regionLocator); println("HFiles loaded into HBase successfully!\n") } finally { regionLocator.close(); table.close(); admin.close(); conn.close() }

println("=== Verifying: sample rows from HBase ===")
val hbaseConf2    = HBaseConfiguration.create()
val hbaseContext2 = new HBaseContext(sc, hbaseConf2)
val scan = new Scan()
hbaseContext2.hbaseRDD(tableName, scan).map { case (immutableKey, result) => val rowKey = Bytes.toString(immutableKey.copyBytes()); val cols = result.listCells().toArray.map { c => val cell = c.asInstanceOf[org.apache.hadoop.hbase.Cell]; s"${Bytes.toString(CellUtil.cloneQualifier(cell))}=${Bytes.toString(CellUtil.cloneValue(cell))}" }.mkString(", "); s"  $rowKey -> $cols" }.take(5).foreach(println)

println(s"\nBulk load complete: $rowCount rows loaded via HFiles.")

System.exit(0)
