#!/bin/bash
# Run inside spark-worker: docker exec -it spark-worker bash /scripts/run-query-catalog.sh
set -e

HBASE_HOME=/opt/hbase

echo "=== Building classpath ==="
HB_JARS=""
for jar in "$HBASE_HOME"/lib/*.jar; do
  fname=$(basename "$jar")
  case "$fname" in
    hbase-*)          ;;
    opentelemetry-*)  ;;
    *)                continue ;;
  esac
  HB_JARS="${HB_JARS}:${jar}"
done
HB_JARS="${HB_JARS#:}"

# HBaseRelation (catalog DataSource) initializes org.apache.hadoop.hbase.spark.Logging,
# which requires an SLF4J binding (StaticLoggerBinder). Those JARs live under
# client-facing-thirdparty/, not in the top-level lib/*.jar whitelist above.
CFTP="$HBASE_HOME/lib/client-facing-thirdparty"
for jar in "$CFTP"/slf4j-api-*.jar "$CFTP"/slf4j-reload4j-*.jar; do
  if [ -f "$jar" ]; then
    HB_JARS="${HB_JARS}:${jar}"
  fi
done
HB_JARS="${HB_JARS#:}"

PROTO_JAR=/scripts/protobuf-java-2.5.0.jar
if [ -f "$PROTO_JAR" ]; then
  HB_JARS="${HB_JARS}:${PROTO_JAR}"
fi

HB_CP="${HBASE_HOME}/conf:${HB_JARS}"

echo "=== Running catalog-based Spark SQL query ==="
# Spark 4: feed the script on stdin (whole file). Do not use -i or :load with a
# multi-line spark.read chain — the REPL binds df as DataFrameReader and then
# createOrReplaceTempView fails with a misleading compile error.
exec spark-shell \
  --conf "spark.driver.extraClassPath=$HB_CP" \
  --conf "spark.executor.extraClassPath=$HB_CP" \
  < /scripts/query-dengue-catalog.scala
