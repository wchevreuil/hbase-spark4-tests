#!/bin/bash
# Run inside spark-worker: docker exec -it spark-worker bash /scripts/run-query-mappartitions.sh
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

PROTO_JAR=/scripts/protobuf-java-2.5.0.jar
if [ -f "$PROTO_JAR" ]; then
  HB_JARS="${HB_JARS}:${PROTO_JAR}"
fi

HB_CP="${HBASE_HOME}/conf:${HB_JARS}"

echo "=== Running mapPartitions aggregation query ==="
exec spark-shell \
  --conf "spark.driver.extraClassPath=$HB_CP" \
  --conf "spark.executor.extraClassPath=$HB_CP" \
  <<'REPL'
:load /scripts/query-dengue-mappartitions.scala
REPL
