#!/bin/bash
# Run inside spark-worker: docker exec -it spark-worker bash /scripts/run-bulkload.sh
set -e

HBASE_HOME=/opt/hbase

# ── Step 1: Wait for HBase ────────────────────────────────────────────────────
echo "=== Step 1: Waiting for HBase to be ready ==="
MAX_WAIT=120
WAITED=0
until echo "list" | hbase shell 2>&1 | grep -qE "^TABLE|^0 row"; do
  if [ "$WAITED" -ge "$MAX_WAIT" ]; then
    echo "Timed out. Check: docker logs hbase-master"
    exit 1
  fi
  echo "  Not ready yet... (${WAITED}s elapsed)"
  sleep 5
  WAITED=$((WAITED + 5))
done
echo "  HBase is up."

# ── Step 2: Create table ──────────────────────────────────────────────────────
echo ""
echo "=== Step 2: Creating HBase table ==="
hbase shell /scripts/create-table.hbase

# ── Step 3: Build classpath ───────────────────────────────────────────────────
#
# Whitelist approach: only include JARs that HBase strictly needs and that
# Spark does NOT already ship at a compatible version.
#
#   hbase-*              HBase-specific, obviously needed
#   opentelemetry-api-*  Required by hbase-client tracing (ConnectionFactory
#   opentelemetry-context-*  → TraceUtil → Span); Spark does not ship these
#
# Everything else (hadoop, jackson, commons-lang3, zookeeper, netty, slf4j…)
# is provided by Spark at the correct version. Including HBase's copies only
# causes conflicts:
#   commons-lang3  HBase may ship < 3.12 which lacks SystemUtils.JAVA_17
#   jackson-*      HBase ships 2.17.2, incompatible with Spark's scala module
#   hadoop-*       HBase's copy shadows FileSystem.openFile (added in 3.3+)
#
echo ""
echo "=== Step 3: Building classpath ==="
HB_JARS=""
for jar in "$HBASE_HOME"/lib/*.jar; do
  fname=$(basename "$jar")
  case "$fname" in
    hbase-*)          ;;   # include
    opentelemetry-*)  ;;   # include all OTel JARs — Spark ships none of these
    *)                continue ;;
  esac
  HB_JARS="${HB_JARS}:${jar}"
done
HB_JARS="${HB_JARS#:}"

# protobuf-java-2.5.0.jar must be added explicitly — hbase-client references
# com.google.protobuf.RpcChannel which was removed in protobuf 3.20+ and is
# not present in HBase's lib/ (it used to come from Hadoop, now it's gone).
# Place protobuf-java-2.5.0.jar in scripts/ and it'll be picked up here.
PROTO_JAR=/scripts/protobuf-java-2.5.0.jar
if [ -f "$PROTO_JAR" ]; then
  HB_JARS="${HB_JARS}:${PROTO_JAR}"
  echo "  + protobuf-java-2.5.0.jar from /scripts/"
else
  echo "  WARNING: $PROTO_JAR not found — download it with:"
  echo "    wget https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/2.5.0/protobuf-java-2.5.0.jar -P ~/Projects/hbase-spark-tests/scripts/"
fi

HB_CP="${HBASE_HOME}/conf:${HB_JARS}"

echo "  $(echo "$HB_JARS" | tr ':' '\n' | wc -l | tr -d ' ') JARs (whitelist: hbase-* + opentelemetry-* + protobuf-2.5.0)"

# ── Step 4: Run Spark bulk load ───────────────────────────────────────────────
echo ""
echo "=== Step 4: Running Spark bulk load ==="
# Spark 4: use :load via stdin (see run-query-catalog.sh).
exec spark-shell \
  --conf "spark.driver.extraClassPath=$HB_CP" \
  --conf "spark.executor.extraClassPath=$HB_CP" \
  <<'REPL'
:load /scripts/bulkload.scala
REPL
