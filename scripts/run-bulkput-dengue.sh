#!/bin/bash
# Run inside spark-worker: docker exec -it spark-worker bash /scripts/run-bulkput-dengue.sh
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

# ── Step 2: Build classpath ───────────────────────────────────────────────────
echo ""
echo "=== Step 2: Building classpath ==="
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

# ── Step 3: Run Spark bulkPut ─────────────────────────────────────────────────
echo ""
echo "=== Step 3: Running Spark bulkPut ==="
exec spark-shell \
  --conf "spark.driver.extraClassPath=$HB_CP" \
  --conf "spark.executor.extraClassPath=$HB_CP" \
  <<'REPL'
:load /scripts/bulkput-dengue.scala
REPL
