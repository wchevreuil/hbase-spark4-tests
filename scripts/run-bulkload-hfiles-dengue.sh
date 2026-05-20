#!/bin/bash
# Run inside spark-worker: docker exec -it spark-worker bash /scripts/run-bulkload-hfiles-dengue.sh
set -e

HBASE_HOME=/opt/hbase
BASE_TABLE="dengue"

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

# ── Step 2: Pick table name and create it ─────────────────────────────────────
echo ""
echo "=== Step 2: Creating HBase table ==="
TABLE_NAME="$BASE_TABLE"
if echo "exists '$BASE_TABLE'" | hbase shell 2>&1 | grep -q "true"; then
  SUFFIX=$(cat /dev/urandom | tr -dc 'a-z0-9' | head -c 6)
  TABLE_NAME="${BASE_TABLE}_${SUFFIX}"
  echo "  Table '$BASE_TABLE' already exists — using '$TABLE_NAME' instead."
fi
echo "create '$TABLE_NAME', {NAME => 'cf', VERSIONS => 1}" | hbase shell
echo "  Table '$TABLE_NAME' created."

# ── Step 3: Clean staging dir ─────────────────────────────────────────────────
STAGING_DIR=/tmp/hbase-bulkload-staging
if [ -d "$STAGING_DIR" ]; then
  echo ""
  echo "=== Removing existing staging dir ==="
  rm -rf "$STAGING_DIR"
fi

# ── Step 4: Build classpath ───────────────────────────────────────────────────
echo ""
echo "=== Step 4: Building classpath ==="
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
  echo "  + protobuf-java-2.5.0.jar from /scripts/"
else
  echo "  WARNING: $PROTO_JAR not found"
fi

HB_CP="${HBASE_HOME}/conf:${HB_JARS}"

echo "  $(echo "$HB_JARS" | tr ':' '\n' | wc -l | tr -d ' ') JARs (whitelist: hbase-* + opentelemetry-* + protobuf-2.5.0)"

# ── Step 5: Run Spark HFile bulk load ─────────────────────────────────────────
echo ""
echo "=== Step 5: Running Spark HFile bulk load (table=$TABLE_NAME) ==="
exec spark-shell \
  --conf "spark.driver.extraClassPath=$HB_CP" \
  --conf "spark.executor.extraClassPath=$HB_CP" \
  --conf "spark.hbase.bulkload.tableName=$TABLE_NAME" \
  <<'REPL'
:load /scripts/bulkload-hfiles-dengue.scala
REPL
