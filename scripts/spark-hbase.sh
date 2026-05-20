#!/bin/bash
set -e

# Build the HBase classpath, but EXCLUDE Hadoop JARs.
# Spark 3.5 ships hadoop-client 3.3.4 which is newer than HBase's bundled
# Hadoop. If HBase's hadoop JARs take precedence (via userClassPathFirst),
# FileSystem.openFile(Path) — added in 3.3 — is missing and CSV reads fail.
# We let Spark own all Hadoop classes; HBase only adds its own client JARs.
HB_CP=$(hbase classpath | tr ':' '\n' \
  | grep -v '^$' \
  | grep -v '/hadoop-' \
  | tr '\n' ':' | sed 's/:$//')

HB_JARS=$(echo "$HB_CP" | tr ':' ',')

exec spark-shell \
  --jars "$HB_JARS" \
  --conf "spark.driver.extraClassPath=$HB_CP" \
  --conf "spark.executor.extraClassPath=$HB_CP" \
  "$@"
