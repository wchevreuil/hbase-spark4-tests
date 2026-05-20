FROM eclipse-temurin:17-jdk-focal

ENV HBASE_HOME=/opt/hbase
ENV SPARK_HOME=/opt/spark
ENV HBASE_CONF_DIR=/opt/hbase/conf
ENV PATH=$PATH:$HBASE_HOME/bin:$SPARK_HOME/bin

# ── Unpack HBase ─────────────────────────────────────────────────────────────
COPY lib/hbase-2.6.2-bin.tar.gz /tmp/
RUN tar -xzf /tmp/hbase-2.6.2-bin.tar.gz -C /opt && \
    mv /opt/hbase-2.6.2 $HBASE_HOME && \
    rm /tmp/hbase-2.6.2-bin.tar.gz

# ── Unpack Spark ─────────────────────────────────────────────────────────────
COPY lib/spark-4.0.2-bin-hadoop3.tgz /tmp/
RUN tar -xzf /tmp/spark-4.0.2-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-4.0.2-bin-hadoop3 $SPARK_HOME && \
    rm /tmp/spark-4.0.2-bin-hadoop3.tgz

# ── Remove shaded fat JARs ────────────────────────────────────────────────────
# hbase-shaded-mapreduce and hbase-shaded-client bundle ALL HBase classes
# unrelocated. When on Spark's classpath they create a second copy of every
# HBase class loaded by a different classloader, causing:
#   "ClusterStatusListener$MulticastListener not ClusterStatusListener$Listener"
RUN rm -f $HBASE_HOME/lib/hbase-shaded-mapreduce-*.jar \
          $HBASE_HOME/lib/hbase-shaded-client-*.jar

# ── Connector JARs ───────────────────────────────────────────────────────────
COPY lib/hbase-spark4-1.1.0-SNAPSHOT.jar                    $HBASE_HOME/lib/
COPY lib/hbase-spark-protocol-shaded-1.1.0-SNAPSHOT.jar     $HBASE_HOME/lib/
COPY lib/hbase-spark-pushdown_2.13-1.1.0-SNAPSHOT.jar       $HBASE_HOME/lib/
COPY lib/slf4j-reload4j-1.7.36.jar                          $HBASE_HOME/lib/client-facing-thirdparty/

# ── HBase config ─────────────────────────────────────────────────────────────
RUN rm -f $HBASE_HOME/conf/hbase-site.xml
COPY scripts/hbase-site.xml $HBASE_HOME/conf/hbase-site.xml

# RegionServer-side Spark SQL filter pushdown (SparkSQLPushDownFilter) requires
# scala-library + hbase-spark + hbase-spark-protocol-shaded on HBASE_CLASSPATH.
# See https://hbase.apache.org/book.html#_sparksqldataframes
RUN SCALA_LIB=$(basename "$(ls "$SPARK_HOME"/jars/scala-library-*.jar | head -1)") && \
    cp "$SPARK_HOME/jars/$SCALA_LIB" "$HBASE_HOME/lib/" && \
    echo "export HBASE_CLASSPATH=\$HBASE_CLASSPATH:\
/opt/hbase/lib/hbase-spark4-1.1.0-SNAPSHOT.jar:\
/opt/hbase/lib/hbase-spark-protocol-shaded-1.1.0-SNAPSHOT.jar:\
/opt/hbase/lib/hbase-spark-pushdown_2.13-1.1.0-SNAPSHOT.jar:\
/opt/hbase/lib/$SCALA_LIB" \
    >> "$HBASE_HOME/conf/hbase-env.sh"

# ── Link HBase config into Spark ─────────────────────────────────────────────
RUN ln -s $HBASE_HOME/conf/hbase-site.xml $SPARK_HOME/conf/hbase-site.xml

# ── spark-hbase wrapper ───────────────────────────────────────────────────────
COPY scripts/spark-hbase.sh /usr/local/bin/spark-hbase
RUN chmod +x /usr/local/bin/spark-hbase

WORKDIR /root
