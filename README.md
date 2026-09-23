# HBase Spark4 Connector Tests

This repo includes scala code for testing the new hbase-connectors spark4 module. It uses docker and docker compose to deploy an HBase cluster over docker containers using
local volumes for its file system (there's no HDFS). As defined in docker-compose.yml, it launches four docker containers: zookeeper, hbase-master, hbase-regionserver and 
spark-worker.

## Dependencies
The Dockerfile expects a lib dir under the project root with the following libs:
    - spark-4.0.2-bin-hadoop3.tgz
    - slf4j-reload4j-1.7.36.jar
    - hbase-spark-protocol-shaded-1.1.0-SNAPSHOT.jar
    - hbase-2.6.2-bin.tar.gz
    - hbase-spark-pushdown_2.13-1.1.0-SNAPSHOT.jar
    - hbase-spark4-1.1.0-SNAPSHOT.jar

Please make sure to create the *lib* folder with these dependencies on the project root, before building the docker image. 

Spark4 requires scala 2.13 and all related scala libs are included within the spark binary tar ball.

The former two jars in the list above, hbase-spark-pushdown_2.13 and the hbase-spark4 jars, are the hbase connectors spark4 module 
generated with the hbase-connectors mvn build profile *-Pspark4*.


