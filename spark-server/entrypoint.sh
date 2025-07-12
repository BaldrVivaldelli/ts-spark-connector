#!/bin/bash

/opt/bitnami/spark/bin/spark-submit \
  --class org.apache.spark.sql.connect.service.SparkConnectServer \
  --conf spark.sql.connect.enable=true \
  --conf spark.sql.connect.grpc.binding=0.0.0.0:15002 \
  /opt/bitnami/spark/jars/spark-connect_2.12-4.0.0.jar
