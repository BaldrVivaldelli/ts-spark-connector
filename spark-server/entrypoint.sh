#!/usr/bin/env bash
set -euo pipefail

# Por si alguna imagen/base no persiste los dirs:
mkdir -p "$HOME/.ivy2" "$HOME/.m2" "$SPARK_LOCAL_DIRS"

/opt/bitnami/spark/bin/spark-submit \
  --class org.apache.spark.sql.connect.service.SparkConnectServer \
  --conf spark.sql.connect.enable=true \
  --conf spark.sql.connect.grpc.binding=0.0.0.0:15002 \
    /opt/bitnami/spark/jars/spark-connect_2.12-4.0.0.jar \
  --conf spark.ui.port=4040 \
    --conf spark.jars.ivy=$HOME/.ivy2
