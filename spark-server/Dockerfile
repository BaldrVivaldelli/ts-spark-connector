ARG SPARK_VERSION=4.0.4
FROM apache/spark:${SPARK_VERSION}

ARG SPARK_VERSION

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl netcat-openbsd \
 && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark

# Runtime user and writable Spark directories.
RUN id -u spark >/dev/null 2>&1 || (groupadd -g 1001 spark && useradd -u 1001 -g 1001 -m -s /bin/bash spark) && \
    mkdir -p /home/spark && chown -R spark:spark /home/spark && \
    mkdir -p "$SPARK_HOME/conf" "$SPARK_HOME/jars" "$SPARK_HOME/tmp" "$SPARK_HOME/logs" /opt/spark/conf-plain /data/dest /opt/certs && \
    chown -R spark:spark "$SPARK_HOME" /opt/spark/conf-plain /data /opt/certs

# Pin the version-matched Scala 2.13 Avro artifact and avoid runtime downloads.
ARG SPARK_AVRO_SHA256=f2862c13564bf6cd78cfdfe7b902ef07e68d1d6a5edd9fde336c0cf0fcdb8c55
RUN curl --fail --show-error --location \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/${SPARK_VERSION}/spark-avro_2.13-${SPARK_VERSION}.jar" \
      --output "/opt/spark/jars/spark-avro_2.13-${SPARK_VERSION}.jar" \
 && echo "$SPARK_AVRO_SHA256  /opt/spark/jars/spark-avro_2.13-${SPARK_VERSION}.jar" | sha256sum --check --strict

# TLS is the default profile. docker-compose.plain.yml selects conf-plain.
COPY --chown=spark:spark ./spark-server/conf/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
COPY --chown=spark:spark ./spark-server/conf-plain/spark-defaults.conf /opt/spark/conf-plain/spark-defaults.conf
COPY --chown=spark:spark ./example_data /data
COPY --chown=spark:spark ./spark-server/certs/ca.crt /opt/certs/ca.crt
COPY --chown=spark:spark ./spark-server/certs/cert.crt /opt/certs/cert.crt
COPY --chown=spark:spark ./spark-server/certs/keystore.p12 /opt/certs/keystore.p12

# Stable HOME for Spark/JVM caches.
ENV HOME=/home/spark \
    HADOOP_USER_NAME=spark \
    SPARK_LOCAL_DIRS=$SPARK_HOME/tmp \
    JAVA_TOOL_OPTIONS="-Duser.name=spark -Duser.home=/home/spark"

COPY --chown=spark:spark ./spark-server/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER spark
EXPOSE 15002
ENTRYPOINT ["/bin/bash","/usr/local/bin/entrypoint.sh"]
