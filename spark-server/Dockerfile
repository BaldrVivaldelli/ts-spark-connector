ARG SPARK_VERSION=4.0.4
# The image tag defaults to the Spark version. Spark 3.5 images default to
# Java 11, which the Iceberg runtime no longer supports, so the 3.5 line is
# built from its `-java17` tag.
ARG SPARK_IMAGE_TAG=${SPARK_VERSION}
FROM apache/spark:${SPARK_IMAGE_TAG}

ARG SPARK_VERSION

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl haproxy netcat-openbsd openssl \
 && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark

# Runtime user and writable Spark directories.
RUN id -u spark >/dev/null 2>&1 || (groupadd -g 1001 spark && useradd -u 1001 -g 1001 -m -s /bin/bash spark) && \
    mkdir -p /home/spark && chown -R spark:spark /home/spark && \
    mkdir -p "$SPARK_HOME/conf" "$SPARK_HOME/jars" "$SPARK_HOME/tmp" "$SPARK_HOME/logs" /opt/spark/conf-plain /data/dest /opt/certs && \
    chown -R spark:spark "$SPARK_HOME" /opt/spark/conf-plain /data /opt/certs

# Pin the version-matched Avro artifact (Scala 2.13 from Spark 4, 2.12 on
# Spark 3.5) and avoid runtime downloads.
ARG SPARK_AVRO_SHA256=f2862c13564bf6cd78cfdfe7b902ef07e68d1d6a5edd9fde336c0cf0fcdb8c55
RUN set -eu; \
    case "$SPARK_VERSION" in 3.*) scala="2.12" ;; *) scala="2.13" ;; esac; \
    curl --fail --show-error --location \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_${scala}/${SPARK_VERSION}/spark-avro_${scala}-${SPARK_VERSION}.jar" \
      --output "/opt/spark/jars/spark-avro_${scala}-${SPARK_VERSION}.jar"; \
    echo "$SPARK_AVRO_SHA256  /opt/spark/jars/spark-avro_${scala}-${SPARK_VERSION}.jar" | sha256sum --check --strict

# Spark 4 distributions bundle the Connect server; Spark 3.5 ships it as a
# separate artifact, pinned here per patch release.
RUN set -eu; \
    case "$SPARK_VERSION" in \
      3.5.6) connect_sha="d40a2d1cd2f6201d37750282435cb340a06ec6cbbdcffca5950e2e0edce1163d" ;; \
      3.*) echo "No pinned Spark Connect server jar for Spark ${SPARK_VERSION}." >&2; exit 1 ;; \
      *) connect_sha="" ;; \
    esac; \
    if [ -n "$connect_sha" ]; then \
      curl --fail --show-error --location \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/${SPARK_VERSION}/spark-connect_2.12-${SPARK_VERSION}.jar" \
        --output "/opt/spark/jars/spark-connect_2.12-${SPARK_VERSION}.jar"; \
      echo "${connect_sha}  /opt/spark/jars/spark-connect_2.12-${SPARK_VERSION}.jar" | sha256sum --check --strict; \
    fi

# Lakehouse formats, pinned per Spark minor and verified by checksum.
# Delta 4.4.1 publishes one build per supported Spark minor and needs Spark 4,
# so Spark 3.5 carries the Delta 3.3 line. Iceberg 1.12.0 ships Spark 3.5, 4.0
# and 4.1 runtimes and none for 4.2 yet, so a 4.2 image carries Delta only and
# the E2E suite gates its Iceberg cases on the server version.
ARG DELTA_VERSION=4.4.1
ARG ICEBERG_VERSION=1.12.0
RUN set -eu; \
    minor="${SPARK_VERSION%.*}"; \
    delta_version="$DELTA_VERSION"; \
    delta_storage_sha="f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f"; \
    case "$minor" in \
      3.5) delta_version="3.3.3"; \
           delta_artifact="delta-spark_2.12"; \
           delta_sha="4912bf902a1c453aba3bd838ff212dedd357155212f9bb37a6b47d97f2827ebc"; \
           delta_storage_sha="45ad193053fd30cebe6d9ab1889873cde39b223a0f210e00ebee8b6642476901"; \
           iceberg_artifact="iceberg-spark-runtime-3.5_2.12"; \
           iceberg_sha="8dd8c90e54ed1a20f2461309d047a255ee56382eb7c0a46162b27a4936fd86dc" ;; \
      4.0) delta_artifact="delta-spark_4.0_2.13"; \
           delta_sha="d0766b4b0ace348dca7a5851418767e1eea170c7303c4771cda2934f49627ce8"; \
           iceberg_artifact="iceberg-spark-runtime-4.0_2.13"; \
           iceberg_sha="3f50e93e8ed322ae0da0e380f1b6c27116404329c62e55f0586697b1b8378306" ;; \
      4.1) delta_artifact="delta-spark_4.1_2.13"; \
           delta_sha="ef0e152d6f43a22a4fed7b65a1b7508cee1e20a70f7d301fb53c6a9225e1b979"; \
           iceberg_artifact="iceberg-spark-runtime-4.1_2.13"; \
           iceberg_sha="3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82" ;; \
      4.2) delta_artifact="delta-spark_2.13"; \
           delta_sha="2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be"; \
           iceberg_artifact=""; \
           iceberg_sha="" ;; \
      *) echo "No pinned lakehouse artifacts for Spark ${SPARK_VERSION}." >&2; exit 1 ;; \
    esac; \
    curl --fail --show-error --location \
      "https://repo1.maven.org/maven2/io/delta/${delta_artifact}/${delta_version}/${delta_artifact}-${delta_version}.jar" \
      --output "/opt/spark/jars/${delta_artifact}-${delta_version}.jar"; \
    echo "${delta_sha}  /opt/spark/jars/${delta_artifact}-${delta_version}.jar" | sha256sum --check --strict; \
    curl --fail --show-error --location \
      "https://repo1.maven.org/maven2/io/delta/delta-storage/${delta_version}/delta-storage-${delta_version}.jar" \
      --output "/opt/spark/jars/delta-storage-${delta_version}.jar"; \
    echo "${delta_storage_sha}  /opt/spark/jars/delta-storage-${delta_version}.jar" | sha256sum --check --strict; \
    if [ -n "$iceberg_artifact" ]; then \
      curl --fail --show-error --location \
        "https://repo1.maven.org/maven2/org/apache/iceberg/${iceberg_artifact}/${ICEBERG_VERSION}/${iceberg_artifact}-${ICEBERG_VERSION}.jar" \
        --output "/opt/spark/jars/${iceberg_artifact}-${ICEBERG_VERSION}.jar"; \
      echo "${iceberg_sha}  /opt/spark/jars/${iceberg_artifact}-${ICEBERG_VERSION}.jar" | sha256sum --check --strict; \
    fi

# TLS is the default profile. docker-compose.plain.yml selects conf-plain.
COPY --chown=spark:spark ./spark-server/conf/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
COPY --chown=spark:spark ./spark-server/conf-plain/spark-defaults.conf /opt/spark/conf-plain/spark-defaults.conf

# Register the lakehouse formats in both configuration profiles: the Delta
# extension always, the Iceberg extension and catalog only when this image
# carries its runtime jar (Spark 4.2 ships without one).
RUN set -eu; \
    for conf in "$SPARK_HOME/conf/spark-defaults.conf" /opt/spark/conf-plain/spark-defaults.conf; do \
      if ls /opt/spark/jars/iceberg-spark-runtime-* >/dev/null 2>&1; then \
        printf '\n%s\n%s\n%s\n%s\n%s\n' \
          'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions' \
          'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' \
          'spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog' \
          'spark.sql.catalog.iceberg.type=hadoop' \
          'spark.sql.catalog.iceberg.warehouse=/data/dest/iceberg' >> "$conf"; \
      else \
        printf '\n%s\n%s\n' \
          'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' \
          'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' >> "$conf"; \
      fi; \
    done
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
