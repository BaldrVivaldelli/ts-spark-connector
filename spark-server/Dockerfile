FROM bitnami/spark:4.0.0-debian-12-r20

USER root
RUN install_packages curl

# Avro ya embebido (si no lo tenías):
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/4.0.0/spark-avro_2.13-4.0.0.jar \
  -o /opt/bitnami/spark/jars/spark-avro_2.13-4.0.0.jar

# Crear usuario/grupo 1001 con nombre "spark" (ignora error si ya existen)
RUN groupadd -g 1001 spark 2>/dev/null || true && \
    useradd  -u 1001 -g 1001 -m -s /bin/bash spark 2>/dev/null || true && \
    mkdir -p /opt/bitnami/spark/{.ivy2,.m2,tmp} && \
    chown -R 1001:1001 /opt/bitnami/spark


COPY ./conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

# Variables que ayudan a Hadoop/JVM
ENV HOME=/home/spark
ENV HADOOP_USER_NAME=spark
ENV SPARK_LOCAL_DIRS=/opt/bitnami/spark/tmp
ENV JAVA_TOOL_OPTIONS=-Duser.name=spark

# Entrypoint
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh && chown 1001:1001 /usr/local/bin/entrypoint.sh

USER 1001
ENTRYPOINT ["bash","/usr/local/bin/entrypoint.sh"]
