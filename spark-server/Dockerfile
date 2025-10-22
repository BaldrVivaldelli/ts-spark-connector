FROM apache/spark:4.0.0

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl netcat-openbsd \
 && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark

# Usuario "spark" y directorios
RUN id -u spark >/dev/null 2>&1 || (groupadd -g 1001 spark && useradd -u 1001 -g 1001 -m -s /bin/bash spark) && \
    mkdir -p /home/spark && chown -R spark:spark /home/spark && \
    mkdir -p $SPARK_HOME/{conf,jars,tmp,logs} /data /opt/certs && \
    chown -R spark:spark $SPARK_HOME /data /opt/certs

# Conf/datos/certs con ownership correcto (evita Permission denied)
COPY --chown=spark:spark ./spark-server/conf/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
COPY --chown=spark:spark ./example_data /data
COPY --chown=spark:spark ./spark-server/certs /opt/certs

# ❗️ Claves:
# - Forzamos user.home para la JVM
# - Definimos un Ivy cache escribible
ENV HOME=/home/spark \
    HADOOP_USER_NAME=spark \
    SPARK_LOCAL_DIRS=$SPARK_HOME/tmp \
    JAVA_TOOL_OPTIONS="-Duser.name=spark -Duser.home=/home/spark" \
    SPARK_IVY_HOME=/home/spark/.ivy2

# Entrypoint
COPY --chown=spark:spark ./spark-server/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER spark
EXPOSE 15002
ENTRYPOINT ["/bin/bash","/usr/local/bin/entrypoint.sh"]
