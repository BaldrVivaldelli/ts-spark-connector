#!/usr/bin/env bash
set -euo pipefail

export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

# Asegura rutas escribibles para el usuario
export SPARK_IVY_HOME="${SPARK_IVY_HOME:-$HOME/.ivy2}"
mkdir -p "${HOME:-/tmp}/.m2" "$SPARK_IVY_HOME" "${SPARK_LOCAL_DIRS:-$SPARK_HOME/tmp}" "$SPARK_HOME/logs"

# Detectar versiones para armar el paquete Avro por defecto
SCALA_BIN_VER="${SCALA_BIN_VER:-}"
if [[ -z "${SCALA_BIN_VER}" ]]; then
  SCALA_BIN_VER="$(basename "$(ls -1 "$SPARK_HOME"/jars/spark-core_*.jar | head -n1)" | sed -E 's/.*spark-core_([0-9]+\.[0-9]+)-.*/\1/')" || true
fi
SPARK_VER="${SPARK_VER:-}"
if [[ -z "${SPARK_VER}" ]]; then
  SPARK_VER="$(basename "$(ls -1 "$SPARK_HOME"/jars/spark-catalyst_*.jar | head -n1)" | sed -E 's/.*spark-catalyst_[0-9.]+-([0-9.]+)\.jar/\1/')" || true
fi

PACKAGES_DEFAULT="org.apache.spark:spark-avro_${SCALA_BIN_VER:-2.13}:${SPARK_VER:-4.0.0}"
PACKAGES="${SPARK_PACKAGES:-$PACKAGES_DEFAULT}"

start_connect() {
  local FLAGS=(--host 0.0.0.0 --port "${SPARK_CONNECT_PORT:-15002}")

  if [[ -x "$SPARK_HOME/sbin/start-connect-server.sh" ]]; then
    if [[ -x "$SPARK_HOME/bin/spark-connect-server" ]]; then
      # Binario directo: pasamos --packages
      exec "$SPARK_HOME/bin/spark-connect-server" \
        "${FLAGS[@]}" \
        --packages "$PACKAGES"
    else
      # Solo sbin: usamos un SPARK_CONF_DIR escribible y seteamos spark.jars.*
      mkdir -p "$HOME/conf"
      cp -a "$SPARK_HOME/conf/." "$HOME/conf/" 2>/dev/null || true
      {
        grep -q '^spark.jars.packages' "$HOME/conf/spark-defaults.conf" 2>/dev/null || \
          echo "spark.jars.packages $PACKAGES"
        grep -q '^spark.jars.ivy' "$HOME/conf/spark-defaults.conf" 2>/dev/null || \
          echo "spark.jars.ivy $SPARK_IVY_HOME"
      } >> "$HOME/conf/spark-defaults.conf"
      export SPARK_CONF_DIR="$HOME/conf"

      "$SPARK_HOME/sbin/start-connect-server.sh" "${FLAGS[@]}"
      exec tail -F "$SPARK_HOME/logs/"*.out
    fi

  elif [[ -x "$SPARK_HOME/bin/spark-connect-server" ]]; then
    exec "$SPARK_HOME/bin/spark-connect-server" \
      "${FLAGS[@]}" \
      --packages "$PACKAGES"
  else
    echo "No encuentro Spark Connect Server en $SPARK_HOME."
    ls -la "$SPARK_HOME/bin" "$SPARK_HOME/sbin" || true
    exit 127
  fi
}

case "${SPARK_ROLE:-connect}" in
  connect) start_connect ;;
  master)  exec start-master.sh -p "${SPARK_MASTER_PORT:-7077}" ;;
  worker)  exec start-worker.sh "${SPARK_MASTER_URL:-spark://localhost:7077}" ;;
  submit)  exec spark-submit ${SPARK_SUBMIT_ARGS:-} "${SPARK_APP:-$SPARK_HOME/examples/src/main/python/pi.py}" ;;
  *)       exec "$@" ;;
esac
