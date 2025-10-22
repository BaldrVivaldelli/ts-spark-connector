#!/usr/bin/env bash
set -euo pipefail

export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

mkdir -p "${HOME:-/tmp}/.m2" "${SPARK_LOCAL_DIRS:-$SPARK_HOME/tmp}" "$SPARK_HOME/logs"

start_connect() {
  local FLAGS=(--host 0.0.0.0 --port "${SPARK_CONNECT_PORT:-15002}")

  if [[ -x "$SPARK_HOME/bin/spark-connect-server" ]]; then
    # binario directo en 4.x
    exec "$SPARK_HOME/bin/spark-connect-server" "${FLAGS[@]}"
  elif [[ -x "$SPARK_HOME/sbin/start-connect-server.sh" ]]; then
    # script sbin (por compatibilidad)
    "$SPARK_HOME/sbin/start-connect-server.sh" "${FLAGS[@]}"
    exec tail -F "$SPARK_HOME/logs/"*.out
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
