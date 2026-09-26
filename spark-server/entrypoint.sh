#!/usr/bin/env bash
set -euo pipefail

export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

mkdir -p "${HOME:-/tmp}/.m2" "${SPARK_LOCAL_DIRS:-$SPARK_HOME/tmp}" "$SPARK_HOME/logs"

run_connect() {
  local host="$1"
  local port="$2"
  local FLAGS=(--host "$host" --port "$port")

  if [[ -x "$SPARK_HOME/bin/spark-connect-server" ]]; then
    # Direct launcher provided by the pinned Spark 4.x image.
    exec "$SPARK_HOME/bin/spark-connect-server" "${FLAGS[@]}"
  elif [[ -x "$SPARK_HOME/sbin/start-connect-server.sh" ]]; then
    # Keep the upstream sbin launcher as a packaging-compatible fallback.
    "$SPARK_HOME/sbin/start-connect-server.sh" "${FLAGS[@]}"
    exec tail -F "$SPARK_HOME/logs/"*.out
  else
    echo "Spark Connect Server launcher not found in $SPARK_HOME."
    ls -la "$SPARK_HOME/bin" "$SPARK_HOME/sbin" || true
    exit 127
  fi
}

wait_for_backend() {
  local host="$1"
  local port="$2"
  local pid="$3"

  for _ in $(seq 1 90); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "Spark Connect exited before its backend became ready." >&2
      wait "$pid"
      return 1
    fi
    if nc -z "$host" "$port"; then
      return 0
    fi
    sleep 1
  done

  echo "Spark Connect backend did not become ready on ${host}:${port}." >&2
  return 1
}

start_connect() {
  local public_port="${SPARK_CONNECT_PORT:-15002}"
  local tls_enabled="${SPARK_CONNECT_TLS_ENABLED:-true}"

  if [[ "$tls_enabled" != "true" ]]; then
    run_connect 0.0.0.0 "$public_port"
    return
  fi

  local backend_host=127.0.0.1
  local backend_port="${SPARK_CONNECT_BACKEND_PORT:-15003}"
  local runtime_dir="${SPARK_LOCAL_DIRS:-$SPARK_HOME/tmp}/connect-tls"
  local private_key="$runtime_dir/private-key.pem"
  local certificate_bundle="$runtime_dir/server.pem"
  local haproxy_config="$runtime_dir/haproxy.cfg"
  local spark_pid
  local proxy_pid

  mkdir -p "$runtime_dir"
  chmod 700 "$runtime_dir"

  cleanup_connect() {
    local status=$?
    trap - EXIT INT TERM
    [[ -n "${proxy_pid:-}" ]] && kill -TERM "$proxy_pid" 2>/dev/null || true
    [[ -n "${spark_pid:-}" ]] && kill -TERM "$spark_pid" 2>/dev/null || true
    [[ -n "${proxy_pid:-}" ]] && wait "$proxy_pid" 2>/dev/null || true
    [[ -n "${spark_pid:-}" ]] && wait "$spark_pid" 2>/dev/null || true
    rm -f "$private_key" "$certificate_bundle" "$haproxy_config"
    exit "$status"
  }
  trap cleanup_connect EXIT
  trap 'exit 143' INT TERM

  run_connect "$backend_host" "$backend_port" &
  spark_pid=$!
  wait_for_backend "$backend_host" "$backend_port" "$spark_pid"

  openssl pkcs12 \
    -in /opt/certs/keystore.p12 \
    -nocerts \
    -nodes \
    -passin "pass:${SPARK_TLS_KEYSTORE_PASSWORD:-password}" \
    -out "$private_key"
  cat "$private_key" /opt/certs/cert.crt /opt/certs/ca.crt > "$certificate_bundle"
  chmod 600 "$private_key" "$certificate_bundle"

  cat > "$haproxy_config" <<EOF
global
  log stdout format raw local0
  maxconn 256

defaults
  log global
  mode tcp
  timeout connect 10s
  timeout client 5m
  timeout server 5m

frontend spark_connect_tls
  bind 0.0.0.0:${public_port} ssl crt ${certificate_bundle} alpn h2 ssl-min-ver TLSv1.2
  default_backend spark_connect_plain

backend spark_connect_plain
  server spark_connect ${backend_host}:${backend_port} check
EOF

  haproxy -db -f "$haproxy_config" &
  proxy_pid=$!

  if wait -n "$spark_pid" "$proxy_pid"; then
    echo "Spark Connect TLS supervisor exited unexpectedly." >&2
    exit 1
  else
    local status=$?
    echo "Spark Connect TLS component exited with status ${status}." >&2
    exit "$status"
  fi
}

case "${SPARK_ROLE:-connect}" in
  connect) start_connect ;;
  master)  exec start-master.sh -p "${SPARK_MASTER_PORT:-7077}" ;;
  worker)  exec start-worker.sh "${SPARK_MASTER_URL:-spark://localhost:7077}" ;;
  submit)  exec spark-submit ${SPARK_SUBMIT_ARGS:-} "${SPARK_APP:-$SPARK_HOME/examples/src/main/python/pi.py}" ;;
  *)       exec "$@" ;;
esac
