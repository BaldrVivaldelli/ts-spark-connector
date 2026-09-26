#!/usr/bin/env bash
set -euo pipefail

# Development-only credentials. The PKCS#12 password is intentionally public
# because this keystore is committed for local Docker/E2E use; never reuse it.
readonly DEV_KEYSTORE_PASSWORD="password"
readonly DEFAULT_CA_DAYS=3650
readonly DEFAULT_SERVER_DAYS=825
readonly RENEWAL_WINDOW_SECONDS=$((30 * 24 * 60 * 60))

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
MODE="generate"

usage() {
  cat <<'EOF'
Usage: ./spark-server/certs/generate-dev-certs.sh [--check]

Without arguments, generates a development CA, a SAN-enabled server
certificate, and a password-protected PKCS#12 keystore. Private PEM keys and
OpenSSL temporary files are created only in a mode-0700 temporary directory
and are deleted on exit.

Options:
  --check  Validate the committed CA, server certificate, SANs, expiry, and
           PKCS#12 contents without replacing anything.
  --help   Show this help.

Optional environment variables:
  SPARK_DEV_CA_DAYS    CA lifetime in days (default: 3650)
  SPARK_DEV_CERT_DAYS  Server certificate lifetime in days (default: 825)

The development keystore password is "password" to match spark-defaults.conf.
These credentials are public test fixtures and must never be used outside
local development or CI.
EOF
}

case "${1:-}" in
  "") ;;
  --check) MODE="check" ;;
  --help|-h) usage; exit 0 ;;
  *) usage >&2; exit 2 ;;
esac

command -v openssl >/dev/null 2>&1 || {
  echo "error: openssl is required" >&2
  exit 127
}

CA_DAYS="${SPARK_DEV_CA_DAYS:-$DEFAULT_CA_DAYS}"
SERVER_DAYS="${SPARK_DEV_CERT_DAYS:-$DEFAULT_SERVER_DAYS}"

for value_name in CA_DAYS SERVER_DAYS; do
  value="${!value_name}"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: $value_name must be a positive integer, got '$value'" >&2
    exit 2
  fi
done

umask 077
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ts-spark-connector-certs.XXXXXX")"
trap 'rm -rf -- "$WORK_DIR"' EXIT HUP INT TERM
printf '%s' "$DEV_KEYSTORE_PASSWORD" >"$WORK_DIR/keystore-password"

verify_artifacts() {
  local artifact_dir="$1"
  local ca="$artifact_dir/ca.crt"
  local cert="$artifact_dir/cert.crt"
  local keystore="$artifact_dir/keystore.p12"

  for artifact in "$ca" "$cert" "$keystore"; do
    if [[ ! -f "$artifact" ]]; then
      echo "error: missing certificate artifact: $artifact" >&2
      return 1
    fi
  done

  openssl verify -purpose sslserver -CAfile "$ca" "$cert" >/dev/null
  openssl verify -CAfile "$ca" -verify_hostname localhost "$cert" >/dev/null
  openssl verify -CAfile "$ca" -verify_hostname spark "$cert" >/dev/null
  openssl verify -CAfile "$ca" -verify_hostname spark-connect "$cert" >/dev/null
  openssl verify -CAfile "$ca" -verify_ip 127.0.0.1 "$cert" >/dev/null

  if ! openssl x509 -in "$cert" -checkend "$RENEWAL_WINDOW_SECONDS" -noout >/dev/null; then
    echo "error: $cert expires within 30 days; regenerate it" >&2
    return 1
  fi

  openssl pkcs12 \
    -in "$keystore" \
    -passin "file:$WORK_DIR/keystore-password" \
    -clcerts \
    -nokeys \
    -out "$WORK_DIR/pkcs12-leaf.pem" \
    >/dev/null 2>&1
  openssl pkcs12 \
    -in "$keystore" \
    -passin "file:$WORK_DIR/keystore-password" \
    -cacerts \
    -nokeys \
    -out "$WORK_DIR/pkcs12-ca.pem" \
    >/dev/null 2>&1
  openssl pkcs12 \
    -in "$keystore" \
    -passin "file:$WORK_DIR/keystore-password" \
    -nocerts \
    -nodes \
    -out "$WORK_DIR/pkcs12-key.pem" \
    >/dev/null 2>&1

  local pem_fingerprint
  local pkcs12_fingerprint
  pem_fingerprint="$(openssl x509 -in "$cert" -noout -fingerprint -sha256)"
  pkcs12_fingerprint="$(openssl x509 -in "$WORK_DIR/pkcs12-leaf.pem" -noout -fingerprint -sha256)"
  if [[ "$pem_fingerprint" != "$pkcs12_fingerprint" ]]; then
    echo "error: cert.crt does not match the leaf certificate in keystore.p12" >&2
    return 1
  fi

  local ca_fingerprint
  local pkcs12_ca_fingerprint
  ca_fingerprint="$(openssl x509 -in "$ca" -noout -fingerprint -sha256)"
  pkcs12_ca_fingerprint="$(openssl x509 -in "$WORK_DIR/pkcs12-ca.pem" -noout -fingerprint -sha256)"
  if [[ "$ca_fingerprint" != "$pkcs12_ca_fingerprint" ]]; then
    echo "error: ca.crt is not the CA certificate in keystore.p12" >&2
    return 1
  fi

  openssl x509 -in "$cert" -pubkey -noout >"$WORK_DIR/cert-public-key.pem"
  openssl pkey -in "$WORK_DIR/pkcs12-key.pem" -pubout >"$WORK_DIR/pkcs12-public-key.pem"
  if ! cmp -s "$WORK_DIR/cert-public-key.pem" "$WORK_DIR/pkcs12-public-key.pem"; then
    echo "error: the private key in keystore.p12 does not match cert.crt" >&2
    return 1
  fi

  if command -v keytool >/dev/null 2>&1; then
    keytool -list \
      -storetype PKCS12 \
      -keystore "$keystore" \
      -storepass "$DEV_KEYSTORE_PASSWORD" \
      >/dev/null
    echo "keytool: PKCS#12 keystore is readable"
  else
    echo "keytool: not installed; PKCS#12 was validated with openssl"
  fi

  openssl x509 -in "$cert" -noout -subject -issuer -dates -ext subjectAltName
}

if [[ "$MODE" == "check" ]]; then
  verify_artifacts "$SCRIPT_DIR"
  exit 0
fi

cat >"$WORK_DIR/ca.cnf" <<'EOF'
[req]
prompt = no
distinguished_name = distinguished_name
x509_extensions = v3_ca

[distinguished_name]
CN = ts-spark-connector development CA
O = ts-spark-connector development only

[v3_ca]
basicConstraints = critical, CA:true, pathlen:0
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always, issuer
EOF

cat >"$WORK_DIR/server.cnf" <<'EOF'
[req]
prompt = no
distinguished_name = distinguished_name
req_extensions = request_extensions

[distinguished_name]
CN = spark-connect
O = ts-spark-connector development only

[request_extensions]
basicConstraints = critical, CA:false
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @subject_alt_names

[server_extensions]
basicConstraints = critical, CA:false
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid, issuer
subjectAltName = @subject_alt_names

[subject_alt_names]
DNS.1 = localhost
DNS.2 = spark
DNS.3 = spark-connect
IP.1 = 127.0.0.1
EOF

openssl genpkey \
  -algorithm RSA \
  -pkeyopt rsa_keygen_bits:3072 \
  -out "$WORK_DIR/ca.key" \
  >/dev/null 2>&1
openssl req \
  -new \
  -x509 \
  -sha256 \
  -key "$WORK_DIR/ca.key" \
  -days "$CA_DAYS" \
  -config "$WORK_DIR/ca.cnf" \
  -out "$WORK_DIR/ca.crt"

openssl genpkey \
  -algorithm RSA \
  -pkeyopt rsa_keygen_bits:3072 \
  -out "$WORK_DIR/cert.key" \
  >/dev/null 2>&1
openssl req \
  -new \
  -sha256 \
  -key "$WORK_DIR/cert.key" \
  -config "$WORK_DIR/server.cnf" \
  -out "$WORK_DIR/cert.csr"
openssl x509 \
  -req \
  -sha256 \
  -in "$WORK_DIR/cert.csr" \
  -CA "$WORK_DIR/ca.crt" \
  -CAkey "$WORK_DIR/ca.key" \
  -CAcreateserial \
  -days "$SERVER_DAYS" \
  -extfile "$WORK_DIR/server.cnf" \
  -extensions server_extensions \
  -out "$WORK_DIR/cert.crt"

openssl pkcs12 \
  -export \
  -name spark-connect \
  -inkey "$WORK_DIR/cert.key" \
  -in "$WORK_DIR/cert.crt" \
  -certfile "$WORK_DIR/ca.crt" \
  -passout "file:$WORK_DIR/keystore-password" \
  -out "$WORK_DIR/keystore.p12"

verify_artifacts "$WORK_DIR"

install -m 0644 "$WORK_DIR/ca.crt" "$SCRIPT_DIR/ca.crt"
install -m 0644 "$WORK_DIR/cert.crt" "$SCRIPT_DIR/cert.crt"
install -m 0600 "$WORK_DIR/keystore.p12" "$SCRIPT_DIR/keystore.p12"

echo "Development TLS artifacts renewed in $SCRIPT_DIR"
echo "Rebuild the Spark Docker image before using the renewed keystore."
