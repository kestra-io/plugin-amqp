#!/usr/bin/env bash
set -euo pipefail

mkdir -p certs src/test/resources/tls

if [ ! -f certs/ca.crt ]; then
  openssl genrsa -out certs/ca.key 2048
  openssl req -x509 -new -nodes -key certs/ca.key -sha256 -days 365 \
    -subj "/CN=AMQP Test CA" -out certs/ca.crt

  openssl genrsa -out certs/server.key 2048
  openssl req -new -key certs/server.key -out certs/server.csr -subj "/CN=localhost"
  openssl x509 -req -in certs/server.csr -CA certs/ca.crt -CAkey certs/ca.key \
    -CAcreateserial -out certs/server.crt -days 365 -sha256 \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")
fi

chmod 644 certs/ca.crt certs/server.crt certs/server.key
cp certs/ca.crt src/test/resources/tls/ca.crt

cat > certs/20-tls.conf <<'EOF'
listeners.ssl.default = 5671
ssl_options.cacertfile = /certs/ca.crt
ssl_options.certfile = /certs/server.crt
ssl_options.keyfile = /certs/server.key
ssl_options.verify = verify_none
ssl_options.fail_if_no_peer_cert = false
EOF

docker compose -f docker-compose-ci.yml up -d

# Docker accepts TCP on a published port before RabbitMQ is up, so wait for real AMQP and TLS replies.
amqp_ready() {
  timeout 3 bash -c 'exec 3<>/dev/tcp/127.0.0.1/5672 && printf "AMQP\000\000\011\001" >&3 && [ -n "$(head -c 1 <&3 | od -An -tx1)" ]' 2>/dev/null
}
amqps_ready() {
  local out
  out=$(timeout 5 openssl s_client -connect 127.0.0.1:5671 -servername localhost -CAfile certs/ca.crt </dev/null 2>/dev/null) || true
  [[ "$out" == *"Verify return code: 0 (ok)"* ]]
}
for check in amqp_ready amqps_ready; do
  ready=false
  for _ in $(seq 1 90); do
    if "$check"; then
      ready=true
      break
    fi
    sleep 1
  done
  if [ "$ready" != true ]; then
    echo "RabbitMQ did not become ready ($check)" >&2
    docker compose -f docker-compose-ci.yml logs --tail 50 rabbitmq >&2
    exit 1
  fi
done
echo "RabbitMQ started on 5672 (plain) and 5671 (TLS)"
