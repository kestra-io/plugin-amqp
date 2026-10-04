set -e

# Throwaway CA and broker certificate for the TLS (AMQPS) tests.
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

for port in 5672 5671; do
  for i in $(seq 1 60); do
    nc -z 127.0.0.1 "$port" && break
    sleep 1
  done
done
echo "RabbitMQ started on 5672 (plain) and 5671 (TLS)"
