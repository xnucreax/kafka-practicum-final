#!/bin/bash
set -e

CA_DIR="../ca/certs"

for SERVICE in practicum-1 practicum-2; do
    mkdir -p "$SERVICE/certs"

    openssl req -new \
        -newkey rsa:2048 \
        -keyout "$SERVICE/certs/$SERVICE.key" \
        -out "$SERVICE/certs/$SERVICE.csr" \
        -config "$SERVICE/service-cert.cnf" \
        -nodes

    openssl x509 -req \
        -days 365 \
        -in "$SERVICE/certs/$SERVICE.csr" \
        -CA "$CA_DIR/ca.crt" \
        -CAkey "$CA_DIR/ca.key" \
        -CAcreateserial \
        -out "$SERVICE/certs/$SERVICE.crt"

    echo "Generated cert for $SERVICE"
done
