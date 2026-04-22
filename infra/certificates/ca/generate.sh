#!/bin/bash
set -e

mkdir -p certs

openssl req -new -nodes -x509 \
    -days 365 \
    -newkey rsa:2048 \
    -keyout certs/ca.key \
    -out certs/ca.crt \
    -config ca-cert.cnf

cat certs/ca.crt certs/ca.key > certs/ca.pem