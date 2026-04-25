#!/bin/sh

NAME="${1:?Usage: find-product.sh <name>}"

curl -s -X POST http://localhost:8080/find-product \
  -H "Content-Type: application/json" \
  -d "$(jq -n --arg name "$NAME" '{name: $name}')" | jq .
