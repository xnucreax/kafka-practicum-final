#!/bin/sh

curl -s http://localhost:8083/connectors/pg-debezium/status | jq .
