#!/bin/sh

curl -X PUT \
-H "Content-Type: application/json" \
--data '{
  "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
  "connection.url": "jdbc:postgresql://postgres:5432/postgres",
  "connection.username": "postgres",
  "connection.password": "postgres",
  "topics": "shop-products-filtered",
  "table.name.format": "products",
  "insert.mode": "upsert",
  "primary.key.mode": "record_value",
  "primary.key.fields": "product_id",
  "auto.create": "true",
  "auto.evolve": "true",
  "db.timezone": "UTC",
  "tasks.max": "1",
  "schemas.enable": "false"
}' \
http://kafka-connect:8083/connectors/pg-sink/config
