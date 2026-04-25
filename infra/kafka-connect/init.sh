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
  "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false"
}' \
http://kafka-connect:8083/connectors/pg-sink/config

curl -X PUT \
-H "Content-Type: application/json" \
--data '{
  "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
  "connection.url": "jdbc:postgresql://postgres:5432/postgres",
  "connection.username": "postgres",
  "connection.password": "postgres",
  "topics": "client-requests",
  "table.name.format": "client_requests",
  "insert.mode": "insert",
  "auto.create": "false",
  "db.timezone": "UTC",
  "tasks.max": "1",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "true"
}' \
http://kafka-connect:8083/connectors/pg-client-requests-sink/config
