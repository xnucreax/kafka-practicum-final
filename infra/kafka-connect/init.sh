#!/bin/sh

curl -X PUT \
-H "Content-Type: application/json" \
--data '{
  "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
  "connection.url": "jdbc:postgresql://postgres:5432/postgres",
  "connection.username": "postgres",
  "connection.password": "postgres",
  "topics": "products",
  "table.name.format": "products",
  "insert.mode": "upsert",
  "primary.key.mode": "record_value",
  "primary.key.fields": "product_id",
  "auto.create": "true",
  "auto.evolve": "true",
  "db.timezone": "UTC",
  "tasks.max": "1",
  "consumer.override.security.protocol": "SASL_PLAINTEXT",
  "consumer.override.sasl.mechanism": "PLAIN",
  "consumer.override.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka-connect\" password=\"kafka-connect-password\";"
}' \
http://kafka-connect:8083/connectors/pg-sink/config
