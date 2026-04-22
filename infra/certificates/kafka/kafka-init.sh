#!/bin/bash
set -e

BOOTSTRAP="kafka-1:9092,kafka-2:9092,kafka-3:9092"
CMD_CONFIG="/etc/kafka/client.properties"

kafka-topics --create --if-not-exists \
  --topic topic-1 \
  --partitions 3 \
  --replication-factor 2 \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CMD_CONFIG"

kafka-topics --create --if-not-exists \
  --topic topic-2 \
  --partitions 3 \
  --replication-factor 2 \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CMD_CONFIG"

# topic-1: practicum-1 can produce and consume
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$CMD_CONFIG" \
  --add --allow-principal "User:practicum-1" \
  --operation Write --topic topic-1

kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$CMD_CONFIG" \
  --add --allow-principal "User:practicum-1" \
  --operation Read --topic topic-1

kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$CMD_CONFIG" \
  --add --allow-principal "User:practicum-1" \
  --operation Read --group "*"

# topic-2: practicum-2 can only produce, not consume
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$CMD_CONFIG" \
  --add --allow-principal "User:practicum-2" \
  --operation Write --topic topic-2
