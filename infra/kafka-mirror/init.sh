#!/bin/bash
set -e

BOOTSTRAP="kafka-mirror-1:9092,kafka-mirror-2:9092,kafka-mirror-3:9092"
CMD_CONFIG="/etc/kafka/client.properties"

PARTITIONS=3
REPLICATION=2
TOPICS=(
  "recommendations"
)

for TOPIC in ${TOPICS[@]}; do
  kafka-topics --create --if-not-exists \
    --topic "$TOPIC" \
    --partitions $PARTITIONS \
    --replication-factor $REPLICATION \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$CMD_CONFIG"
done
