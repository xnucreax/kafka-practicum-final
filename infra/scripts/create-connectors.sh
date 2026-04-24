#!/bin/sh

curl -X PUT \
-H "Content-Type: application/json" \
--data '{
"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
"database.hostname": "postgres",
"database.port": "5432",
"database.user": "postgres",
"database.password": "postgres",
"database.dbname": "postgres",
"database.server.name": "postgres",
"table.include.list": "public.users, public.orders",
"transforms": "unwrap",
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
"transforms.unwrap.drop.tombstones": "false",
"transforms.unwrap.delete.handling.mode": "rewrite",
"topic.prefix": "debezium",
"topic.creation.enable": "true",
"topic.creation.default.replication.factor": "3",
"topic.creation.default.partitions": "3",
"skipped.operations": "none"
}' \
http://localhost:8083/connectors/pg-debezium/config | jq


