#!/usr/bin/env bash
set -e

echo "➡️ Register Debezium connector 1"
/app/run.sh register_connector /app/datasource1-cdc-avro.json

echo "➡️ Register Debezium connector 2"
/app/run.sh register_connector /app/datasource3-mongo.json

echo "➡️ Register Sink connector"
/app/run.sh register_connector /app/postgresql_sink-json.json

echo "✅ All connectors registered successfully"
