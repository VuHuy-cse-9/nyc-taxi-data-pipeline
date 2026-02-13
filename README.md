# NYC Data Processing Pipeline

![](./assets/mlops2_architecture.png)

# Install dependency
uv sync --extra spark
uv sync --extra flink

# Streaming pipeline
## 1. Register connector to Debezium
Source CDC:
./local/debezium/run.sh register_connector ./local/debezium/datasource1-cdc-avro.json
./local/debezium/run.sh register_connector ./local/debezium/datasource3-mongo.json
Sink CDC:
./local/kafka_connect/run.sh register_connector ./local/kafka_connect/postgresql_sink-json.json

## 2. Generate data
python3 -m local.generator.datasource1_generator
python3 -m local.generator.datasource2_cassandra
python3 -m local.generator.datasource3_mongodb

## Debug
docker compose -f docker-compose.stream.yml up -d
python3 -m local.generator.kafka_preprocess_generator
python3 -m stream_processing.main

## Note on MongoDB setup
Step 1: Start mongodb with docker compose.
Step 2: Execute into primary node, run: `mongosh`. Copy those code in mongodb/init-replica.js to start the replica set.
Step 3: In /etc/hosts add the following lines. This allows host resolution for the services.
```
127.0.0.1 datasource3
```


## 3. Note
Trino and its services are used to query and visualize cassandra database.
Use mongoDB Compass to visualize mongodb database.
Use DBeaver to visualize postgresql database.