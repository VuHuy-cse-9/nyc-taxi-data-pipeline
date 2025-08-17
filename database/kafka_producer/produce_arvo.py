import avro
import asyncio
from dotenv import load_dotenv
import os
from schema_registry.client import SchemaRegistryClient, schema
import json
import io
import pandas as pd
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka import AIOKafkaProducer

load_dotenv()

def get_schema_registry_client():
    schema_registry_endpoint = os.getenv("SCHEMA_REGISTRY_ENDPOINT")
    print(f"SCHEMA_REGISTRY_ENDPOINT: {schema_registry_endpoint}")

    client = SchemaRegistryClient(schema_registry_endpoint)

    return client

def get_kafka_client():
    servers = os.getenv('KAFKA_SERVER', 'localhost:9092')
    try:
        admin = AIOKafkaAdminClient(bootstrap_servers=[servers])
        producer = AIOKafkaProducer(bootstrap_servers=[servers])
        print("SUCCESS: instantiated Kafka admin and producer")
    except Exception as e:
        print(f"ERROR: Failed to create Kafka admin client: {e}")
        
    return admin, producer

async def create_topic(admin: AIOKafkaAdminClient, topic_name):
    # Create topic if not exists
    try:
        # Create Kafka topic
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        await admin.create_topics([topic])
        print(f"A new topic {topic_name} has been created!")
    except Exception:
        print(f"Topic {topic_name} already exists. Skipping creation!")
        pass

async def main():
    # Create schema client
    schema_client = get_schema_registry_client()

    # Load the schema
    schema_path = "data_ingestion/kafka_producer/schemas/schema_for_hire_car.avsc"
    with open(schema_path, "r") as f:
        deployment_schema = json.load(f)

    avro_schema = schema.AvroSchema(deployment_schema)

    schema_version_info = schema_client.check_version(
            "for-hire-vehicle", avro_schema)

    if schema_version_info is not None:
        schema_id = schema_version_info.schema_id
        print(f"Found an existing schema ID: {schema_id}. Skipping creation!")
    else:
        schema_id = schema_client.register("for-hire-vehicle", avro_schema)
        print(f"Schema registered with ID: {schema_id}")

    topic_name = "for-hire-vehicle"

    # Create topic if not exists.
    admin, producer = get_kafka_client()

    # Avro Encoder
    schema.AvroSchema(deployment_schema)
    writer = avro.io.DatumWriter(avro_schema)

    # Load sample dataset
    licence2companies = {
        'HVV0002': 'Jun',
        'HVV0003': 'Ube',
        'HVV0004': 'Via',
        'HVV0005': 'Lyft',
    }

    await create_topic(admin, topic_name)
        
    df = pd.read_parquet('dataset/fhvhv_tripdata_2025-05.parquet')

    for id, record in df.iterrows():
        bytes_writer = io.BytesIO()
        # Write the Confluence "Magic Byte"
        bytes_writer.write(bytes([0]))

        # Write schema ID
        bytes_writer.write(int.to_bytes(schema_id, 4, byteorder="big"))

        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(record, encoder)

        key = licence2companies[record['hvfhs_license_num']]

        await producer.send_and_wait(
            topic_name,
            key=key.encode('utf-8'),
            value=bytes_writer.getvalue()   
        )

        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())