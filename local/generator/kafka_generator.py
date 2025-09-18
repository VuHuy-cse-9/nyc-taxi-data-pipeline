import asyncio
import json
import logging
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from threading import Thread
from copy import deepcopy
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time
from typing import Callable
from random import randint

tz = ZoneInfo("Asia/Ho_Chi_Minh")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka settings
BOOTSTRAP_SERVERS = "localhost:9092"

async def create_topic(topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
    try:
        admin_client = AIOKafkaAdminClient(
            bootstrap_servers='localhost:9092',
            client_id='admin-client'
        )

        await admin_client.start()

        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )

        response = await admin_client.create_topics(new_topics=[topic], timeout_ms=10000)
        logger.info(f"Topic creation response: {response}")

    except Exception as e:
        logger.error(f"Unexpected error during topic creation: {e}", exc_info=True)
    return

async def consume_and_produce(topic: str, input_file: str,
                              pickup_datetime_key: str,
                              dropoff_datetime_key: str,
                              num_partitions: int = 1,
                              replication_factor: int = 1,
                              partition_by: str = None):
    try:
        await create_topic(
            topic, num_partitions=num_partitions, 
            replication_factor=replication_factor
        )

        # Create consumer and producer
        producer = AIOKafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        # Start consumer and producer
        await producer.start()

        # Load sample data
        with open(input_file, "r") as file:
            sample_data = json.loads(s=file.read())

        current_datetime = datetime.now(tz)

        i = 0
        partition_keys = [f"key_{i}" for i in range(num_partitions)]
        partitions = await producer.partitions_for(topic)
        partitions = list(partitions)
        print(partitions)
        while True:
            try:
                data = deepcopy(sample_data)

                pickup_datetime = current_datetime + timedelta(seconds=i)
                dropoff_datetime = current_datetime + timedelta(seconds=i+1800)

                # Out of order record
                data['payload']['after'][pickup_datetime_key] = pickup_datetime.strftime("%Y-%m-%d %H:%M:%S")
                data['payload']['after'][dropoff_datetime_key] = dropoff_datetime.strftime("%Y-%m-%d %H:%M:%S")
                # Produce to output topic
                if partition_by:
                    partition_id = randint(0, num_partitions - 1)
                    key = partition_keys[partition_id].encode('utf-8')
                    
                    await producer.send_and_wait(topic, data, key=key, partition=partitions[partition_id])
                else:
                    await producer.send_and_wait(topic, data)

                logger.info(f"Produced message to {topic}")

                i += 1

            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

            await asyncio.sleep(0.5)

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        # Ensure clean shutdown
        await producer.stop()


if __name__ == "__main__":
    # Run the async service
    fhhv_thread = Thread(
        target=lambda: asyncio.run(consume_and_produce(
            "raw.public.for_hire_vehicle", 
            "dataset/samples/kafka/debezium_for_hire_vehicle.json",
            "pickup_datetime",
            "dropoff_datetime",
            num_partitions=4,
            replication_factor=1,
            partition_by="hvfhs_license_num"
            )),
        daemon=True,
    )
    fhhv_thread.start()
    time.sleep(1)
    
    # green_taxi_thread = Thread(
    #     target=lambda: asyncio.run(consume_and_produce(
    #         "raw.public.green_taxi", 
    #         "dataset/samples/kafka/debezium_green_taxi.json",
    #         "lpep_pickup_datetime",
    #         "lpep_dropoff_datetime"
    #     )),
    #     daemon=True,
    # )
    # green_taxi_thread.start()
    # time.sleep(1)

    # yellow_taxi_thread = Thread(
    #    target=lambda: asyncio.run(consume_and_produce(
    #        "raw.public.yellow_taxi",
    #        "dataset/samples/kafka/debezium_yellow_taxi.json",
    #        "tpep_pickup_datetime",
    #        "tpep_dropoff_datetime"
    #    )),
    #    daemon=True,
    # )
    # yellow_taxi_thread.start()
    # time.sleep(1)

    # Keep the main thread alive
    fhhv_thread.join()
    # green_taxi_thread.join()
    # yellow_taxi_thread.join()
