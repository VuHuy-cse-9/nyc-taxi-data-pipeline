import asyncio
import json
import logging
from aiokafka import AIOKafkaProducer
from threading import Thread
from copy import deepcopy
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

tz = ZoneInfo("Asia/Ho_Chi_Minh")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka settings
BOOTSTRAP_SERVERS = "localhost:9092"

async def consume_and_produce(topic: str, input_file: str,
                              pickup_datetime_key: str,
                              dropoff_datetime_key: str):
    try:
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

        print(f"Sample data loaded: {sample_data}")

        current_datetime = datetime.now(tz)

        i = 0
        while True:
            try:
                data = deepcopy(sample_data)

                pickup_datetime = current_datetime + timedelta(seconds=i)
                dropoff_datetime = current_datetime + timedelta(seconds=i+1800)

                # Out of order record
                data['payload']['after'][pickup_datetime_key] = pickup_datetime.strftime("%Y-%m-%d %H:%M:%S")
                data['payload']['after'][dropoff_datetime_key] = dropoff_datetime.strftime("%Y-%m-%d %H:%M:%S")
                # Produce to output topic
                await producer.send_and_wait(topic, data)

                logger.info(f"Produced message to {topic}")

                i += 1

            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

            await asyncio.sleep(0.1)

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
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
            "dropoff_datetime"
            )),
        daemon=True,
    )
    fhhv_thread.start()
    
    green_taxi_thread = Thread(
        target=lambda: asyncio.run(consume_and_produce(
            "raw.public.green_taxi", 
            "dataset/samples/kafka/debezium_green_taxi.json",
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime"
        )),
        daemon=True,
    )
    green_taxi_thread.start()

    yellow_taxi_thread = Thread(
       target=lambda: asyncio.run(consume_and_produce(
           "raw.public.yellow_taxi",
           "dataset/samples/kafka/debezium_yellow_taxi.json",
           "tpep_pickup_datetime",
           "tpep_dropoff_datetime"
       )),
       daemon=True,
    )
    yellow_taxi_thread.start()

    # Keep the main thread alive
    fhhv_thread.join()
    green_taxi_thread.join()
    yellow_taxi_thread.join()
