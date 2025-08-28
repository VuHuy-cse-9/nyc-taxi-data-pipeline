import asyncio
import json
import logging
from aiokafka import AIOKafkaProducer
from threading import Thread

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka settings
BOOTSTRAP_SERVERS = "localhost:9092"

async def consume_and_produce(topic: str, input_file: str):
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

    
        while True:
            try:
                # Produce to output topic
                await producer.send_and_wait(topic, sample_data)

                logger.info(f"Produced message to {topic}")

            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

            await asyncio.sleep(0.5)

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
            "dataset/samples/kafka/debezium_for_hire_vehicle.json")),
        daemon=True,
    )
    fhhv_thread.start()
    
    green_taxi_thread = Thread(
        target=lambda: asyncio.run(consume_and_produce(
            "raw.public.green_taxi", 
            "dataset/samples/kafka/debezium_green_taxi.json")),
        daemon=True,
    )
    green_taxi_thread.start()

    yellow_taxi_thread = Thread(
       target=lambda: asyncio.run(consume_and_produce(
           "raw.public.yellow_taxi",
           "dataset/samples/kafka/debezium_yellow_taxi.json")),
       daemon=True,
    )
    yellow_taxi_thread.start()

    # Keep the main thread alive
    fhhv_thread.join()
    green_taxi_thread.join()
    yellow_taxi_thread.join()
