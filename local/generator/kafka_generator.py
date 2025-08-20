import asyncio
import json
import logging
from aiokafka import AIOKafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka settings
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "raw.public.yellow_taxi"
GROUP_ID = "kafka_produce_sample_group"


async def consume_and_produce():
    # Create consumer and producer
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    # Start consumer and producer
    await producer.start()

    # Load sample data
    with open("dataset/samples/debezium_yellow_taxi.json", "r") as file:
        sample_data = json.loads(s=file.read())

    print(f"Sample data loaded: {sample_data}")

    try:
        while True:
            try:
                # Produce to output topic
                await producer.send_and_wait(TOPIC, sample_data)

                logger.info(f"Produced message to {TOPIC}")

            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                # Optionally send to a dead-letter queue
                # await producer.send_and_wait("dlq.topic", msg.value)

            await asyncio.sleep(2)

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        # Ensure clean shutdown
        await producer.stop()


if __name__ == "__main__":
    # Run the async service
    asyncio.run(consume_and_produce())