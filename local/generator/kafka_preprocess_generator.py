
import asyncio
import json
import logging
from aiokafka import AIOKafkaProducer
from threading import Thread
from datetime import datetime, timedelta
from numpy import record
from copy import deepcopy

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka settings
BOOTSTRAP_SERVERS = "localhost:9092"

sample = {
  "id": "7925841a-1ac6-477d-bb7b-ba3a97d6ca13",
  "vendorid": 2,
  "passenger_count": 1,
  "trip_miles": 5.6,
  "ratecodeid": 1,
  "pulocationid": 179,
  "dolocationid": 107,
  "fare_amount": 31,
  "extra": 2.5,
  "mta_tax": 0.5,
  "tip_amount": 0,
  "tolls_amount": 0,
  "improvement_surcharge": 1,
  "total_amount": 38.5,
  "congestion_surcharge": 2.75,
  "cbd_congestion_fee": 0.75,
  "pickup_datetime": "2025-01-15 18:57:14",
  "dropoff_datetime": "2025-01-15 19:26:09",
  "store_and_fwd_flag": True,
  "trip_type": "Street-hail",
  "payment_type": "Credit Card",
  "airport_fee": 0,
  "taxi_type": "green_taxi",
  "trip_duration": 1735
}

async def consume_and_produce(topic: str):
    try:
        # Create consumer and producer
        producer = AIOKafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        # Start consumer and producer
        await producer.start()

        print(f"Sample data loaded: {sample}")
       
        i = 0
        delay = 0.1
        current_date = datetime.now()
        while True:
            i += 1
            sample_data = deepcopy(sample)
            
            # if (i % 2 == 0) and (i != 0):
            #     pickup_datetime = current_date + timedelta(seconds=i + 6) # >= Window End
            #     dropoff_datetime = current_date + timedelta(seconds=i+1800)
            # else:
            pickup_datetime = current_date + timedelta(seconds=i)
            dropoff_datetime = current_date + timedelta(seconds=i+1800)

            # Out of order record
            sample_data['pickup_datetime'] = pickup_datetime.strftime("%Y-%m-%d %H:%M:%S")
            sample_data['dropoff_datetime'] = dropoff_datetime.strftime("%Y-%m-%d %H:%M:%S")
            
            try:
                # Produce to output topic
                await producer.send_and_wait(topic, sample_data)

                logger.info(f"{pickup_datetime.strftime('%H:%M:%S')} - {dropoff_datetime.strftime('%H:%M:%S')}")

            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

            await asyncio.sleep(delay)

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        # Ensure clean shutdown
        await producer.stop()


if __name__ == "__main__":
    # Run the async service
    asyncio.run(consume_and_produce(
        "preprocess.public.traditional_taxi"))