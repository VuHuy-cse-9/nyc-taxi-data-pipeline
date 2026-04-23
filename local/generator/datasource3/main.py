from pymongo import AsyncMongoClient
import pandas as pd
import asyncio
import logging
from dotenv import load_dotenv
import os
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


def intialize_connection():
    while True:
        try:

            host = os.getenv("DATASOURCE3_HOST", "datasource3")
            port = os.getenv("DATASOURCE3_PORT", "27017")
            client = AsyncMongoClient(f"mongodb://{host}:{port}/?replicaSet=rs0")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            logger.info("Retrying in 5 seconds...")
            time.sleep(5)
        

async def main():
     # Initialize MongoDB connection
    client = intialize_connection()
    db = client.datasource3
    collection = db.fhvhv_taxi
    try:
        # Load dataset
        df = pd.read_csv("/data/fhvhv.csv")
        df = df.sort_values(by="pickup_datetime").reset_index(drop=True)

        for index, row in df.iterrows():
            record = {
                    key: v if pd.notna(v) else None for key, v in row.items()
                }
            # Intsert records
            await collection.insert_one(record)

            await asyncio.sleep(2.0)
            logger.info(f"Inserted record {index + 1}/{len(df)}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        await client.close()


    return


if __name__ == "__main__":
    asyncio.run(main())