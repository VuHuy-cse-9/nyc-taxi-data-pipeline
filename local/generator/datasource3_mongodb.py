from pymongo import AsyncMongoClient
import pandas as pd
import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def intialize_connection():
    client = AsyncMongoClient("mongodb://localhost:27017/?replicaSet=rs0")
    return client

async def main():
     # Initialize MongoDB connection
    client = intialize_connection()
    db = client.datasource3
    collection = db.fhvhv_taxi
    try:
        # Load dataset
        df = pd.read_csv("dataset/samples/csv/fhvhv.csv")
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