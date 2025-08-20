import os

from dotenv import load_dotenv
from psycopg_pool import AsyncConnectionPool
import logging
import asyncio
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS yellow_taxi (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    VendorID INT,
    tpep_pickup_datetime VARCHAR(30),
    tpep_dropoff_datetime VARCHAR(30),
    passenger_count INT,
    trip_distance FLOAT,
    RatecodeID INT,
    store_and_fwd_flag VARCHAR(1),
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    cbd_congestion_fee FLOAT
);
"""

INSERT_QUERY = """
INSERT INTO yellow_taxi (
    VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
    trip_distance, RatecodeID, store_and_fwd_flag, PULocationID,
    DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount,
    tolls_amount, improvement_surcharge, total_amount, airport_fee,
    congestion_surcharge, cbd_congestion_fee
) VALUES (
    %(VendorID)s, %(tpep_pickup_datetime)s, %(tpep_dropoff_datetime)s, %(passenger_count)s,
    %(trip_distance)s, %(RatecodeID)s, %(store_and_fwd_flag)s, %(PULocationID)s,
    %(DOLocationID)s, %(payment_type)s, %(fare_amount)s, %(extra)s, %(mta_tax)s, %(tip_amount)s,
    %(tolls_amount)s, %(improvement_surcharge)s, %(total_amount)s, %(airport_fee)s,
    %(congestion_surcharge)s, %(cbd_congestion_fee)s
)"""

def initialize_db_connection(min_size=2, max_size=50)->AsyncConnectionPool:
    DB_HOST = os.getenv("DATASOURCE2_HOST")
    DB_PORT = os.getenv("DATASOURCE2_PORT", "5432")
    DB_NAME = os.getenv("DATASOURCE2_DB", "default")
    DB_USER = os.getenv("DATASOURCE2_USER", "datasource2")
    DB_PASSWORD = os.getenv("DATASOURCE2_PASSWORD", "datasource2")
    db_conn_info = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    logger.info(f"Connecting to PostgreSQL database at {db_conn_info}")
    try:
        pool = AsyncConnectionPool(
            db_conn_info,
            min_size=min_size, max_size=max_size,
            timeout=300,
        )
        logger.info(f"PostgreSQL connection pool created with min_size={min_size} and max_size={max_size}")
    except Exception as e:
        logger.error(f"Failed to create PostgreSQL connection pool: {e}")
        return None
    return pool

async def create_table(pool: AsyncConnectionPool, create_table_query: str):
    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute(create_table_query)
                await conn.commit()
                logger.info("Table created successfully")
            except Exception as e:
                logger.error(f"Failed to create table: {e}")
                await conn.rollback()
                logger.error("Rolled back the transaction due to error")


async def insert_record(pool: AsyncConnectionPool, insert_query: str, record: dict):
    logger.info(f"Inserting record: {record}")
    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute(insert_query, record)
                await conn.commit()
                logger.info("Record inserted successfully")
            except Exception as e:
                logger.error(f"Failed to insert record: {e}")
                await conn.rollback()
                logger.error("Rolled back the transaction due to error")
    return

async def main():
    # Initialize the database connection pool
    pool = initialize_db_connection()
    if pool is None:
        logger.error("Could not initialize database connection pool. Exiting.")
        return

    # Create the table
    try:
        await create_table(pool, CREATE_TABLE_QUERY)

        # Read yellow taxi data
        yellow_taxi_df = pd.read_csv("dataset/yellow_tripdata_2025-05.csv")
        yellow_taxi_df = yellow_taxi_df.rename(columns={
            'Airport_fee': 'airport_fee',
        })
        yellow_taxi_df = yellow_taxi_df.sort_values(by="tpep_pickup_datetime").reset_index(drop=True)
        for index, row in yellow_taxi_df.iterrows():
            record = {
                key: v if pd.notna(v) else None for key, v in row.items()
            }
            await insert_record(pool, INSERT_QUERY, record)

            await asyncio.sleep(2)  # To avoid overwhelming the database with too many requests
            logger.info(f"Inserted record {index + 1}/{len(yellow_taxi_df)}")
    
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
