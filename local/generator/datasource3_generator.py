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
CREATE TABLE IF NOT EXISTS for_hire_vehicle (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hvfhs_license_num VARCHAR(10),
    dispatching_base_num VARCHAR(10),
    originating_base_num VARCHAR(10),
    request_datetime VARCHAR(30),
    on_scene_datetime VARCHAR(30),
    pickup_datetime VARCHAR(30),
    dropoff_datetime VARCHAR(30),
    PULocationID INT,
    DOLocationID INT,
    trip_miles FLOAT,
    trip_time INT,
    base_passenger_fare FLOAT,
    tolls FLOAT,
    bcf FLOAT,
    sales_tax FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    tips FLOAT,
    driver_pay FLOAT,
    shared_request_flag VARCHAR(1),
    shared_match_flag VARCHAR(1),
    access_a_ride_flag VARCHAR(1),
    wav_request_flag VARCHAR(1),
    wav_match_flag VARCHAR(1),
    cbd_congestion_fee FLOAT
);
"""

INSERT_QUERY = """
INSERT INTO for_hire_vehicle (
    hvfhs_license_num, dispatching_base_num, originating_base_num, request_datetime,
    on_scene_datetime, pickup_datetime, dropoff_datetime, PULocationID,
    DOLocationID, trip_miles, trip_time, base_passenger_fare, tolls,
    bcf, sales_tax, congestion_surcharge, airport_fee, tips,
    driver_pay, shared_request_flag, shared_match_flag, access_a_ride_flag,
    wav_request_flag, wav_match_flag, cbd_congestion_fee
) VALUES (
    %(hvfhs_license_num)s, %(dispatching_base_num)s, %(originating_base_num)s, %(request_datetime)s,
    %(on_scene_datetime)s, %(pickup_datetime)s, %(dropoff_datetime)s, %(PULocationID)s,
    %(DOLocationID)s, %(trip_miles)s, %(trip_time)s, %(base_passenger_fare)s, %(tolls)s,
    %(bcf)s, %(sales_tax)s, %(congestion_surcharge)s, %(airport_fee)s,
    %(tips)s, %(driver_pay)s, %(shared_request_flag)s, %(shared_match_flag)s,
    %(access_a_ride_flag)s, %(wav_request_flag)s, %(wav_match_flag)s, %(cbd_congestion_fee)s
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

        # Read data
        df = pd.read_csv("dataset/fhvhv/fhvhv_sample.csv/part-00000-54786787-dda7-45ca-91e2-a4e2f413b4c2-c000.csv")
        df = df.sort_values(by="pickup_datetime").reset_index(drop=True)
        for index, row in df.iterrows():
            record = {
                key: v if pd.notna(v) else None for key, v in row.items()
            }
            await insert_record(pool, INSERT_QUERY, record)

            await asyncio.sleep(2)  # To avoid overwhelming the database with too many requests
            logger.info(f"Inserted record {index + 1}/{len(df)}")
    
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
