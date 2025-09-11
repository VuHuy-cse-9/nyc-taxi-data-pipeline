from cassandra.cluster import Cluster
import pandas as pd
import time
import logging
from uuid import uuid4

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

INSERT_QUERY = """
INSERT INTO default.user_account (id, name, email, value)
VALUES (?, ?, ?, ?)
"""

CREATE_KEYSPACE = """
CREATE KEYSPACE IF NOT EXISTS default 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
"""

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS default.user_account (
    id UUID PRIMARY KEY,
    name TEXT,
    email TEXT,
    value INT
)
"""

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS default.yellow_taxi (
    id UUID PRIMARY KEY,
    VendorID INT,
    tpep_pickup_datetime VARCHAR,
    tpep_dropoff_datetime VARCHAR,
    passenger_count FLOAT,
    trip_distance FLOAT,
    RatecodeID FLOAT,
    store_and_fwd_flag VARCHAR,
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
) WITH cdc=true;
"""

INSERT_QUERY = """
INSERT INTO default.yellow_taxi (
    id, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
    trip_distance, RatecodeID, store_and_fwd_flag, PULocationID,
    DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount,
    tolls_amount, improvement_surcharge, total_amount, airport_fee,
    congestion_surcharge, cbd_congestion_fee
) VALUES (
    %(id)s, %(VendorID)s, %(tpep_pickup_datetime)s, %(tpep_dropoff_datetime)s, %(passenger_count)s,
    %(trip_distance)s, %(RatecodeID)s, %(store_and_fwd_flag)s, %(PULocationID)s,
    %(DOLocationID)s, %(payment_type)s, %(fare_amount)s, %(extra)s, %(mta_tax)s, %(tip_amount)s,
    %(tolls_amount)s, %(improvement_surcharge)s, %(total_amount)s, %(airport_fee)s,
    %(congestion_surcharge)s, %(cbd_congestion_fee)s
)"""

DROP_TABLE = """
DROP TABLE IF EXISTS default.yellow_taxi;
"""


def main():
    cluster = Cluster(["localhost"])
    session = cluster.connect()

    print("Creating keyspace and table...")
    session.execute(CREATE_KEYSPACE)
    session.execute(DROP_TABLE)
    time.sleep(2)  # Wait for keyspace to be fully created
    session.execute(CREATE_TABLE)
    print("Inserting sample records...")
    yellow_taxi_df = pd.read_csv("dataset/samples/csv/yellow_taxi.csv")
    yellow_taxi_df = yellow_taxi_df.rename(columns={
            'Airport_fee': 'airport_fee',
        })
    yellow_taxi_df = yellow_taxi_df.sort_values(by="tpep_pickup_datetime").reset_index(drop=True)

    for index, row in yellow_taxi_df.iterrows():
        record = {
            key: v if pd.notna(v) else None for key, v in row.items()
        }
        record['id'] = uuid4()
        logger.info(f"Inserting record: {record}")
        session.execute(INSERT_QUERY, record)

        time.sleep(2)  # To avoid overwhelming the database with too many requests
        logger.info(f"Inserted record {index + 1}/{len(yellow_taxi_df)}")

    return

if __name__ == "__main__":
    main()