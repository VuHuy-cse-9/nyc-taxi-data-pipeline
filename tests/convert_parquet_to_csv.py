import pandas as pd


df = pd.read_parquet("dataset/yellow_taxi/yellow_tripdata_2025-05.parquet")
df.to_csv("dataset/samples/csv/yellow_taxi.csv", index=False)