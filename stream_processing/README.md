# GUIDE

1. Zip the folder: zip -r deps.zip parsers preprocess schemas.py online_feat.py
2. Submit job: flink run -m localhost:8090 -py main.py --pyFiles deps.zip


--- IGNORE ---
broker:29092 -> broker:29092
