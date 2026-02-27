from fastapi import FastAPI
import pandas as pd
from typing import List

from schemas import FeatureRequest, FeatureResponse
from configs.feature_store import get_feature_store

app = FastAPI()
feature_store = get_feature_store()


@app.post("/get-features")
async def get_features(requests: List[FeatureRequest]) -> List[FeatureResponse]:
    """
    Retrieve features from the online feature store.
    
    Args:
        requests: List of FeatureRequest objects containing pulocationid and event_timestamp.
    
    Returns:
        List of FeatureResponse objects with retrieved features.
    """
    # Convert request objects to DataFrame format
    entity_rows = [
        {
            "pulocationid": req.pulocationid,
            "event_timestamp": req.event_timestamp,
        }
        for req in requests
    ]
    
    # Retrieve features from online feature store
    df = feature_store.get_online_features(
        entity_rows=entity_rows,
        features=[
            "green_taxi_features:trip_miles",
            "green_taxi_features:fare_amount",
        ],
    ).to_df()
    
    # Convert DataFrame rows to FeatureResponse objects
    responses = [
        FeatureResponse(
            pulocationid=int(row["pulocationid"]),
            event_timestamp=row["event_timestamp"],
            trip_miles=float(row["green_taxi_features__trip_miles"]),
            fare_amount=float(row["green_taxi_features__fare_amount"]),
        )
        for _, row in df.iterrows()
    ]
    
    return responses


