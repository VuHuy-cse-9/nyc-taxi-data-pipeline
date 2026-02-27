from pydantic import BaseModel
from datetime import datetime


class FeatureRequest(BaseModel):
    """Request schema for querying online feature store."""
    pulocationid: int
    event_timestamp: datetime


class FeatureResponse(BaseModel):
    """Response schema containing retrieved features from online feature store."""
    pulocationid: int
    event_timestamp: datetime
    trip_miles: float
    fare_amount: float
