from feast import FeatureStore


def get_feature_store() -> FeatureStore:
    """Initialize and return the Feast FeatureStore instance."""
    return FeatureStore(repo_path="./feature_store")
