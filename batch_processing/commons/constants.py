DATA2MINIO_DIR = {
    'for-hire-vehicle_dir': 'nyc_taxi_dataset/for_hire_vehicle',
    'green-taxi_dir': 'nyc_taxi_dataset/green_taxi',
    'yellow-taxi_dir': 'nyc_taxi_dataset/yellow_taxi',
    'taxi_zone': 'nyc_taxi_dataset/metadata/taxi_zone_lookup.csv',
    'fh_license_affiliation': 'nyc_taxi_dataset/metadata/High_Volume_License_Numbers_and_Affiliations.csv',
    'green_taxi_dictionary': 'nyc_taxi_dataset/metadata/green_taxi_dictionary.csv',
    'yellow_taxi_dictionary': 'nyc_taxi_dataset/metadata/yellow_taxi_dictionary.csv',
    'fhvhv_dictionary': 'nyc_taxi_dataset/metadata/High_Volume_FHV_trip_data_dictionary.csv'
}

NYC_TAXI_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

SOURCE2MINIO_FOLDER = {
    "green": "green_taxi",
    "yellow": "yellow_taxi",
    "fhvhv": "for_hire_vehicle"
}