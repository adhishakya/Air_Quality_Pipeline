from pipeline.extract import extract_data
from pipeline.transform import transform_data
from pipeline.load import load_data

def start_etl():
    json_data = extract_data()
    transformed_json_data = transform_data(json_data)
    load_data(transformed_json_data)