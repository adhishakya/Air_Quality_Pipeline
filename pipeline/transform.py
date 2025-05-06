import pandas as pd
from pandas import json_normalize
from utils.aqi_remarks import get_remarks

def transform_data(json_response):
    df = json_normalize(json_response['data'])
    df.columns = df.columns.str.replace('.','_')


    df['current_weather_ts'] = pd.to_datetime(df['current_weather_ts']) # Convert to datetime
    df['current_weather_ts'] = df['current_weather_ts'].dt.tz_localize(None) # Remove timezone info

    df['remarks'] = df['current_pollution_aqius'].apply(get_remarks) # Get remarks based on AQI

    filter_columns = [
        'city',
        'country',
        'current_pollution_aqius',
        'current_weather_ts',
        'remarks',
    ]
    df_filtered = df[filter_columns]

    df_filtered = df_filtered.rename(columns={
        'city': 'City',
        'country': 'Country',
        'current_pollution_aqius': 'AQI',
        'current_weather_ts': 'AQI_Timestamp',
        'remarks': 'Remarks'
    })

    return df_filtered.to_dict(orient='records')