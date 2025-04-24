import requests
import os
from dotenv import load_dotenv
import pandas as pd
from pandas import json_normalize

load_dotenv()

api_key = os.getenv('API_KEY')
search_city = 'Kathmandu'
search_state = 'Central Region'
search_country = 'Nepal'

url = f'http://api.airvisual.com/v2/city?city={search_city}&state={search_state}&country={search_country}&key={api_key}'
response = requests.get(url)
json_response = response.json()

df = json_normalize(json_response['data'])
df.columns = df.columns.str.replace('.','_')

df['current_weather_ts'] = pd.to_datetime(df['current_weather_ts'])

def get_remarks(aqi):
    if aqi <= 50:
        return 'Good'
    elif aqi <= 100:
        return 'Moderate'
    elif aqi <= 150:
        return 'Unhealthy for Sensitive Groups'
    elif aqi <= 200:
        return 'Unhealthy'
    elif aqi <= 300:
        return 'Very Unhealthy'
    else:
        return 'Hazardous'

df['remarks'] = df['current_pollution_aqius'].apply(get_remarks)

filter_columns = [
    'city',
    'state',
    'country',
    'current_pollution_aqius',
    'current_weather_ts',
    'remarks',
]
df_filtered = df[filter_columns]

df_filtered = df_filtered.rename(columns={
    'city': 'City',
    'state': 'State',
    'country': 'Country',
    'current_pollution_aqius': 'AQI',
    'current_weather_ts': 'Timestamp',
    'remarks': 'Remarks'
})

data = df_filtered.to_dict(orient='records')

print(data)

