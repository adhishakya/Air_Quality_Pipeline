import requests
import os
from dotenv import load_dotenv
import pandas as pd
from pandas import json_normalize
import psycopg2
import logging

logging.basicConfig(level = logging.INFO)

load_dotenv()

api_key = os.getenv('API_KEY')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')

search_city = 'Kathmandu'
search_state = 'Central Region'
search_country = 'Nepal'
url = f'http://api.airvisual.com/v2/city?city={search_city}&state={search_state}&country={search_country}&key={api_key}'

try:
    response = requests.get(url)
    response.raise_for_status()  
    json_response = response.json()

except Exception as e:
    logging.error(f"Error fetching data from API: {e}")
    exit()

df = json_normalize(json_response['data'])
df.columns = df.columns.str.replace('.','_')

df['current_weather_ts'] = pd.to_datetime(df['current_weather_ts'])
df['current_weather_ts'] = df['current_weather_ts'].dt.tz_localize(None)

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

try:
    with psycopg2.connect(
        database = db_name,
        user = db_user,
        password = db_password,
        host = db_host,
        port = db_port
    ) as conn:
        with conn.cursor() as cursor:
            try:
                with open('sql/create_table.sql', 'r') as file:
                    create_table_query = file.read()
                
                with open('sql/insert_data.sql', 'r') as file:
                    insert_data_query = file.read()
            
            except FileNotFoundError as e:
                logging.error(f'SQL file not found: {e}')
                exit()

            except Exception as e:
                logging.error(f'Error reading SQL file: {e}')
                exit()

            cursor.execute(create_table_query)
            logging.info('Table exists or created successfully')

            values = [
                (
                    value['City'],
                    value['Country'],
                    value['AQI'],
                    value['Timestamp'],
                    value['Remarks']
                )
                for value in data
            ]

            try:    
                cursor.executemany(insert_data_query, values)
                conn.commit()
                logging.info('Data inserted successfully')
            except Exception as e:
                logging.error(f'Error inserting data: {e}')
                exit()

except Exception as e:
    logging.error(f'Error while inserting data into database: {e}')
