import os
from dotenv import load_dotenv
import pandas as pd
from pandas import json_normalize
import logging
from utils.db_connection import get_db_connection
from utils.api_request import fetch_data_from_api
from utils.aqi_remarks import get_remarks
from utils.load_queries import load_queries

logging.basicConfig(level = logging.INFO)

load_dotenv()

api_key = os.getenv('API_KEY')

search_city = 'Kathmandu'
search_state = 'Central Region'
search_country = 'Nepal'
url = f'http://api.airvisual.com/v2/city?city={search_city}&state={search_state}&country={search_country}&key={api_key}'

json_response = fetch_data_from_api(url)

df = json_normalize(json_response['data'])
df.columns = df.columns.str.replace('.','_')

df['current_weather_ts'] = pd.to_datetime(df['current_weather_ts'])
df['current_weather_ts'] = df['current_weather_ts'].dt.tz_localize(None)

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
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            create_table_query, insert_data_query = load_queries()

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
