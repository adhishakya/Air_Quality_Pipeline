import os
from dotenv import load_dotenv
from config import city, state, country
from utils.api_request import fetch_data_from_api

def extract_data():
    load_dotenv()
    api_key = os.getenv('API_KEY')
    url = f'http://api.airvisual.com/v2/city?city={city}&state={state}&country={country}&key={api_key}'
    return fetch_data_from_api(url)
