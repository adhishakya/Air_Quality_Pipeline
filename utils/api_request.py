import requests
import logging

def fetch_data_from_api(url):
    try:
        response = requests.get(url) 
        response.raise_for_status() # Raise an error for bad responses
        json_response = response.json()
        return json_response
    except Exception as e:
        logging.error(f"Error fetching data from API: {e}")
        return None