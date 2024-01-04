import requests
import json
from datetime import datetime, timedelta
import os
import yaml
import logging

# Implement logger
logging.basicConfig(filename='extract.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s: %(message)s')

# Define function to load last timestamp in config
def load_last_timestamp():
    try:
        with open('config/last_timestamp.txt', 'r') as timestamp_file:
            return timestamp_file.read().strip()
    except FileNotFoundError:
        return None

# Define function to save timestamp to config
def save_last_timestamp(timestamp):
    with open('config/last_timestamp.txt', 'w') as timestamp_file:
        timestamp_file.write(str(timestamp))

# Read api config file
with open('config/api_config.yml', 'r') as config_file:
    api_config = yaml.safe_load(config_file)

# Get lichess api url from config file
lichess_api_url = api_config['lichess']['api_url']
lichess_username = api_config['lichess']['user']

# Define function to fetch data from lichess
def fetch_lichess_data(username):
    
    try:
        since = load_last_timestamp()
        until = datetime.now().strftime("%Y%m%d%H%M%S")
        
        url = f"{lichess_api_url}/{username}"    
        headers = {"Accept": "application/x-ndjson"}
        params = {
            "since": since,
            "until": until,
            "max":2,
            "perfType": "ultraBullet, bullet, blitz",
            "analysed": True,
            "pgnInJson": True,
            "clocks": True,
            "evals": True,
            "opening": True,
            "sort": "dateAsc"
        }
        
        response = requests.get(url, params=params, headers=headers, stream=True)
        response.raise_for_status() # Raise an HTTPError for bad responses
        
        if response.status_code == 200:
            save_last_timestamp(until) # Save the current timestamp for future requests

        # Process NDJSON response
        lichess_data = []
        for line in response.iter_lines():
            if line:
                lichess_data.append(json.loads(line.decode('utf-8')))

        return lichess_data
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data from Lichess for user {username}. Error: {e}")
        return None

# Define function to extract data
def extract_data(username):
    lichess_data = fetch_lichess_data(username)

    if lichess_data is not None:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        output_filename = f"data/raw/{username}_data_{timestamp}.json"

        with open(output_filename, 'w') as output_file:
            json.dump({'lichess': lichess_data}, output_file)

if __name__ == "__main__":
    try:
        username = lichess_username
        extract_data(username)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")