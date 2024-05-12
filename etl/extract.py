import requests
import ndjson
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

# Read api config
def load_config():
    with open('config/api_config.yml', 'r') as config_file:
        return yaml.safe_load(config_file)

# Define function to fetch data from lichess
def fetch_lichess_data(url, username):

    since = load_last_timestamp()
    until = datetime.now().strftime("%Y%m%d%H%M%S")

    url = f"{url}/{username}"
    headers = {"Accept": "application/x-ndjson"}
    params = {
        "since": since,
        "until": until,
        "max":3,
        "perfType": "ultraBullet, bullet, blitz",
        "analysed": True,
        "clocks": True,
        "opening": True,
        "sort": "dateAsc"
        }
    
    try:
        # Get api response
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status() # Raise an HTTPError for bad responses
        
        if response.status_code == 200:
            save_last_timestamp(until) # Save the current timestamp for future requests

        # Process ND-JSON response
        lichess_data = response.json(cls=ndjson.Decoder)

        return lichess_data
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data from Lichess for user {username}. Error: {e}")
        return None

# Define function to extract data
def extract_data():
    # Get lichess api url from config file
    api_config = load_config()
    lichess_api_url = api_config['lichess']['api_url']
    lichess_username = api_config['lichess']['user']
    
    lichess_data = fetch_lichess_data(lichess_api_url, lichess_username)

    if lichess_data is not None:
        if not lichess_data:
            logging.info(f"No new data on Lichess for user {lichess_username} since the last extraction.")
        else:
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            output_filename = f"data/raw/{lichess_username}_data_{timestamp}.ndjson"

            with open(output_filename, 'w') as output_file:
                writer = ndjson.writer(output_file, ensure_ascii=False)
                
                for data in lichess_data:
                    writer.writerow(data)

if __name__ == "__main__":
    try:
        extract_data()
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")