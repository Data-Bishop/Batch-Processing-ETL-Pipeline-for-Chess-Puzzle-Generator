import requests
import ndjson
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
import os
from dotenv import load_dotenv
import logging

# Load environment variables from .env
load_dotenv()

# Define config path and data directory
config_path = Path("config")
data_dir = Path("data") / "raw"

# Implement logger
logging.basicConfig(
    filename='extract.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s'
)

def load_last_timestamp() -> Optional[str] :
    """Loads the last timetamp used for extraction (if available)."""
    
    try:
        timestamp_path = config_path / "last_timestamp.txt"
        return timestamp_path.read_text().strip()
    except FileNotFoundError:
        logging.warning("Last timestamp not found...")
        return None

def save_last_timestamp(timestamp: str) -> None:
    """Saves the current timestamp for future data extraction."""
    
    timestamp_path = config_path / "last_timestamp.txt"
    with open(timestamp_path, 'w') as timestamp_file:
        timestamp_file.write(str(timestamp))

def fetch_lichess_data(url:str, username:str) -> Optional[list]:
    """Fetches chess game data from Lichess API in ND-JSON format

    Args:
        url (str): Lichess API endpoint URL.
        username (str): Username to fetch games for.

    Returns:
        Optional[List]: List of chess game data dictionaries or none on error
    """
    
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
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        
        if response.status_code == 200:
            save_last_timestamp(until)

        # Process ND-JSON response
        lichess_data = response.json(cls=ndjson.Decoder)

        return lichess_data
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error occurred: {e}")
    except requests.exceptions.ConnectionError as e:
        logging.error(f"Connection error occurred: {e}")
    except requests.exceptions.Timeout as e:
        logging.error(f"Timeout error occurred: {e}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data from Lichess for user {username}. Error: {e}")

    return None

def write_ndjson_data(data: Any, output_path: Path) -> None:
    """Writes the chess game data to an ND-JSON file

    Args:
        data (Any): The chess game data to be written to file
        output_path (Path): The ouput path for the file
    """
    
    with output_path.open('w', encoding='utf-8') as output_file:
        writer = ndjson.writer(output_file, ensure_ascii=False)
        for item in data:
            writer.writerow(item)
    logging.info(f"Data successfully written to {output_path}")

def extract_data() -> None:
    """Fetches Chess game data from the Lichess API URL, parses it and writes to an ND-JSON file."""
    
    lichess_api_url = os.getenv('LICHESS_API_URL')
    lichess_username = os.getenv('LICHESS_USERNAME')

    if not lichess_api_url or not lichess_username:
        logging.error("Lichess API URL or Username is not set in environment variables.")
        return
    
    lichess_data = fetch_lichess_data(lichess_api_url, lichess_username)

    if lichess_data:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        output_filename = f"{lichess_username}_data_{timestamp}.ndjson"
        output_path = data_dir / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        write_ndjson_data(lichess_data, output_path)        
    else:
        logging.info(f"No new data for user {lichess_username} since the last extraction.")     
        

if __name__ == "__main__":
    try:
        extract_data()
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")