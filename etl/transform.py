from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pathlib import Path
import os
import yaml
import logging

# Define config path and data directory
config_path = Path("config")
data_dir = Path("data")

# Implement logger
logging.basicConfig(
    filename='extract.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s'
)

def load_config():
    path = config_path / "etl_config.yml"
    with open(path, 'r') as config_file:
        return yaml.safe_load(config_file)

def load_processed_files():
    try:
        with open('config/processed_files.txt', 'r') as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()

def save_processed_file(filename):
    path = config_path / "processed_files.txt"
    with open(path, 'a') as file:
        file.write(f"{filename}\n")

def write_to_pgn(games, file, output_path):
    
    filename, ext = os.path.splitext(file)
        
    with open(f"{output_path}/{filename}.pgn", 'a') as pgn:
        count = 1
        for game in games:
            if count == 1:
                pgn.write(f'[Game {count}]\n')
            else:
                pgn.write(f'\n[Game {count}]\n')
            pgn.write(f'[Game ID "{game["game_id"]}"]\n')
            pgn.write(f'[White "{game["white_name"]}"]\n')
            pgn.write(f'[Black "{game["black_name"]}"]\n')
            pgn.write(f'[Opening Eco "{game["opening_eco"]}"]\n')
            pgn.write(f'[Opening Name "{game["opening_name"]}"]\n')
            pgn.write(f'[Game Winner "{game["winner"]}"]\n')
            pgn.write(f'\n{game["moves"]}\n')
            count += 1


def parse_game(game_data):
    game_id = game_data["game_id"]
    white_name = game_data["white_name"]
    black_name = game_data["black_name"]
    opening_eco = game_data["opening_eco"]
    opening_name = game_data["opening_name"]
    winner = game_data["winner"]
    moves_str = game_data["moves"]
    
    return {
        "game_id": game_id,
        "white_name": white_name,
        "black_name": black_name,
        "opening_eco": opening_eco,
        "opening_name": opening_name,
        "winner": winner,
        "moves": moves_str
    }

def main():
    etl_config = load_config()
    processed_files = load_processed_files()

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ChessPuzzleGeneratorETL") \
        .master(etl_config['pyspark']['master']) \
        .config("spark.executor.memory", etl_config['pyspark']['executor_memory']) \
        .config("spark.executor.cores", etl_config['pyspark']['num_executors']) \
        .getOrCreate()
    
    # Read raw data
    raw_data_path = etl_config['data']['raw_data_path']
    transformed_data_path = etl_config['data']['transformed_data_path']
    new_files = [file for file in os.listdir(raw_data_path) if file.endswith(".ndjson") and file not in processed_files]
    
    for file in new_files:
        df = spark.read.json(f"{raw_data_path}/{file}")
        
        row_count = df.count()
        logging.info(f"File: {file} - Number of rows read: {row_count}")
        
        # Filter for games that ended in a mate (potential puzzles)
        filtered_df = df.filter((df.status == "mate") & (df.variant == "standard"))
        
        # Process data within Spark
        processed_games = filtered_df.select(
            col("id").alias("game_id"),
            col("players.white.user.name").alias("white_name"),
            col("players.black.user.name").alias("black_name"),
            col("opening.eco").alias("opening_eco"),
            col("opening.name").alias("opening_name"),
            col("winner"),
            col("moves"),
        ).rdd.map(lambda row: parse_game(row.asDict()))
        
        processed_games_count = processed_games.count()
        logging.info(f"File: {file} - Number of processed games: {processed_games_count}")        
        
        # Write each game to PGN file within 'foreachPartition'
        processed_games.foreachPartition(lambda partition: write_to_pgn(partition, file, transformed_data_path))
            
        save_processed_file(file)

    spark.stop()

    
if __name__ == '__main__':
    main()