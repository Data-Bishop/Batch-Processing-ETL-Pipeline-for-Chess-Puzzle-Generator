import yaml
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, udf, explode
from pyspark.sql.types import StringType, ArrayType, IntegerType, FloatType

def load_config():
    with open('config/etl_config.yml', 'r') as config_file:
        return yaml.safe_load(config_file)

def load_processed_files():
    try:
        with open('config/processed_files.txt', 'r') as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()

def save_processed_file(filename):
    with open('config/processed_files.txt', 'a') as file:
        file.write(f"{filename}\n")

def transform_data(raw_data):
    # Define udf to convert moves string to array
    split_moves_udf = udf(lambda moves: moves.split(), ArrayType(StringType()))
    
    # Define udf to calculate time efficiency
    time_efficiency_udf = udf(lambda initial_time, increment, total_time: (total_time - initial_time) / total_time)
    
    # Transformation logic
    transform_data = raw_data.select(
        col("id").alias("game_id"),
        col("players.white.user.name").alias("white_name"),
        col("players.white.user.id").alias("white_id"),
        col("players.white.user.title").alias("white_title"),
        col("players.white.rating").alias("white_rating"),
        col("players.white.ratingDiff").alias("white_rating_diff"),
        col("players.black.user.name").alias("black_name"),
        col("players.black.user.id").alias("black_id"),
        col("players.black.rating").alias("black_rating"),
        col("players.black.ratingDiff").alias("black_rating_diff"),
        col("status").alias("outcome"),
        col("clock.initial").alias("initial_time"),
        col("clock.increment").alias("increment"),
        col("clock.total_time").alias("total_time"),
        col("opening.eco").alias("opening_eco"),
        col("opening.name").alias("opening_name"),
        col("opening.ply").alias("opening_ply"),
        split_moves_udf(col("moves")).alias("moves"),
        expr("size(moves)").alias("move_count"),
        time_efficiency_udf(col("clock.initial"), col("clock.increment"), col("clock.totalTime")).alias("time_efficiency")
    )
    return transform_data

def main():
    etl_config = load_config()

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ChessPuzzleGeneratorETL") \
        .master(etl_config['pyspark']['master']) \
        .config("spark.executor.memory", etl_config['pyspark']['executor_memory']) \
        .config("spark.executor.cores", etl_config['pyspark']['num_executors']) \
        .getOrCreate()

    try:
        # Load processed files
        processed_files = load_processed_files()

        # Get list of new files
        raw_data_path = etl_config['data']['raw_data_path']
        transformed_data_path = etl_config['data']['transformed_data_path']
        new_files = [file for file in os.listdir(raw_data_path) if file.endswith(".json") and file not in processed_files]

        for filename in new_files:
            # Read raw data
            raw_data = spark.read.json(f"{raw_data_path}/{filename}")

            # Apply transformation to each game using explode
            transformed_data = raw_data.select(
                col("id"),
                col("rated"),
                col("variant"),
                col("speed"),
                col("perf"),
                col("createdAt"),
                col("lastMoveAt"),
                col("status"),
                col("players"),
                col("opening"),
                col("moves"),
                col("clock"),
                explode(col("games")).alias("game_data")
            ).select(
                col("id").alias("game_id"),
                col("game_data.*")
            ).rdd.map(transform_data).toDF()

            # Save transformed data
            transformed_data.write.mode("append").json(transformed_data_path)

            # Update list of processed files
            save_processed_file(filename)

    finally:
        # Stop the Spark session
        spark.stop()

if __name__ == "__main__":
    main()