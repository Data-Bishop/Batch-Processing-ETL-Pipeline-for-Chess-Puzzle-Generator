import yaml
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, expr, udf
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, IntegerType, ArrayType

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
    # Define UDFs
    split_moves_udf = udf(lambda moves: moves.split(), ArrayType(StringType()))
    time_efficiency_udf = udf(lambda initial, total: (total - initial) / total)

    
    # Transformation logic for each game
    transform_data = raw_data.select(
        col("id").alias("game_id"),
        col("rated"),
        col("variant"),
        col("speed"),
        col("perf"),
        col("createdAt"),
        col("lastMoveAt"),
        col("status"),
        col("players.white.user.name").alias("white_name"),
        col("players.white.user.id").alias("white_id"),
        col("players.white.user.title").alias("white_title"),
        col("players.white.rating").alias("white_rating"),
        col("players.white.ratingDiff").alias("white_rating_diff"),
        col("players.black.user.name").alias("black_name"),
        col("players.black.user.id").alias("black_id"),
        col("players.black.rating").alias("black_rating"),
        col("players.black.ratingDiff").alias("black_rating_diff"),
        col("winner"),
        col("opening.eco").alias("opening_eco"),
        col("opening.name").alias("opening_name"),
        col("opening.ply").alias("opening_ply"),
        split_moves_udf(col("moves")).alias("moves"),
        expr("size(moves)").alias("move_count"),
        time_efficiency_udf(col("clock.initial"), col("clock.totalTime")).alias("time_efficiency")
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
    
    # Read raw data
    raw_data_path = etl_config['data']['raw_data_path']
    transformed_data_path = etl_config['data']['transformed_data_path']
    raw_data = spark.read.json(f"{raw_data_path}/drnykterstein_data_20240114120856.json", multiLine=True)
    raw_data.show()
    
    split_moves_udf = udf(lambda moves: moves.split(), ArrayType(StringType()))
    time_efficiency_udf = udf(lambda initial, total: (total - initial) / total)
    
    # Apply transformation to each game
    transformed_data = raw_data.select(
        col("lichess.id").alias("game_id"),
        col("lichess.rated"),
        col("lichess.variant"),
        col("lichess.speed"),
        col("lichess.perf"),
        col("lichess.createdAt"),
        col("lichess.lastMoveAt"),
        col("lichess.status"),
        col("lichess.players.white.user.name").alias("white_name"),
        col("lichess.players.white.user.id").alias("white_id"),
        col("lichess.players.white.user.title").alias("white_title"),
        col("lichess.players.white.rating").alias("white_rating"),
        col("lichess.players.white.ratingDiff").alias("white_rating_diff"),
        col("lichess.players.black.user.name").alias("black_name"),
        col("lichess.players.black.user.id").alias("black_id"),
        col("lichess.players.black.rating").alias("black_rating"),
        col("lichess.players.black.ratingDiff").alias("black_rating_diff"),
        col("lichess.winner"),
        col("lichess.opening.eco").alias("opening_eco"),
        col("lichess.opening.name").alias("opening_name"),
        col("lichess.opening.ply").alias("opening_ply")
    )

    # If needed, you can perform additional transformations or filtering here

    # Show the transformed data
    transformed_data.show()
    
    """  
    # Load processed files
    processed_files = load_processed_files()

    # Get list of new files
    raw_data_path = etl_config['data']['raw_data_path']
    transformed_data_path = etl_config['data']['transformed_data_path']
    new_files = [file for file in os.listdir(raw_data_path) if file.endswith(".json") and file not in processed_files]

    # Define the schema for the JSON data 
    lichess_schema = StructType([
        StructField("lichess", ArrayType(
            StructType([
                StructField("id", StringType(), True),
                StructField("rated", BooleanType(), True),
                StructField("variant", StringType(), True),
                StructField("speed", StringType(), True),
                StructField("perf", StringType(), True),
                StructField("createdAt", LongType(), True),
                StructField("lastMoveAt", LongType(), True),
                StructField("status", StringType(), True),
                StructField("players", StructType({
                    StructField("white", StructType({
                        StructField("user", StructType({
                            StructField("name", StringType()), 
                            StructField("title", StringType()), 
                            StructField("flair", StringType()), 
                            StructField("patron", BooleanType()), 
                            StructField("id", StringType())
                        })), 
                        StructField("rating", IntegerType()), 
                        StructField("ratingDiff", IntegerType())
                    })),
                    StructField("black", StructType({
                        StructField("user", StructType({
                            StructField("name", StringType()), 
                            StructField("title", StringType()), 
                            StructField("flair", StringType()), 
                            StructField("patron", BooleanType()), 
                            StructField("id", StringType())
                        })), 
                        StructField("rating", IntegerType()), 
                        StructField("ratingDiff", IntegerType()),
                        StructField("provisional", BooleanType())
                    }))
                })), 
                StructField("winner", StringType(), True),
                StructField("opening", StructType({
                    StructField("eco", StringType()), 
                    StructField("name", StringType()), 
                    StructField("ply", IntegerType())
                }), True),
                StructField("moves", StringType(), True),
                StructField("clocks", ArrayType(LongType()), True),
                StructField("clock", StructType({
                    StructField("initial", IntegerType()),
                    StructField("increment", IntegerType()),
                    StructField("totalTime", IntegerType())
                }), True)
            ])
        ))
    ])

    for filename in new_files:
    """


    # Save transformed data
    filename = "drnykterstein_data_20240117012957.json"
    transformed_data.write.mode("overwrite").json("output1")

    # Update list of processed files
    #save_processed_file(filename)


if __name__ == "__main__":
    main()
