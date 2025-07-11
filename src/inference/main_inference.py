import os
import sys
import logging
import pprint
import argparse
import configparser
from psycopg2 import sql, pool, DatabaseError
from pyspark.sql import SparkSession
from src.training.load_training_data import load_base_training_data
from src.training.fox_speed_figure import create_custom_speed_figure
from src.data_preprocessing.data_prep2.data_healthcheck import time_series_data_healthcheck
from src.data_preprocessing.data_prep1.data_utils import initialize_environment, save_parquet
from src.inference.load_and_clean_race_data import load_races
from src.inference.make_predictions import prediction_maker

def setup_logging():
    """Sets up logging configuration to write logs to a file and the console."""
    try:
        # Default log directory
        log_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs'
        
        # Ensure the log directory exists
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'inference_prep.log')

        # Clear the log file by opening it in write mode
        with open(log_file, 'w'):
            pass  # This will truncate the file without writing anything
        
        # Create a logger and clear existing handlers
        logger = logging.getLogger()
        if logger.hasHandlers():
            logger.handlers.clear()

        logger.setLevel(logging.INFO)

        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        # Create console handler
        #console_handler = logging.StreamHandler()
        #console_handler.setLevel(logging.INFO)

        # Define a common format
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        #console_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(file_handler)
        #logger.addHandler(console_handler)

        logger.info("Logging has been set up successfully.")
    except Exception as e:
        print(f"Failed to set up logging: {e}", file=sys.stderr)
        sys.exit(1)

def read_config(script_dir):
    """Reads the configuration file and returns the configuration object."""
    config = configparser.ConfigParser()
    
    # Construct the absolute path to config.ini
    root_dir = os.path.abspath(os.path.join(script_dir, '../../'))
    config_file_path = os.path.join(root_dir, 'config.ini')
    
    logging.info(f"Reading configuration from {config_file_path}")
    
    if not os.path.exists(config_file_path):
        logging.error(f"Configuration file '{config_file_path}' does not exist.")
        raise FileNotFoundError(f"Configuration file '{config_file_path}' does not exist.")
    
    config.read(config_file_path)
    
    if 'database' not in config:
        logging.error("The 'database' section is missing in the configuration file.")
        logging.error(f"Available sections: {config.sections()}")
        raise KeyError("The 'database' section is missing in the configuration file.")
    
    return config

def get_db_pool(config):
    """Creates a connection pool to PostgreSQL."""
    try:
        db_pool_args = {
            'user': config['database']['user'],
            'host': config['database']['host'],
            'port': config['database']['port'],
            'database': config['database']['dbname']
        }
        
        password = config['database'].get('password')
        if password:
            db_pool_args['password'] = password
            logging.info("Password found in configuration. Using provided password.")
        else:
            logging.info("No password in config. Attempting .pgpass or other authentication.")

        db_pool = pool.SimpleConnectionPool(
            1, 20,  # min and max connections
            **db_pool_args
        )
        if db_pool:
            logging.info("Connection pool created successfully.")
        return db_pool
    except DatabaseError as e:
        logging.error(f"Database error creating connection pool: {e}")
        sys.exit(1)
    except KeyError as e:
        logging.error(f"Missing configuration key: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error creating connection pool: {e}")
        sys.exit(1)

def main():
    """Main function to execute data ingestion tasks."""
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = read_config(script_dir)

    # 1) Create DB pool
    db_pool = get_db_pool(config)
    conn = db_pool.getconn()

    # Determine the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Script_dir: {script_dir}")

    spark = None
    try:
        spark, jdbc_url, jdbc_properties, parquet_dir, _ = initialize_environment()
        spark.catalog.clearCache()
        setup_logging()

        # 1. Load Race Data and Make Predictions
        logging.info("Running race data inference...")
        races_df = load_races(spark, jdbc_url, jdbc_properties, parquet_dir)
        healthcheck_report = time_series_data_healthcheck(races_df)
        pprint.pprint(healthcheck_report)
        logging.info("Ingestion job for training data succeeded")

            # 2. Compute the custom speed figure
            enhanced_df = create_custom_speed_figure(races_df)

            # Convert back to Spark
            spark_df = spark.createDataFrame(enhanced_df)

            # Example filtering logic
            spark_df.filter(
                (spark_df["official_fin"] <= 4) & (spark_df["class_rating"] >= spark_df["previous_class"])
            ).select(
                "race_id",
                "horse_id", 
                "official_fin", 
                "class_rating", 
                "previous_class", 
                "speed_rating", 
                "horse_itm_percentage", 
                "perf_target", 
                "custom_speed_figure"
            ).show(30)
            
            # Save the Spark DataFrame as a Parquet file
            save_parquet(spark, spark_df, "speed_figure", "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet")
            # logging.info("Ingestion job succeeded and Speed Figure Catboost model complete")
            
            # 3) Embed horse_id and compute custom_speed_figure
            #  End product is a parquet file in panda format with horse embeddings and the custom_speed_figure
            model_filename = embed_and_train(spark, parquet_dir)
            
            # 4) Build CatBoost Model
            
            build_catboost_model(spark) #, parquet_dir, model_filename)
            
        elif args.mode == "load":
            # Prediction mode
            print("Loading and Preparing data for inference...")
            # speed_figure = spark.read.parquet("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/speed_figure.parquet")
            # horse_embedding_function(speed_figure)
            # speed_figure.printSchema()
            # logging.info("Prediction job succeeded")

    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}")

    finally:
        if spark:
            spark.stop()
            logging.info("Spark session stopped.")

if __name__ == "__main__":
    main()