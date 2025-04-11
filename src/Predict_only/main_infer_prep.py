import os
import sys
import time
import logging
import pandas as pd
import datetime
import pprint
import argparse
import configparser
from pyspark.sql.types import TimestampType
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql.functions import (
    col, when, lit, row_number, expr,
    min as F_min, max as F_max, datediff,
    lag, count, trim, mean as F_mean
)
from psycopg2 import sql, pool, DatabaseError
from pyspark.sql import SparkSession
from src.Predict_only.load_prediction_data import load_prediction_data
from src.Predict_only.join_horse_embedding_and_predict import race_predictions
from src.data_preprocessing.data_prep2.data_healthcheck import time_series_data_healthcheck
from src.data_preprocessing.data_prep1.data_utils import initialize_environment, save_parquet

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
    
    # Parse command-line arguments
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run training or prediction tasks.")
    parser.add_argument("mode", choices=["load", "train", "predict"], help="Mode to run: load, train or predict")
    args = parser.parse_args()

    # Determine the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Script_dir: {script_dir}")

    spark = None
    try:
        spark, jdbc_url, jdbc_properties, parquet_dir, log_file = initialize_environment()
        spark.catalog.clearCache()
        setup_logging()
        logging.info("Starting main function...")
        logging.info(f"Mode: {args.mode}")
        # Used to determine if we are training or predicting in build_catboost_model function
        action = args.mode
        
        if args.mode == "load":
            logging.info("Running training data ingestion steps...")
            ###################################################
            # 1. Load Training data
            ###################################################
            # time_start = time.time()
            # logging.info("Running training data ingestion steps...")
            # prediction_df = load_prediction_data(spark, jdbc_url, jdbc_properties, parquet_dir)
            # healthcheck_report = time_series_data_healthcheck(prediction_df)
            # pprint.pprint(healthcheck_report)
            # logging.info("Ingestion job for prediction data succeeded")
            # total_time = time.time() - time_start
            # logging.info(f"Loading prediction_df took {total_time} to complete.")
            # input("Press Enter to continue and begin step 2 ...")
            
            # ###################################################
            # # 2. Merge with horse_embedding data and makre predictions
            # ###################################################
        if args.mode == "predict":
            horse_embedding = pd.read_parquet("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/horse_embedding_data-20250409_1552.parquet", engine="pyarrow")
            predictions_pdf = pd.read_parquet("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/prediction_df", engine="pyarrow")
            scored_sdf = race_predictions(spark, predictions_pdf, horse_embedding, jdbc_url, jdbc_properties, action) #spark, parquet_dir, speed_figure) # model_filename)
            
            # # total_time = time.time() - time_start
            # # logging.info(f"Training 20 Catboost models Horse embedding took {total_time} to complete.")
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}")
    finally:
        if spark:
            spark.stop()
            logging.info("Spark session stopped.")

if __name__ == "__main__":
    main()