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
from src.train_and_predict.load_training_data import load_base_training_data
from src.train_and_predict.load_prediction_data import load_base_inference_data
from src.train_and_predict.fox_speed_figure import create_custom_speed_figure
from src.data_preprocessing.data_prep2.data_healthcheck import time_series_data_healthcheck
from src.data_preprocessing.data_prep1.data_utils import initialize_environment, save_parquet
from src.train_and_predict.horse_embedding_subnet import embed_and_train
from src.train_and_predict.build_cat_model import build_catboost_model
from src.train_and_predict.final_predictions import main_inference

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
        if action == "load":
            ###################################################
            # 1. Load Training data
            ###################################################
            time_start = time.time()
            logging.info("Running training data ingestion steps...")
            train_df = load_base_training_data(spark, jdbc_url, jdbc_properties, parquet_dir)
            healthcheck_report = time_series_data_healthcheck(train_df)
            pprint.pprint(healthcheck_report)
            logging.info("Ingestion job for training data succeeded")
            total_time = time.time() - time_start
            logging.info(f"Loading train_df took {total_time} to complete.")
            input("Press Enter to continue and begin step 2 ...")
            
            # ###################################################
            # # 2. Compute the custom speed figure
            # ###################################################
            # time_start = time.time()
            # train_df = spark.read.parquet("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/train_df")
            # # # After loading your data and creating the custom speed figure, e.g.:
            # enhanced_df = create_custom_speed_figure(train_df, jdbc_url, jdbc_properties, parquet_dir)
            # enhanced_df.printSchema()
            # # (Continue with your healthcheck and saving steps)
            # healthcheck_report = time_series_data_healthcheck(enhanced_df)
            # pprint.pprint(healthcheck_report)
            # logging.info("Global_speed_score job completed successfully.")

            # # Save the Spark DataFrame as a Parquet file
            # save_parquet(spark, enhanced_df, "global_speed_score", parquet_dir)
            # logging.info("Ingestion job succeeded and Speed Figure Catboost model complete")
            # logging.info("global_speed_score saved as parquet file.")
            
            # total_time = time.time() - time_start
            # logging.info(f"Creating global_speed_score took {total_time} to complete.")
            # input("Press Enter to continue and begin step 3 ...")          
            # ##################################################
            # # 3) Embed horse_id and compute custom_speed_figure
            # ##################################################
            # time_start = time.time()
            # global_speed_score = spark.read.parquet("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/global_speed_score.parquet")
            
            # # global_speed_score.show(5)
            # global_speed_score.printSchema()
            # print(global_speed_score.count())
            
            # def convert_timestamp_columns(spark_df, timestamp_format="yyyy-MM-dd HH:mm:ss"):
            #     """
            #     Finds all TimestampType columns in a Spark DataFrame, converts them to strings using the specified format,
            #     and returns the modified DataFrame and a list of the names of the columns that were converted.
            #     """
            #     # Get list of timestamp columns from the schema.
            #     timestamp_cols = [field.name for field in spark_df.schema.fields if isinstance(field.dataType, TimestampType)]
            #     print("Timestamp columns found in Spark DataFrame:", timestamp_cols)
                
            #     # For each timestamp column, convert to string using date_format.
            #     for col in timestamp_cols:
            #         spark_df = spark_df.withColumn(col, F.date_format(F.col(col), timestamp_format))
            #     return spark_df, timestamp_cols

            # # Example usage:
            # # Assume 'global_speed_score' is your Spark DataFrame.
            # global_speed_score, ts_cols = convert_timestamp_columns(global_speed_score)

            # global_speed_score = global_speed_score.drop("race_date_str") 

            # model_filename = embed_and_train(spark, jdbc_url, parquet_dir, jdbc_properties, global_speed_score)
            # # Step 4: Use model_filename to load the saved Parquet file
            # parquet_path = os.path.join(parquet_dir, f"{model_filename}.parquet")
            # logging.info(f"Loading Parquet file from: {parquet_path}")

            # # Reload the Parquet file into a Spark DataFrame
            
            # horse_embedding = spark.read.parquet(parquet_path)
            
            # # Step 5: Print the schema of the reloaded DataFrame
            # logging.info(f"Schema of reloaded DataFrame from: {model_filename}:")
            # horse_embedding.printSchema()
            
            # total_time = time.time() - time_start
            # logging.info(f"Horse embedding took {total_time} to complete.")
            # logging.info(f"Starting Training")
            # input("Press Enter to continue and begin step 4 ...")
            pass
        elif action == "predict" or action == "train":
            ###################################################
            # 4) Prep data for Training or Predictions
            ###################################################
            # # Load the Parquet file into a Pandas DataFrame.
#            horse_embedding = pd.read_parquet("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/horse_embedding_data-20250304_1409.parquet", engine="pyarrow")
            #horse_embedding = spark.read.parquet("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/horse_embedding_data-20250304_1409.parquet")
            # Pandas will typically map timestamp/date columns to datetime64[ns].
            #healthcheck_report = time_series_data_healthcheck(horse_embedding)
            #pprint.pprint(healthcheck_report)            
#            time_start = time.time()
#            logging.info("Starting build_catboost_model: Training CatBoost Models -- 20 total")
            # # All models are saved to: ./data/models/all_models.json
#            build_catboost_model(spark, horse_embedding, jdbc_url, jdbc_properties, action) #spark, parquet_dir, speed_figure) # model_filename)
            
#            total_time = time.time() - time_start
#            logging.info(f"Training 20 Catboost models Horse embedding took {total_time} to complete.")
            pass
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}")
    finally:
        if spark:
            spark.stop()
            logging.info("Spark session stopped.")

if __name__ == "__main__":
    main()