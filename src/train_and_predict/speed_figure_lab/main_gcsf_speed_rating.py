import os
import sys
import time
import logging
import pprint
import configparser
from psycopg2 import sql, pool, DatabaseError
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.train_and_predict.speed_figure_lab.gcsf_speed_rating import gps_composite_speed_figure, train_gcsf_model_with_tuning
from src.data_preprocessing.data_prep2.data_healthcheck import time_series_data_healthcheck
from src.data_preprocessing.data_prep1.data_utils import initialize_environment, save_parquet


def setup_logging():
    """Sets up logging configuration to write logs to a file and the console."""
    try:
        # Default log directory
        log_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs'
        
        # Ensure the log directory exists
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'speed_figure_prep.log')

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

def read_config(script_dir, config_relative_path='../../../config.ini'):
    """
    Reads the configuration file and returns the configuration object.
    """
    try:
        config = configparser.ConfigParser()
        config_file_path = os.path.abspath(os.path.join(script_dir, config_relative_path))
        if not os.path.exists(config_file_path):
            raise FileNotFoundError(f"Configuration file '{config_file_path}' does not exist.")
        config.read(config_file_path)
        if 'database' not in config:
            raise KeyError("The 'database' section is missing in the configuration file.")
        return config
    except Exception as e:
        logging.error(f"Error reading configuration file: {e}")
        sys.exit(1)


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
    print(f"script_dir: {script_dir}")
    config = read_config(script_dir)

    # 1) Create DB pool
    db_pool = get_db_pool(config)
    conn = db_pool.getconn()
    
    # Determine the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Script_dir: {script_dir}")

    spark = None
    try:
        spark, jdbc_url, jdbc_properties, parquet_dir, log_file = initialize_environment()
        spark.catalog.clearCache()
        setup_logging()
        logging.info("Starting main function...")
        # Used to determine if we are training or predicting in build_catboost_model function
        ###################################################
        # 1. Load TPD GPS Speed Data
        ###################################################
        conn = db_pool.getconn()
        X, y, final_df = gps_composite_speed_figure(conn)
        model, final_predictions = train_gcsf_model_with_tuning(X, y, use_scaler="minmax")

        spark_pred_df = spark.createDataFrame(final_predictions)
        
        spark_pred_df.orderBy(
            "course_cd","race_date","race_number",
            F.asc("beaten_lengths")
            ).select(
                "course_cd","race_date","race_number","horse_id",
                "beaten_lengths","gcsf_speed_figure"
            ).show(50, truncate=False)
            
        spark_pred_df.orderBy(
                "course_cd","race_date","race_number",
                F.asc("finishing_position")
            ).select(
                "course_cd","race_date","race_number","horse_id",
                "finishing_position","gcsf_speed_figure"
            ).show(50, truncate=False)
            
        spark_pred_df.select("prediction").describe().show()
        
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}")

    finally:
        if spark:
            spark.stop()
            logging.info("Spark session stopped.")

if __name__ == "__main__":
    main()