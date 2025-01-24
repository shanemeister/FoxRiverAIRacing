import logging
import os
import sys
import pprint
import os
import sys
import configparser
from datetime import datetime
from psycopg2 import pool, DatabaseError
import configparser
from src.data_preprocessing.data_prep2.data_healthcheck import time_series_data_healthcheck, dataframe_summary
from src.data_preprocessing.data_prep1.data_utils import save_parquet
from src.data_preprocessing.data_prep1.data_utils import initialize_environment
from src.inference.load_prediction_data import load_base_inference_data
from src.training.load_training_data import load_base_training_data
from src.inference.make_predictions_cat import make_cat_predictions
from catboost import CatBoostRanker
from src.training.fox_speed_figure import create_custom_speed_figure

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
    # Determine the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Script_dir: {script_dir}")

    spark = None
    try:
        spark, jdbc_url, jdbc_properties, parquet_dir, log_file = initialize_environment()
        spark.catalog.clearCache()
        setup_logging()

        # If user requested train_data or no args => run training data ingestion
        print("Running training data ingestion steps...")
        train_df = load_base_training_data(spark, jdbc_url, jdbc_properties, parquet_dir)
        healthcheck_report = time_series_data_healthcheck(train_df)
        pprint.pprint(healthcheck_report)
        logging.info("Ingestion job for training data succeeded")
    except Exception as e:
        print(f"An error occurred during initialization: {e}")
        logging.error(f"An error occurred during initialization: {e}")
    finally:
        if spark:
            spark.stop()
            logging.info("Spark session stopped.")    
    try:   
        # 1) Initialize SparkSession and load data
        spark, jdbc_url, jdbc_properties, parquet_dir, _ = initialize_environment()

        # Example: we read the training DataFrame from a Parquet
        train_df = spark.read.parquet(
            "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/train_df"
        )
        train_df.printSchema()
        
        # 2) Compute the custom speed figure
        enhanced_df = create_custom_speed_figure(train_df)

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

        # 3) Save the Spark DataFrame as a Parquet file
        save_parquet(spark, spark_df, "speed_figure", "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet")
        logging.info("Ingestion job succeeded")

    except Exception as e:
        print(f"An error occurred during speed_figure initialization: {e}")
        logging.error(f"An error occurred during initialization: {e}")
    finally:
        
        # 4) Cleanup

        logging.info("Spark session stopped.")
 
if __name__ == "__main__":
    main()
