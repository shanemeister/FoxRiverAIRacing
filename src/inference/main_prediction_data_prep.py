import logging
import os
import sys
import pprint
import argparse
from datetime import datetime
import configparser
from src.data_preprocessing.data_prep2.data_healthcheck import time_series_data_healthcheck, dataframe_summary
from src.data_preprocessing.data_prep1.data_utils import (
    save_parquet
)
from src.data_preprocessing.data_prep1.data_utils import initialize_environment
from src.inference.load_prediction_data import load_base_inference_data
from src.inference.load_training_data import load_base_training_data
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

def main():
    """Main function to execute data ingestion tasks."""
    # Determine the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Script_dir: {script_dir}")

    # Figure out which modes to run
    # If no arguments => run both
    # If "prediction" => run prediction steps
    # If "train" => run training steps
    args = sys.argv[1:]
    run_prediction = False
    run_train = False

    if not args:
        # No arguments => run both
        run_prediction = True
        run_train = True
    else:
        # Parse each argument
        for arg in args:
            arg_lower = arg.lower()
            if arg_lower == "prediction":
                run_prediction = True
            elif arg_lower == "train":
                run_train = True
            else:
                print(f"Unknown argument: {arg}. Valid arguments are 'prediction' or 'train'.")
                sys.exit(1)

    spark = None
    try:
        # Read configuration
        config = read_config(script_dir)
        try:
            spark, jdbc_url, jdbc_properties, parquet_dir, log_file = initialize_environment()
            spark.catalog.clearCache()
            setup_logging()

            # 1) If asked to do "prediction" or if no args => run inference data load
            if run_prediction:
                predict = load_base_inference_data(spark, jdbc_url, jdbc_properties, parquet_dir)
                healthcheck_report = time_series_data_healthcheck(predict)
                pprint.pprint(healthcheck_report)
                logging.info("Ingestion job for predictions succeeded")
                save_parquet(spark, predict, "predict", parquet_dir)

            # 2) If asked to do "train" or if no args => run training data load
            if run_train:
                training_data = load_base_training_data(spark, jdbc_url, jdbc_properties, parquet_dir)
                healthcheck_report = time_series_data_healthcheck(training_data)
                pprint.pprint(healthcheck_report)
                logging.info("Ingestion job for training data succeeded")
                input("Press Enter to continue...")
                save_parquet(spark, training_data, "training_data", parquet_dir)

        except Exception as e:
            print(f"An error occurred during initialization: {e}")
            logging.error(f"An error occurred during initialization: {e}")
    except Exception as e:
        logging.error("Ingestion job failed in main()", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped.")

if __name__ == "__main__":
    main()