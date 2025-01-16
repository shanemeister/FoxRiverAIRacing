import logging
import os
import sys
import pprint
import argparse
from datetime import datetime
import configparser
from src.data_preprocessing.data_prep2.data_healthcheck import time_series_data_healthcheck, dataframe_summary
from src.data_preprocessing.data_prep1.data_utils import save_parquet
from src.data_preprocessing.data_prep1.data_utils import initialize_environment
from src.inference.load_prediction_data import load_base_inference_data
from src.inference.load_training_data import load_base_training_data
from src.inference.make_predictions_cat import make_cat_predictions
from catboost import CatBoostRanker

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

    # Parse arguments
    args = sys.argv[1:]  # excluding script name
    num_args = len(args)

    # Booleans to control logic
    do_train_data = False
    do_pred_data = False
    predict_cat = False

    # 1) No arguments => run train_data only
    if num_args == 0:
        do_train_data = True

    # 2) Exactly 1 argument => must be "train_data" or "pred_data"
    elif num_args == 1:
        arg_lower = args[0].lower()
        if arg_lower == "train_data":
            do_train_data = True
        elif arg_lower == "pred_data":
            do_pred_data = True
        else:
            print(f"ERROR: Invalid single argument '{args[0]}'. "
                  f"Must be 'train_data' or 'pred_data'.")
            sys.exit(1)

    # 3) Exactly 2 arguments => must be "predict" + a number (1 or 2)
    elif num_args == 2:
        arg1_lower = args[0].lower()
        arg2_lower = args[1].lower()

        # Must be "predict" + ("1" or "2")
        if arg1_lower == "predict":
            # If second arg == "1" => XGB; if "2" => CatBoost
            if arg2_lower == "1":
                do_pred_data = True
                predict_xgb = True
            elif arg2_lower == "2":
                do_pred_data = True
                predict_cat = True
            else:
                print(f"ERROR: Invalid second argument '{args[1]}'. "
                      f"When first arg is 'predict', second must be '1' or '2' for XGB/LGB or CAT.")
                sys.exit(1)
        else:
            print(f"ERROR: Invalid arguments '{args[0]} {args[1]}'. "
                  f"Must be 'predict 1' or 'predict 2'.")
            sys.exit(1)

    # More than 2 arguments => error
    else:
        print("ERROR: Too many arguments. Usage examples:")
        print("  no args -> runs train_data")
        print("  train_data")
        print("  pred_data")
        print("  predict 1  (XGB)")
        print("  predict 2  (CATBoost)")
        sys.exit(1)

    spark = None
    try:
        # Read configuration
        config = read_config(script_dir)
        try:
            spark, jdbc_url, jdbc_properties, parquet_dir, log_file = initialize_environment()
            spark.catalog.clearCache()
            setup_logging()

            # If user requested train_data or no args => run training data ingestion
            if do_train_data:
                print("Running training data ingestion steps...")
                training_data = load_base_training_data(spark, jdbc_url, jdbc_properties, parquet_dir)
                healthcheck_report = time_series_data_healthcheck(training_data)
                pprint.pprint(healthcheck_report)
                logging.info("Ingestion job for training data succeeded")
                # Possibly call some function: save_parquet(...) if needed
                # input("Press Enter to continue...") # if you want a pause

            # If user requested pred_data => run inference data ingestion
            if do_pred_data:
                print("Running pred_data ingestion steps...")
                upcoming_races = load_base_inference_data(spark, jdbc_url, jdbc_properties, parquet_dir)
                healthcheck_report = time_series_data_healthcheck(upcoming_races)
                pprint.pprint(healthcheck_report)
                logging.info("Ingestion job for pred_data succeeded")
                save_parquet(spark, upcoming_races, "upcoming_races", parquet_dir)

            # If user requested "predict 2" => run CatBoost predictions
            if predict_cat:
                upcoming_races = upcoming_races.toPandas()
                    # Load the saved CatBoost model
                final_model = CatBoostRanker()
                final_model.load_model(
                    "/home/exx/myCode/horse-racing/FoxRiverAIRacing/src/models/catboost_984316_2025-01-12_final.cbm",
                    format="cbm"
                )

                make_cat_predictions(upcoming_races, final_model)

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