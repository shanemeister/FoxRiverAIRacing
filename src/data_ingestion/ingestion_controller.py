import logging
import os
import argparse
from datetime import datetime
import configparser
from src.data_ingestion.ingestion_utils import (
    get_db_connection, update_tracking, load_processed_files
)
from src.data_ingestion.eqb_ppData import process_pluspro_data
from src.data_ingestion.eqb_resultsCharts import process_resultscharts_data
from src.data_ingestion.tpd_datasets import (
    process_tpd_sectionals_data,
    process_tpd_gpsdata_data
)
from src.data_ingestion.race_list import process_tpd_racelist

def setup_logging(script_dir):
    """Sets up logging configuration to write logs to a file."""
    log_dir = os.path.join(script_dir, '../../logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'ingestion.log')
    
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info("Logging has been set up successfully.")

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

def log_start(dataset_name, conn):
    """Logs the start of the ingestion process for a specified dataset."""
    logging.info(f"Starting ingestion for: {dataset_name}")
    update_tracking(conn, dataset_name, "started")

def log_end(dataset_name, conn, success=True):
    """Logs the completion of the ingestion process for a specified dataset."""
    status = "succeeded" if success else "failed"
    logging.info(f"Ingestion for {dataset_name} {status}")
    update_tracking(conn, dataset_name, status)

def run_ingestion_pipeline(datasets_to_process, config, script_dir):
    """Main ingestion pipeline with updated logic for TPD data."""
    # Connect to the database
    conn = get_db_connection(config)
    try:
        # Define datasets to process with corresponding functions
        datasets = {
            'PlusPro': lambda processed_files: process_pluspro_data(
                conn,
                config['paths']['pluspro_dir'],
                config['paths']['xsd_schema_ppd'],
                os.path.join(script_dir, '../../logs/data_ingestion_errors.log'),
                processed_files
            ),
            'ResultsCharts': lambda processed_files: process_resultscharts_data(
                conn,
                config['paths']['resultscharts_dir'],
                config['paths']['xsd_schema_rc'],
                os.path.join(script_dir, '../../logs/result_charts_errors.log'),
                processed_files
            ),
            'Sectionals': lambda processed_files: process_tpd_sectionals_data(
                conn,
                config['paths']['tpd_sectionals_dir'],
                os.path.join(script_dir, '../../logs/tpd_sectionals_errors.log'),
                processed_files
            ),
            'GPSData': lambda processed_files: process_tpd_gpsdata_data(
                conn,
                config['paths']['tpd_gpsdata_dir'],
                os.path.join(script_dir, '../../logs/tpd_gpsdata_errors.log'),
                processed_files
            ),
            'Racelist': lambda processed_files: process_tpd_racelist(
                conn,
                config['paths']['tpd_racelist_dir'],
                os.path.join(script_dir, '../../logs/tpd_racelist_errors.log'),
                processed_files
            )
        }

        # Process each dataset as specified
        for dataset_name, ingest_func in datasets.items():
            if not datasets_to_process or dataset_name in datasets_to_process:
                # Load processed files specific to the dataset
                processed_files = load_processed_files(conn, dataset_type=dataset_name)
                print(f"Processed files for {dataset_name}: {len(processed_files)}")

                log_start(dataset_name, conn)
                try:
                    ingest_func(processed_files)
                    log_end(dataset_name, conn, success=True)
                except Exception as e:
                    logging.error(f"Error processing {dataset_name}: {e}", exc_info=True)
                    log_end(dataset_name, conn, success=False)
    finally:
        conn.close()
        logging.info("Database connection closed.")

def main():
    """Main function to execute data ingestion tasks."""
    # Determine the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Script_dir: {script_dir}")
    # Setup logging
    setup_logging(script_dir)
    
    logging.info("Starting ingestion job")
    
    try:
        # Read configuration
        config = read_config(script_dir)
        
        # Argument parser to allow selective processing of datasets via command line
        parser = argparse.ArgumentParser(description="Run EQB and TPD ingestion pipeline.")
        parser.add_argument('--ppData', action='store_true', help="Process only ppData.")
        parser.add_argument('--resultsCharts', action='store_true', help="Process only resultsCharts.")
        parser.add_argument('--tpdSectionals', action='store_true', help="Process only TPD Sectionals.")
        parser.add_argument('--tpdGPS', action='store_true', help="Process only TPD GPS.")
        parser.add_argument('--tpdRacelist', action='store_true', help="Process only TPD Racelist.")

        args = parser.parse_args()

        # Create a set of datasets to process based on user-specified arguments
        datasets_to_process = set()
        if args.ppData:
            datasets_to_process.add('PlusPro')
        if args.resultsCharts:
            datasets_to_process.add('ResultsCharts')
        if args.tpdSectionals:
            datasets_to_process.add('Sectionals')
        if args.tpdGPS:
            datasets_to_process.add('GPSData')
        if args.tpdRacelist:
            datasets_to_process.add('Racelist')

        # Run the ingestion pipeline with the selected datasets
        run_ingestion_pipeline(datasets_to_process, config, script_dir)
        
        logging.info("Ingestion job succeeded")
    except Exception as e:
        logging.error("Ingestion job failed", exc_info=True)
        raise

if __name__ == "__main__":
    main()