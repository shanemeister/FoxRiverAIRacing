import logging
import os
import argparse
from datetime import datetime
import configparser
from ingestion_utils import get_db_connection, log_ingestion_status, load_processed_files
from eqb_ppData import process_pluspro_data
from eqb_resultsCharts import process_resultscharts_data
# from race_list import process_racelist  # (This line is commented out for future inclusion if needed)
from tpd_racelist import process_tpd_racelist_data  # Import the function to process racelist data
from tpd_sectionals import process_tpd_sectional_data
from tpd_datasets import (
    process_tpd_racelist_data,
    process_tpd_sectionals_data,
    process_tpd_gpsdata_data
)


# Read configuration settings from 'config.ini' file
config = configparser.ConfigParser()
config.read('config.ini')

# Setup logging configuration to write logs to a file
logging.basicConfig(filename='/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/ingestion.log', level=logging.INFO)

def log_start(dataset_name, conn):
    """
    Logs the start of the ingestion process for a specified dataset.
    
    Parameters:
    - dataset_name (str): The name of the dataset being ingested.
    - conn: Database connection to log status in the database.
    """
    logging.info(f"{datetime.now()} - Starting ingestion for: {dataset_name}")
    log_ingestion_status(conn, dataset_name, "started")  # Logs start status to the database.

def log_end(dataset_name, conn, success=True):
    """
    Logs the completion of the ingestion process for a specified dataset.
    
    Parameters:
    - dataset_name (str): The name of the dataset.
    - conn: Database connection to log status in the database.
    - success (bool): Indicates whether the ingestion was successful. Default is True.
    """
    status = "succeeded" if success else "failed"
    logging.info(f"{datetime.now()} - Ingestion for {dataset_name} {status}")
    log_ingestion_status(conn, dataset_name, status)  # Logs end status to the database.

def run_ingestion_pipeline(datasets_to_process):
    """
    Main ingestion pipeline with updated logic for TPD data.
    """
    # Connect to the database
    conn = get_db_connection(config)
    try:
        processed_files = load_processed_files(conn)
        print(f"Processed files: length{processed_files}")
        # Define datasets to process with corresponding functions
        datasets = {
            'PlusPro Data': lambda: process_pluspro_data(
                conn,
                config['paths']['pluspro_dir'],  # Directory for PlusPro data files
                config['paths']['xsd_schema_ppd'],  # XSD schema path for PlusPro data
                "/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/data_ingestion_errors.log",  # Error log path
                processed_files  # List of processed files to avoid re-processing
            ),
            'ResultsCharts Data': lambda: process_resultscharts_data(
                conn,
                config['paths']['resultscharts_dir'],  # Directory for ResultsCharts data files
                config['paths']['xsd_schema_rc'],  # XSD schema path for ResultsCharts data
                "/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/result_charts_errors.log",  # Error log path
                processed_files  # List of processed files to avoid re-processing
            ),
            'TPD Racelist Data': lambda: process_tpd_racelist_data(
                conn, 
                config['paths']['tpd_racelist_dir'], 
                "/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/tpd_racelist_errors.log", 
                ),
            'TPD Sectionals Data': lambda: process_tpd_sectionals_data(
                conn, 
                config['paths']['tpd_sectionals_dir'], 
                "/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/tpd_sectionals_errors.log",  
                processed_files),
        
            'TPD GPS Data': lambda: process_tpd_gpsdata_data(
                conn, 
                config['paths']['tpd_gpsdata_dir'], 
                "/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/tpd_gpsdata_errors.log",  
                processed_files)
        }
        
        # Process each dataset as specified
        for dataset_name, ingest_func in datasets.items():
            if not datasets_to_process or dataset_name in datasets_to_process:
                log_start(dataset_name, conn)
                try:
                    ingest_func()
                    log_end(dataset_name, conn, success=True)
                except Exception as e:
                    logging.error(f"Error processing {dataset_name}: {e}")
                    log_end(dataset_name, conn, success=False)
    finally:
        conn.close()  # Close the database connection after processing

if __name__ == "__main__":
    # Argument parser to allow selective processing of datasets via command line
    parser = argparse.ArgumentParser(description="Run EQB ingestion pipeline.")
    parser.add_argument('--ppData', action='store_true', help="Process only ppData.")
    parser.add_argument('--resultsCharts', action='store_true', help="Process only resultsCharts.")
    args = parser.parse_args()

    # Create a list of datasets to process based on user-specified arguments
    datasets_to_process = []
    if args.ppData:
        datasets_to_process.append('PlusPro Data')
    if args.resultsCharts:
        datasets_to_process.append('ResultsCharts Data')

    # Run the ingestion pipeline with the selected datasets
    run_ingestion_pipeline(datasets_to_process)