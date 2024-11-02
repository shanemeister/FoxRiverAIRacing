import logging
import os
import argparse
from datetime import datetime
import configparser
from src.data_ingestion.ingestion_utils import (
    get_db_connection, log_ingestion_status, load_processed_files
)
from src.data_ingestion.eqb_ppData import process_pluspro_data
from src.data_ingestion.eqb_resultsCharts import process_resultscharts_data
from src.data_ingestion.tpd_datasets import (
    process_tpd_racelist_data,
    process_tpd_sectionals_data,
    process_tpd_gpsdata_data
)

# Read configuration settings from 'config.ini' file
config = configparser.ConfigParser()
config.read('config.ini')

# Verify that the configuration file is read correctly
if 'database' not in config:
    raise KeyError("The 'database' section is missing in the configuration file.")

# Setup logging configuration to write logs to a file
logging.basicConfig(filename='/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/ingestion.log', level=logging.INFO)

def log_start(dataset_name, conn):
    """Logs the start of the ingestion process for a specified dataset."""
    logging.info(f"{datetime.now()} - Starting ingestion for: {dataset_name}")
    log_ingestion_status(conn, dataset_name, "started")

def log_end(dataset_name, conn, success=True):
    """Logs the completion of the ingestion process for a specified dataset."""
    status = "succeeded" if success else "failed"
    logging.info(f"{datetime.now()} - Ingestion for {dataset_name} {status}")
    log_ingestion_status(conn, dataset_name, status)

def run_ingestion_pipeline(datasets_to_process):
    """Main ingestion pipeline with updated logic for TPD data."""
    # Connect to the database
    conn = get_db_connection(config)
    try:
        processed_files = load_processed_files(conn)
        print(f"Processed files RVS: {len(processed_files)}")

        # Define datasets to process with corresponding functions
        datasets = {
            'PlusPro Data': lambda: process_pluspro_data(
                conn,
                config['paths']['pluspro_dir'],
                config['paths']['xsd_schema_ppd'],
                "/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/data_ingestion_errors.log",
                processed_files
            ),
            'ResultsCharts Data': lambda: process_resultscharts_data(
                conn,
                config['paths']['resultscharts_dir'],
                config['paths']['xsd_schema_rc'],
                "/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/result_charts_errors.log",
                processed_files
            ),
            'TPD Racelist Data': lambda: process_tpd_racelist_data(
                conn,
                config['paths']['tpd_racelist_dir'],
                "/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/tpd_racelist_errors.log",
                processed_files
            ),
            'TPD Sectionals Data': lambda: process_tpd_sectionals_data(
                conn,
                config['paths']['tpd_sectionals_dir'],
                "/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/tpd_sectionals_errors.log",
                processed_files
            ),
            'TPD GPS Data': lambda: process_tpd_gpsdata_data(
                conn,
                config['paths']['tpd_gpsdata_dir'],
                "/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/tpd_gpsdata_errors.log",
                processed_files
            )
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
        conn.close()

if __name__ == "__main__":
    # Argument parser to allow selective processing of datasets via command line
    parser = argparse.ArgumentParser(description="Run EQB and TPD ingestion pipeline.")
    parser.add_argument('--ppData', action='store_true', help="Process only ppData.")
    parser.add_argument('--resultsCharts', action='store_true', help="Process only resultsCharts.")
    parser.add_argument('--tpdRacelist', action='store_true', help="Process only TPD Racelist.")
    parser.add_argument('--tpdSectionals', action='store_true', help="Process only TPD Sectionals.")
    parser.add_argument('--tpdGPS', action='store_true', help="Process only TPD GPS.")

    args = parser.parse_args()

    # Create a list of datasets to process based on user-specified arguments
    datasets_to_process = []
    if args.ppData:
        datasets_to_process.append('PlusPro Data')
    if args.resultsCharts:
        datasets_to_process.append('ResultsCharts Data')
    if args.tpdRacelist:
        datasets_to_process.append('TPD Racelist Data')
    if args.tpdSectionals:
        datasets_to_process.append('TPD Sectionals Data')
    if args.tpdGPS:
        datasets_to_process.append('TPD GPS Data')

    # Run the ingestion pipeline with the selected datasets
    run_ingestion_pipeline(datasets_to_process)