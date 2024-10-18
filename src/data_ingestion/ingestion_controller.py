import logging
import os
from datetime import datetime
from race_list import race_list # type: ignore
from racedata import process_racedata_file # type: ignore
from ingestion_utils import get_db_connection # type: ignore
from eqb_ppData import ppData # type: ignore

# Setup logging
logging.basicConfig(filename='/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/ingestion.log', level=logging.INFO)

def log_start(dataset_name):
    logging.info(f"{datetime.now()} - Starting ingestion for: {dataset_name}")

def log_end(dataset_name, success=True):
    status = "succeeded" if success else "failed"
    logging.info(f"{datetime.now()} - Ingestion for {dataset_name} {status}")

def run_ingestion_pipeline():
    conn = get_db_connection()  # Establish connection once and pass it to functions
    error_file = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/data_ingestion_errors.log"  # Log file for rejected data
    
    datasets = [
        #('Race List', lambda: race_list(conn, '/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/TPD/racelist', error_file)),
        ('Race Data', lambda: ppData(conn, '/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/Equibase/PlusPro', 
                                      '/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/Equibase/ResultsCharts', error_file)),
        # Add more datasets and their corresponding functions here (age_restriction, course, etc.)
        ]
    
    for dataset_name, ingest_func in datasets:
        log_start(dataset_name)
        try:
            ingest_func()
            log_end(dataset_name, success=True)
        except Exception as e:
            logging.error(f"Error ingesting {dataset_name}: {e}")
            log_end(dataset_name, success=False)
    
    conn.close()

if __name__ == "__main__":
    run_ingestion_pipeline()