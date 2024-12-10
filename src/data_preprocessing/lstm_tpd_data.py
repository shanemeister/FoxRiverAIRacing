# Environment setup

import logging
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import geopandas as gpd
from datetime import datetime
import configparser
from src.data_ingestion.ingestion_utils import (
    get_db_connection, update_tracking, load_processed_files
)
import traceback

# Load the configuration file
config = configparser.ConfigParser()
config.read('/home/exx/myCode/horse-racing/FoxRiverAIRacing/config.ini')

# Set up logging for consistent logging behavior in Notebook
logging.basicConfig(level=logging.INFO)

# Retrieve database credentials from config file
# Retrieve database credentials from config file
db_host = config['database']['host']
db_port = config['database']['port']
db_name = config['database']['dbname']  # Corrected from 'name' to 'dbname'
db_user = config['database']['user']

# Establish connection using get_db_connection
conn = get_db_connection(config)

# Create the SQLAlchemy engine
engine = create_engine(f'postgresql+psycopg2://{db_user}@{db_host}:{db_port}/{db_name}')


query_results = """
   SELECT course_cd, race_date, race_number, program_num, official_fin, post_pos
    FROM v_results_entries
    WHERE breed = 'TB';
"""

query_sectionals = """    
    
SELECT course_cd, race_date, race_number, saddle_cloth_number, gate_name, gate_numeric, 
    length_to_finish,sectional_time, running_time, distance_back, distance_ran, 
    number_of_strides, post_time 
FROM v_sectionals;
"""

query_gpspoint = """
select course_cd, race_date, race_number, saddle_cloth_number, time_stamp, 
longitude, latitude, speed, progress, stride_frequency, post_time, location
from v_gpspoint;
"""


query_routes = """
select course_cd, track_name, line_type, line_name, coordinates
from routes;
"""
# Execute the query and load data into a DataFrame
gps_df = pd.read_sql_query(query_gpspoint, engine, parse_dates=['time_stamp'])

gps_df.dtypes
print(gps_df.shape)

sectionals_df = pd.read_sql_query(query_sectionals, engine)

sectionals_df.dtypes
print(sectionals_df.shape)

results_df = pd.read_sql_query(query_results, engine)

routes_df = pd.read_sql_query(query_routes, engine)

gps_df['race_date'] = pd.to_datetime(gps_df['race_date'])
sectionals_df['race_date'] = pd.to_datetime(sectionals_df['race_date'])
results_df['race_date'] = pd.to_datetime(results_df['race_date'])

gps_df.to_parquet('/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/training/gps.parquet', index=False)
sectionals_df.to_parquet('/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/training/sectionals.parquet', index=False)
results_df.to_parquet('/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/training/results.parquet', index=False)
routes_df.to_parquet('/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/training/routes.parquet', index=False)

def main():
    """Main function to execute data ingestion tasks."""
    # Determine the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Script_dir: {script_dir}")
    
    try:
        # Read configuration
        config = read_config(script_dir)
        
        # Argument parser to allow selective processing of datasets via command line
        parser = argparse.ArgumentParser(description="Run EQB and TPD ingestion pipeline.")
        parser.add_argument('--ppData', action='store_true', help="Process only ppData.")
        parser.add_argument('--resultsCharts', action='store_true', help="Process only resultsCharts.")
        parser.add_argument('--tpdRacelist', action='store_true', help="Process only TPD Racelist.")
        parser.add_argument('--tpdSectionals', action='store_true', help="Process only TPD Sectionals.")
        parser.add_argument('--tpdGPS', action='store_true', help="Process only TPD GPS.")
        parser.add_argument('--tpdRoutes', action='store_true', help="Process only TPD Routes.")
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
        if args.tpdRoutes:
            datasets_to_process.add('Routes')

        # Run the ingestion pipeline with the selected datasets
        run_ingestion_pipeline(datasets_to_process, config, script_dir)
        
        logging.info("Ingestion job succeeded")
    except Exception as e:
        logging.error("Ingestion job failed", exc_info=True)
        raise

if __name__ == "__main__":
    main()