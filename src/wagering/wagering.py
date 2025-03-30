import logging
import os
import sys
import traceback
import time
from pyspark.sql.functions import to_timestamp
from pyspark.sql import Window
from datetime import datetime
import pandas as pd
import src.wagering.wager_types as wt
from src.wagering.wagering_functions import build_race_objects, build_wagers_dict
from src.wagering.wagering_queries import wager_queries
from src.data_preprocessing.data_prep1.data_utils import initialize_environment
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql
from src.wagering.wagers import implement_ExactaWager, analyze_pick3_wagers
from src.wagering.wagering_helper_functions import (setup_logging, read_config, get_db_pool, parse_winners_str, get_user_wager_preferences,
                                                    convert_timestamp_columns, gather_bet_metrics, save_results_to_parquet)
    
def implement_strategy(parquet_dir, races_pdf, wagers_pdf):
    # 1) Gather user inputs for the wager
    user_prefs = get_user_wager_preferences()
    wager_type = user_prefs["wager_type"]
    base_amount = user_prefs["base_amount"]
    is_box = True

    # 2) Build Race objects, Wagers dict, etc.
    all_races = build_race_objects(races_pdf)
    wagers_dict = build_wagers_dict(wagers_pdf)

    # 3) Instantiate the correct Wager subclass
    #    We'll do just an example for 'Exacta' or 'Daily Double'
    if wager_type == "Exacta":
        bet_results = implement_ExactaWager(all_races, wagers_dict, base_amount=base_amount, top_n=2, box=is_box)
        print("parquet_dir: ", parquet_dir)
        filename=f"{parquet_dir}exacta_bet_results.parquet"
        print("filename: ", filename)
        save_results_to_parquet(bet_results, filename )
    elif wager_type == "Pick 3":
        results = analyze_pick3_wagers( races_pdf, wagers_pdf, base_amount=base_amount, parquet_path=parquet_dir)
    else:
        print(f"'{wager_type}' not yet implemented. Defaulting to Exacta.")
              
def main():
    """
    Main function to:
      - Initialize environment
      - Create SparkSession
      - Create DB connection pool
      - Run Spark-based sectionals aggregation
      - Run net sentiment update
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = read_config(script_dir)
    dataset_log_file = os.path.join(script_dir, f"../../logs/{datetime.now().strftime('%Y-%m-%d')}_dataset.log")
    setup_logging(script_dir, log_dir=dataset_log_file)
    # 1) Create DB pool
    db_pool = get_db_pool(config)
    
    # 2) Initialize Spark, logging, and load data
    try:
        spark, jdbc_url, jdbc_properties, parquet_dir, _ = initialize_environment()
        
        setup_logging(script_dir, log_dir=dataset_log_file)

        # Load and write data to parquet
        queries = wager_queries()
        dfs = load_data_from_postgresql(spark, jdbc_url, jdbc_properties, queries, parquet_dir)
        # Suppose we have a dictionary of queries
        for name, df in dfs.items():
            logging.info(f"DataFrame '{name}' loaded. Schema:")
            df.printSchema()
            if name == "races":
                races_df = df
            elif name == "wagers":
                wagers_df = df
            else:
                logging.error(f"Unknown DataFrame name: {name}")
                continue
        # Fixing the datetime column in a PySpark DataFrame
        races_df, ts_cols = convert_timestamp_columns(races_df)
        races_df.printSchema()
        races_pdf = races_df.toPandas()        
        # races_df_pandas["race_date"] = pd.to_datetime(races_df_pandas["race_date"], errors="coerce")
        
        wagers_df, ts_cols = convert_timestamp_columns(wagers_df)
        wagers_df.printSchema()
        wagers_pdf = wagers_df.toPandas()
        # wagers_df_pandas["race_date"] = pd.to_datetime(wagers_df_pandas["race_date"], errors="coerce")
        
        conn = db_pool.getconn()
        
        logging.info("Ingestion job succeeded")
        spark.catalog.clearCache()

        # 4) net sentiment update                    
        conn = db_pool.getconn()
        try:
            # wagers_df is a pandas DataFrame
            wagers_pdf['parsed_winners'] = wagers_pdf['winners'].astype(str).apply(parse_winners_str)
        except Exception as e:
            logging.error(f"Error updating wagers winners parsing: {e}")
            conn.rollback()
        try:
            implement_strategy(parquet_dir, races_pdf, wagers_pdf)
        except Exception as e:
            logging.error(f"Error updating placeholder 2: {e}")
    except Exception as e:
        logging.error(f"Error during Spark initialization: {e}")
        sys.exit(1)
    
        logging.info("All tasks completed. Spark session stopped and DB pool closed.")
    finally:
        if spark:
            spark.stop()
        if db_pool:
            db_pool.closeall()
            
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
