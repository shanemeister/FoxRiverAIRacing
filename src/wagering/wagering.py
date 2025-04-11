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
from src.wagering.wagers import implement_ExactaWager, implement_multi_race_wager
from src.wagering.wagering_helper_functions import (setup_logging, read_config, get_db_pool, parse_winners_str, get_user_wager_preferences,
                                                    convert_timestamp_columns, gather_bet_metrics, save_results_to_parquet)
from src.data_preprocessing.data_prep1.data_utils import save_parquet
    
def implement_strategy(spark, parquet_dir, races_pdf, wagers_pdf):
    # 1) Gather user inputs for the wager
    user_prefs = get_user_wager_preferences()
    wager_type = user_prefs["wager_type"]
    base_amount = user_prefs["base_amount"]
    num_legs = user_prefs["num_legs"]
    is_box = True


    # 2) Build Race objects, Wagers dict, etc.
    all_races = build_race_objects(races_pdf)
    # Check for missing or null values in 'wager_type'
    wagers_dict = build_wagers_dict(wagers_pdf)
    # 3) Instantiate the correct Wager subclass
    #    We'll do just an example for 'Exacta' or 'Daily Double'
    if wager_type == "Exacta":
        bet_results = implement_ExactaWager(all_races, wagers_dict, base_amount=base_amount, top_n=2, box=is_box)
        bet_results = spark.createDataFrame(bet_results)
        #bet_results.write.mode("overwrite").parquet(f"{parquet_dir}/exacta_results")
        save_parquet(spark, bet_results, "exacta_wagering", parquet_dir)
    elif wager_type in ["Pick 3", "Pick 4", "Pick 5", "Pick 6"]:
        bet_results = implement_multi_race_wager(all_races, wagers_dict, wager_type, num_legs, base_amount=2.0)
        for i,row in bet_results.iterrows():
            print(row.to_dict())  # debug each row
        input("Press Enter to continue...")
        bet_results = spark.createDataFrame(bet_results)
        # Convert wager_type to a filename
        filename = wager_type.lower().replace(" ", "") + "_wagering"
        save_parquet(spark, bet_results, filename, parquet_dir)
    # elif wager_type == "Daily Double":
    #     # use OOP approach:
    #     dd_pdf = implement_DailyDouble_OOP(all_races, wagers_dict, wagers_pdf, base_amount=base_amount)
    #     dd_spark = spark.createDataFrame(dd_pdf)
    #     save_parquet(spark, dd_spark, "daily_double_wagering", parquet_dir)
    # elif wager_type == "Pick 3":
    #     results = analyze_pick3_wagers(races_pdf, wagers_pdf, base_amount=base_amount, parquet_path=parquet_dir)
    #     results = spark.createDataFrame(results)
    #     save_parquet(spark, results, "pick3_wagering", parquet_dir)
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
        decimal_cols = ['num_tickets', 'payoff', 'pool_total']
        for col in decimal_cols:
            wagers_pdf[col] = wagers_pdf[col].astype(float)
            
        conn = db_pool.getconn()
        
        logging.info("Ingestion job succeeded")
        spark.catalog.clearCache()

        # 4) net sentiment update                    
        conn = db_pool.getconn()
        try:
            # wagers_df is a pandas DataFrame
            wagers_pdf['winners'] = wagers_pdf['winners'].astype(str)
            wagers_pdf['parsed_winners'] = wagers_pdf['winners'].apply(parse_winners_str)   
        except Exception as e:
            logging.error(f"Error updating wagers winners parsing: {e}")
            conn.rollback()
        try:
            implement_strategy(spark, parquet_dir, races_pdf, wagers_pdf)
        except Exception as e:
            logging.error(f"Error in implement_strategy: {e}")
            raise
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
