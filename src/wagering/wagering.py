import logging
import os
import sys
import traceback
import time
from pyspark.sql.functions import to_timestamp
from pyspark.sql import Window
from datetime import datetime
import pandas as pd

# Local imports
import src.wagering.wager_types as wt
from src.wagering.wagering_functions import build_race_objects, build_wagers_dict
from src.wagering.wagering_queries import wager_queries
from src.data_preprocessing.data_prep1.data_utils import initialize_environment
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql
from src.wagering.wagers import (
    implement_ExactaWager, implement_TrifectaWager, implement_SuperfectaWager,
    implement_multi_race_wager
)
from src.wagering.wagering_helper_functions import (
    setup_logging,
    read_config,
    get_db_pool,
    parse_winners_str,
    get_user_wager_preferences,
    convert_timestamp_columns,
    gather_bet_metrics,
    save_results_to_parquet
)
from src.data_preprocessing.data_prep1.data_utils import save_parquet


def implement_strategy(spark, parquet_dir, races_pdf, wagers_pdf):
    """
    Decide which wager type to run (Exacta, Trifecta, Daily Double, etc.)
    based on user prefs, then call the relevant function in wagers.py. 
    """
    # 1) Gather user inputs for the wager
    user_prefs = get_user_wager_preferences()
    wager_type = user_prefs["wager_type"]
    base_amount = user_prefs["base_amount"]
    top_n = user_prefs["top_n"]
    # If user picks 'Daily Double', we force num_legs=2 if it's not set
    if wager_type == "Daily Double":
        num_legs = 2
    else:
        num_legs = user_prefs["num_legs"]

    box = user_prefs["is_box"]

    # 2) Build Race objects, Wagers dict
    all_races = build_race_objects(races_pdf)
    wagers_dict = build_wagers_dict(wagers_pdf)

    # 3) Dispatch to the correct function from wagers.py
    if wager_type == "Exacta":
        bet_results_df = implement_ExactaWager(
            spark,
            all_races,
            wagers_dict,
            base_amount=base_amount,
            top_n=top_n,
            box=box
        )
        save_parquet(spark, bet_results_df, "exacta_wagering", parquet_dir)

    elif wager_type == "Trifecta":
        bet_results_df = implement_TrifectaWager(
            spark,
            all_races,
            wagers_dict,
            base_amount,
            top_n=top_n, 
            box=box
        )
        filename = wager_type.lower().replace(" ", "") + "_wagering"
        save_parquet(spark, bet_results_df, filename, parquet_dir)

    elif wager_type == "Superfecta":
        bet_results_df = implement_SuperfectaWager(
            spark,
            all_races,
            wagers_dict,
            base_amount,
            top_n=top_n, 
            box=box
        )
        filename = wager_type.lower().replace(" ", "") + "_wagering"
        save_parquet(spark, bet_results_df, filename, parquet_dir)

    elif wager_type in ["Daily Double", "Pick 3", "Pick 4", "Pick 5", "Pick 6"]:
        # For 'Daily Double', we have forced num_legs=2 above.
        bet_results_df = implement_multi_race_wager(
            spark,
            all_races,
            wagers_dict,
            wager_type,
            num_legs,
            base_amount=base_amount,
            top_n=top_n,
            box=box
        )
        filename = wager_type.lower().replace(" ", "") + "_wagering"
        save_parquet(spark, bet_results_df, filename, parquet_dir)

    else:
        logging.info(f"'{wager_type}' not yet implemented. Defaulting to Exacta.")
        bet_results_df = implement_ExactaWager(
            spark,
            all_races,
            wagers_dict,
            base_amount=base_amount,
            top_n=2,
            box=False
        )
        save_parquet(spark, bet_results_df, "exacta_wagering", parquet_dir)


def main():
    """
    Main function to:
      - Initialize environment
      - Create SparkSession
      - Create DB connection pool
      - Load "races" and "wagers" data
      - Parse and run implement_strategy
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = read_config(script_dir)
    dataset_log_file = os.path.join(
        script_dir,
        f"../../logs/{datetime.now().strftime('%Y-%m-%d')}_dataset.log"
    )
    setup_logging(script_dir, log_dir=dataset_log_file)

    # 1) Create DB pool
    db_pool = get_db_pool(config)

    # 2) Initialize Spark, logging, and load data
    try:
        spark, jdbc_url, jdbc_properties, parquet_dir, _ = initialize_environment()
        setup_logging(script_dir, log_dir=dataset_log_file)

        # Load data from your wager_queries
        queries = wager_queries()
        dfs = load_data_from_postgresql(spark, jdbc_url, jdbc_properties, queries, parquet_dir)

        # Identify "races" and "wagers" from the DFS
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

        # Convert timestamp columns
        races_df, _ = convert_timestamp_columns(races_df)
        wagers_df, _ = convert_timestamp_columns(wagers_df)

        # Convert to Pandas for usage in build_race_objects
        races_pdf = races_df.toPandas()
        wagers_pdf = wagers_df.toPandas()

        # Convert numeric columns
        decimal_cols = ['num_tickets', 'payoff', 'pool_total']
        for col in decimal_cols:
            if col in wagers_pdf.columns:
                wagers_pdf[col] = wagers_pdf[col].astype(float)

        # Acquire a DB connection (if needed)
        conn = db_pool.getconn()

        logging.info("Ingestion job succeeded")
        spark.catalog.clearCache()

        # 3) Parse 'winners' → 'parsed_winners' in wagers PDF
        try:
            wagers_pdf['winners'] = wagers_pdf['winners'].astype(str)
            wagers_pdf['parsed_winners'] = wagers_pdf['winners'].apply(parse_winners_str)
        except Exception as e:
            logging.error(f"Error updating wagers winners parsing: {e}")
            conn.rollback()

        # 4) Implement the main strategy
        try:
            implement_strategy(spark, parquet_dir, races_pdf, wagers_pdf)
        except Exception as e:
            logging.error(f"Error in implement_strategy: {e}")
            raise

    except Exception as e:
        logging.error(f"Error during Spark initialization: {e}")
        sys.exit(1)

    finally:
        if spark:
            spark.stop()
        if db_pool:
            db_pool.closeall()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()