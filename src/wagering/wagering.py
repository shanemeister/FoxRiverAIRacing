import logging
import os
import traceback, sys
import time
from pyspark.sql.functions import to_timestamp
from pyspark.sql import Window
from datetime import datetime
import numpy as np
import pandas as pd
import src.wagering.wager_types as wt
from src.wagering.wager_functions import build_race_objects, build_wagers_dict, clean_races_df
from src.wagering.wager_queries import wager_queries
from src.data_preprocessing.data_prep1.data_utils import initialize_environment
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql
from src.wagering.wagers import (
    implement_ExactaWager, implement_TrifectaWager, implement_SuperfectaWager,
    implement_multi_race_wager
)
from src.wagering.wager_helper_functions import (
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
from decimal import Decimal
from wager_config       import (MIN_FIELD, MAX_FIELD, BANKROLL_START,
                           TRACK_MIN, KELLY_THRESHOLD, KELLY_FRACTION,
                           MAX_FRACTION, EDGE_MIN)
from wager_rules  import WAGER_RULES, choose_rule

_DECIMAL_KINDS = ("object", "category")      # pandas dtypes that can hide Decimals

def force_float(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    Cast the listed columns to plain float64, turning Decimal → float.
    Ignores columns that are already float or missing from the DataFrame.
    """
    for c in cols:
        if c in df.columns and df[c].dtype in _DECIMAL_KINDS:
            if isinstance(df[c].iloc[0], Decimal):          # cheap probe
                df[c] = pd.to_numeric(df[c], downcast="float", errors="coerce")
    return df

    # -------- group-level apply (keep whole DataFrame) --------
    out_df = (
        races_pdf
        .groupby(["course_cd", "race_date", "race_number"],
                 group_keys=False, observed=True)
        .apply(_calc_features)
        .reset_index(drop=True)          # keep it flat
    )
    return out_df

# ────────────────────────────────────────────────────────────
# SAVE *live* bets (future races) to DB / CSV / etc.
# ────────────────────────────────────────────────────────────
def persist_live_bets(live_bets: pd.DataFrame, out_csv: str):
    """Very light-weight example – just write a CSV you can upload."""
    if live_bets.empty:
        logging.info("No live bets generated for today.")
        return
    live_bets.to_csv(out_csv, index=False)
    logging.info(f"Wrote {len(live_bets)} live bets → {out_csv}")

def implement_strategy(spark, parquet_dir, races_pdf, wagers_pdf):
    """
    Decide which wager type to run (Exacta, Trifecta, Daily Double, etc.)
    based on user prefs, then call the relevant function in wagers.py. 
    """
    # print(list(races_pdf.columns))
    # print(list(wagers_pdf.columns))
    
    # 1) Gather user inputs for the wager
    user_prefs = get_user_wager_preferences()
    wager_type = user_prefs["wager_type"]
    wager_amount = user_prefs["wager_amount"]
    top_n = user_prefs["top_n"]
    num_legs = user_prefs["num_legs"]
    box = user_prefs["is_box"]

    # 1) clean + feature-add in one call
    races_pdf = clean_races_df(races_pdf, wagers_pdf)

    # 2) build Race objects
    all_races = build_race_objects(races_pdf)
    #print(races_pdf.columns.tolist())           # ✅ DataFrame columns
    # print(len(all_races), "Race objects")  
    wagers_dict = build_wagers_dict(wagers_pdf)
    # print(wagers_dict.keys())
    # 3) Dispatch to the correct function from wagers.py
    if wager_type == "Exacta":
        bet_results_df = implement_ExactaWager(
            spark,
            all_races,
            wagers_dict,
            wager_amount,
            top_n=top_n,
            box=box
        )
        #save_parquet(spark, bet_results_df, "exacta_wagering", parquet_dir)

    elif wager_type == "Trifecta":
        bet_results_df = implement_TrifectaWager(
            spark,
            all_races,
            wagers_dict,
            wager_amount,
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
            wager_amount,
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
            num_legs=num_legs,
            wager_amount=wager_amount,
            top_n=top_n,
            box=box
        )
        filename = wager_type.lower().replace(" ", "") + "_wagering"
        save_parquet(spark, bet_results_df, filename, parquet_dir)

    else:
        logging.info(f"'{wager_type}' not yet implemented.")
        
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
        if "races" not in dfs:
            raise ValueError("No 'races' key found in the loaded dictionary!")
        
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
        if races_df.count() == 0:
            logging.warning("No rows in 'races' DataFrame. Exiting.")
            return
        # Convert to Pandas for usage in build_race_objects
        races_pdf = races_df.toPandas()
        wagers_pdf = wagers_df.toPandas() 
    
        # ------------------------------------------------------------------
        numeric_cols = ["morn_odds", "score",          # from races
                        "payoff", "num_tickets"]       # from wagers
        races_pdf   = force_float(races_pdf, numeric_cols)
        wagers_pdf  = force_float(wagers_pdf, numeric_cols)
        # ------------------------------------------------------------------

        # Optional — only if you want mathematically-priced vertical exotics
        # Convert logits to “skill” parameter s_i = exp(logit)


        # -------------------------------------------------
        # 1)  build a key shared by both data sets
        # -------------------------------------------------
        wagers_pdf["race_key"] = (
            wagers_pdf["course_cd"].str.upper().str.strip() + "_" +
            wagers_pdf["race_date"].astype(str) + "_" +
            wagers_pdf["race_number"].astype(str)
        )
        # 4) Implement the main strategy
        try:
            implement_strategy(spark, parquet_dir, races_pdf, wagers_pdf)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)   # ← TEMP: show real line
            logging.error("Error in implement_strategy: %s", e)
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