import pandas as pd
import logging
from .config import settings
from src.wagering_bandit.wagering_bandit_queries import wager_queries
from src.data_preprocessing.data_prep1.data_utils import initialize_environment
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql
from src.wagering.wagering_helper_functions import (setup_logging, read_config, get_db_pool, parse_winners_str, get_user_wager_preferences,
                                                    convert_timestamp_columns, gather_bet_metrics, save_results_to_parquet)

def add_race_key(df):
    df = df.copy()
    # 1) ensure race_date is a datetime
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    # 2) normalize race_number → int
    df["race_number_int"] = (
        pd.to_numeric(df["race_number"], errors="coerce")
          .fillna(0)
          .astype(int)
    )
    # 3) build the key
    df["race_key"] = (
        df["course_cd"].astype(str).str.strip() + "_"
      + df["race_date"].dt.strftime("%Y-%m-%d") + "_"
      + df["race_number_int"].astype(str)
    )
    return df

def load_predictions_table(spark, conn, jdbc_url, jdbc_properties, parquet_dir):
    """
    Pull the {calibrated_table} into pandas, add field_size.
    """
    # Load and write data to parquet
    queries = wager_queries()
    dfs = load_data_from_postgresql(spark, jdbc_url, jdbc_properties, queries, parquet_dir)
    # Suppose we have a dictionary of queries
    for name, df in dfs.items():
        logging.info(f"DataFrame '{name}' loaded. Schema:")
        df.printSchema()
        if name == "race_predictions":
            races_df = df
        elif name == "wagers":
            wagers_df = df
        else:
            logging.error(f"Unknown DataFrame name: {name}")
            continue
    # Fixing the datetime column in a PySpark DataFrame
    races_df, ts_cols = convert_timestamp_columns(races_df)
    races_pdf = races_df.toPandas()        
    # races_df_pandas["race_date"] = pd.to_datetime(races_df_pandas["race_date"], errors="coerce")
    
    wagers_df, ts_cols = convert_timestamp_columns(wagers_df)
    wagers_pdf = wagers_df.toPandas()
    # wagers_df_pandas["race_date"] = pd.to_datetime(wagers_df_pandas["race_date"], errors="coerce")
    decimal_cols = ['num_tickets', 'payoff', 'pool_total']
    for col in decimal_cols:
        wagers_pdf[col] = wagers_pdf[col].astype(float)
   
    try:
        wagers_pdf['winners'] = wagers_pdf['winners'].astype(str)
        wagers_pdf['parsed_winners'] = wagers_pdf['winners'].apply(parse_winners_str)
        wagers_pdf = add_race_key(wagers_pdf)
        races_pdf = add_race_key(races_pdf)
    except Exception as e:
        logging.error(f"Error updating wagers winners parsing: {e}")
        conn.rollback()

    logging.info(f"races_pdf columns: {list(races_pdf.columns)}")
    logging.info(races_pdf.head())
    logging.info(f"wagers_pdf columns: {list(wagers_pdf.columns)}")
    logging.info(wagers_pdf.head())
        
    return races_pdf, wagers_pdf

def assemble_bandit_training_data(pred_df, rewards_df, feature_cols):
    """
    Merge pred_df + rewards_df on race_key → X,y,r arrays.
    """
    df = rewards_df.merge(
        pred_df[feature_cols + ["race_key"]].drop_duplicates(),
        on="race_key", how="left"
    )
    X = df[feature_cols].values
    y = df["arm"].values
    r = df["reward"].values
    return X, y, r
