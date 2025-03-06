import os
import sys
import logging
import psycopg2
import numpy as np
import pandas as pd
import configparser
from psycopg2 import pool
from datetime import datetime
from scipy.optimize import minimize

from src.data_preprocessing.data_prep1.data_utils import initialize_environment
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

########################################
# 1) SETUP CONFIG / DB POOL
########################################
def setup_logging(log_file):
    """Sets up logging configuration to write logs to a file and the console."""
    try:
        # Default log directory
        log_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs'
        
        # Ensure the log directory exists
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'inference_prep.log')

        # Clear the log file by opening it in write mode
        with open(log_file, 'w'):
            pass  # This will truncate the file without writing anything
        
        # Create a logger and clear existing handlers
        logger = logging.getLogger()
        if logger.hasHandlers():
            logger.handlers.clear()

        logger.setLevel(logging.INFO)

        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        # Create console handler
        #console_handler = logging.StreamHandler()
        #console_handler.setLevel(logging.INFO)

        # Define a common format
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        #console_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(file_handler)
        #logger.addHandler(console_handler)

        logger.info("Logging has been set up successfully.")
    except Exception as e:
        print(f"Failed to set up logging: {e}", file=sys.stderr)
        sys.exit(1)
        
def read_config(config_path='../../config.ini'):
    config = configparser.ConfigParser()
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")
    config.read(config_path)
    if 'database' not in config:
        raise KeyError("Missing 'database' section in the config file.")
    return config

def get_db_pool(config):
    db_pool_args = {
        'user': config['database']['user'],
        'host': config['database']['host'],
        'port': config['database']['port'],
        'database': config['database']['dbname']
    }
    password = config['database'].get('password')
    if password:
        db_pool_args['password'] = password

    return pool.SimpleConnectionPool(1, 10, **db_pool_args)

########################################
# 2) SAFE SOFTMAX
########################################
def safe_softmax(arr):
    """A stable softmax for 1D array to reduce overflow."""
    arr = np.array(arr, dtype=float)
    shift = np.max(arr)
    exp_scores = np.exp(arr - shift)  # shift so max is 0
    return exp_scores / np.sum(exp_scores)

########################################
# 3) MULTINOMIAL TEMPERATURE SCALING
########################################
def apply_temperature_scaling(arr, T):
    """
    p_i = exp(logit_i / T) / sum_j exp(logit_j / T)
    with a shift to prevent overflow.
    """
    arr = np.array(arr, dtype=float)
    # Scale by 1/T
    scaled_logits = arr / T
    shift = np.max(scaled_logits)
    exp_vals = np.exp(scaled_logits - shift)
    return exp_vals / exp_vals.sum()

def neg_log_likelihood_temperature(T, df):
    """
    Summed over all races: -log(prob_of_winner).
    df has columns: [race_key, score (0..1), winner (0/1)].
    """
    eps = 1e-12
    total_nll = 0.0

    for _, grp in df.groupby('race_key', sort=False):
        scores = grp['score'].values
        winners = grp['winner'].values
        probs = apply_temperature_scaling(scores, T)
        # negative log-likelihood
        log_probs = np.log(probs + eps)
        total_nll += -np.sum(winners * log_probs)

    return total_nll

def fit_temperature_scaling(df_cal):
    """
    Minimizes neg log-likelihood across races in df_cal to find best T.
    """
    init_T = 1.0
    bounds = [(0.001, 100.0)]
    result = minimize(
        fun=neg_log_likelihood_temperature,
        x0=[init_T],
        args=(df_cal,),
        bounds=bounds,
        method='L-BFGS-B'
    )

    if result.success:
        best_T = result.x[0]
        logging.info(f"Fitted T successfully. T={best_T:.4f}")
    else:
        logging.warning("Minimize did not converge. Using T=1.0 as fallback.")
        best_T = 1.0

    return best_T

########################################
# 4) MAIN
########################################
def main():
    # -------------------------
    # A) Setup / Environment
    # -------------------------
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_file = os.path.join(script_dir, "calibrate_multinomial.log")
    setup_logging(log_file)

    # Initialize Spark, read config
    spark, jdbc_url, jdbc_properties, parquet_dir, _ = initialize_environment()
    config_path = os.path.join(script_dir, '../../config.ini')
    config = read_config(config_path)
    db_pool = get_db_pool(config)

    # -------------------------
    # B) Load HISTORICAL => Pandas => Fit T
    # -------------------------
    conn = db_pool.getconn()
    cursor = conn.cursor()
    sql_cal = """
        SELECT
            course_cd,
            race_date,
            race_number,
            prediction AS model_score,
            CASE WHEN official_fin=1 THEN 1 ELSE 0 END AS winner
        FROM catboost_enriched_results
        WHERE data_flag = 'historical'
          AND model_key = 'YetiRank:top=3_NDCG:top=2_20250304_184142'
    """
    try:
        cursor.execute(sql_cal)
        rows = cursor.fetchall()
        df_cal = pd.DataFrame(rows, columns=[
            "course_cd","race_date","race_number","score","winner"
        ])
        logging.info(f"Loaded {len(df_cal)} rows of calibration data.")
    except Exception as e:
        logging.error(f"Error loading calibration data: {e}", exc_info=True)
        sys.exit(1)
    finally:
        cursor.close()
        db_pool.putconn(conn)

    if df_cal.empty:
        logging.error("No calibration data found. Exiting.")
        sys.exit(1)

    # Build race_key
    df_cal['race_key'] = (
        df_cal['course_cd'].astype(str) + "_" +
        df_cal['race_date'].astype(str) + "_" +
        df_cal['race_number'].astype(str)
    )

    # Convert raw score => per-race softmax
    df_cal['score'] = df_cal.groupby('race_key')['score'].transform(safe_softmax)
    # Fit T
    T_best = fit_temperature_scaling(df_cal)

    # -------------------------
    # C) Load FUTURE => Calibrate => Spark Write
    # -------------------------
    conn = db_pool.getconn()
    cursor = conn.cursor()
    sql_future = """
        SELECT
         morn_odds, group_id, post_time,track_name, has_gps, horse_name,
         global_speed_score_iq, course_cd, race_date,race_number,
         horse_id, saddle_cloth_number,yetirank_ndcg_top_2 AS model_score
        FROM predictions_2025_03_05_1
        WHERE race_date >= CURRENT_DATE
        ORDER BY course_cd, race_date, race_number, saddle_cloth_number
    """
    try:
        cursor.execute(sql_future)
        rows = cursor.fetchall()
        future_df = pd.DataFrame(rows, columns=[
            "morn_odds", "group_id", "post_time","track_name", "has_gps", "horse_name",
         "global_speed_score_iq", "course_cd", "race_date","race_number",
         "horse_id", "saddle_cloth_number" , "score"
        ])
        logging.info(f"Loaded {len(future_df)} future predictions.")
    except Exception as e:
        logging.error(f"Error loading future predictions: {e}", exc_info=True)
        sys.exit(1)
    finally:
        cursor.close()
        db_pool.putconn(conn)

    if future_df.empty:
        logging.info("No future predictions found. Nothing to calibrate.")
        sys.exit(0)

    future_df['race_key'] = (
        future_df['course_cd'].astype(str) + "_" +
        future_df['race_date'].astype(str) + "_" +
        future_df['race_number'].astype(str)
    )

    # 1) Softmax for each race
    future_df['unscaled_prob'] = future_df.groupby('race_key')['score'].transform(safe_softmax)

    # 2) Temperature scaling
    #   We'll group only on ["unscaled_prob"], not the entire DataFrame,
    #   to avoid the DeprecationWarning about grouping columns.
    def group_temp_scaling(series, T):
        arr = series.values
        scaled_probs = apply_temperature_scaling(arr, T)  # returns a NumPy array
        # Return as a Series with the same index as 'series'
        return pd.Series(scaled_probs, index=series.index)

    # Instead of .apply(lambda g: group_temp_scaling(g, T_best)) on the entire DF,
    # we group specifically on the "unscaled_prob" column:

    # "grouped_result" is a Series of arrays. We assign it back to future_df['calibrated_prob']
    future_df['calibrated_prob'] = (
    future_df.groupby('race_key', group_keys=False)['unscaled_prob']
            .apply(lambda s: group_temp_scaling(s, T_best))
)
    # Now we have a fully calibrated DataFrame in Pandas

    # -------------------------
    # D) Convert to Spark DF, overwrite predictions_2025_03_05_1_calibrated
    # -------------------------
    spark_df = spark.createDataFrame(future_df)

    (
        spark_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "predictions_2025_03_05_1_calibrated")
        .option("user", jdbc_properties["user"])
        .option("driver", jdbc_properties["driver"])
        .mode("overwrite")
        .save()
    )

    logging.info("Wrote calibrated predictions to predictions_2025_03_05_1_calibrated via Spark.")
    spark.stop()
    logging.info("All done with calibration pipeline.")

if __name__ == "__main__":
    main()