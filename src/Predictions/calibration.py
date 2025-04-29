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
        log_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs'
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'inference_prep.log')

        # Clear the log file
        with open(log_file, 'w'):
            pass
        
        logger = logging.getLogger()
        if logger.hasHandlers():
            logger.handlers.clear()
        logger.setLevel(logging.INFO)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

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
# 2) POWER-BASED TEMPERATURE SCALING
########################################
import numpy as np

def apply_temperature_scaling(scores, T):
    """
    p_i = exp((score_i - max_score)/T) / sum_j exp((score_j - max_score)/T).

    This ensures we avoid overflow by shifting the scores so the largest is 0.
    It is strictly monotonic in the original scores, so the ordering is preserved.
    """
    eps = 1e-12
    scores = np.array(scores, dtype=float)
    
    # Optionally clamp extreme raw scores to avoid huge exponents
    # (You can adjust limits as desired)
    scores = np.clip(scores, -50, 50)

    # Shift so largest score is 0
    max_s = np.max(scores)
    shifted = (scores - max_s) / T

    # Exponentiate and normalize
    exp_vals = np.exp(shifted)
    denom = np.sum(exp_vals) + eps
    return exp_vals / denom

def neg_log_likelihood_temperature(T, df):
    """
    Summed over all races: -log(prob_of_winner).
    df has columns: [race_key, score, winner].
    For each race, p_i = apply_temperature_scaling(score_i, T).
    """
    eps = 1e-12
    total_nll = 0.0

    for _, grp in df.groupby('race_key', sort=False):
        scores = grp['score'].values
        winners = grp['winner'].values

        probs = apply_temperature_scaling(scores, T)
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
# 3) MAIN
########################################
def main():
    # A) Setup environment + DB
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_file = os.path.join(script_dir, "calibrate_multinomial.log")
    setup_logging(log_file)

    spark, jdbc_url, jdbc_properties, parquet_dir, _ = initialize_environment()
    config_path = os.path.join(script_dir, '../../config.ini')
    config = read_config(config_path)
    db_pool = get_db_pool(config)

    #
    # B) Query the calibration subset from `predictions_20250426_151421_1`
    #    => must have official_fin != null + > 0 to define who the winner is, plus score>0
    #
    conn = db_pool.getconn()
    cursor = conn.cursor()
    sql_cal = """
        SELECT
            course_cd,
            race_date,
            race_number,
            top_4_score AS score,
            CASE WHEN official_fin=1 THEN 1 ELSE 0 END AS winner
        FROM predictions_20250426_151421_1
        WHERE official_fin IS NOT NULL
          AND official_fin > 0
          AND top_4_score > 0
        ORDER BY course_cd, race_date, race_number
    """
    try:
        cursor.execute(sql_cal)
        rows = cursor.fetchall()
        df_cal = pd.DataFrame(rows, columns=[
            "course_cd","race_date","race_number","score","winner"
        ])
        logging.info(f"Loaded {len(df_cal)} calibration rows from predictions_20250426_151421_1.")
    except Exception as e:
        logging.error(f"Error loading calibration data: {e}", exc_info=True)
        sys.exit(1)
    finally:
        cursor.close()
        db_pool.putconn(conn)

    if df_cal.empty:
        logging.error("No calibration data found. Exiting.")
        sys.exit(1)

    # Build a race_key to group by race
    df_cal["race_key"] = (
        df_cal["course_cd"].astype(str)
        + "_"
        + df_cal["race_date"].astype(str)
        + "_"
        + df_cal["race_number"].astype(str)
    )

    # Fit T from historical data
    T_best = fit_temperature_scaling(df_cal)
    #
    # C) >> NEW: read _all_ original predictions, build race_key, and calibrate every row
    #
    conn = db_pool.getconn()
    cursor = conn.cursor()
    sql_all = """
        SELECT
            course_cd,
            race_date,
            race_number,
            horse_id,
            saddle_cloth_number,
            top_4_score AS score,
            top_4_rank AS rank,
            group_id,
            post_time,
            track_name,
            has_gps,
            horse_name,
            morn_odds,
            official_fin
        FROM predictions_20250426_151421_1
        ORDER BY course_cd, race_date, race_number, saddle_cloth_number
    """
    cursor.execute(sql_all)
    rows = cursor.fetchall()
    cols = [
        "course_cd","race_date","race_number","horse_id","saddle_cloth_number",
        "score","rank","group_id","post_time","track_name","has_gps",
        "horse_name","morn_odds","official_fin"
    ]
    full_df = pd.DataFrame(rows, columns=cols)
    cursor.close()
    db_pool.putconn(conn)

    # build the same race_key
    full_df["race_key"] = (
        full_df["course_cd"].astype(str) + "_" +
        full_df["race_date"].astype(str) + "_" +
        full_df["race_number"].astype(str)
    )

    # E) apply T to every race_key group
    def scale_group(s):
        return apply_temperature_scaling(s.values, T_best)

    full_df["calibrated_prob"] = (
        full_df
         .groupby("race_key")["score"]
         .transform(scale_group)
    )

    # F) write the fully‐calibrated table back
    spark_full = spark.createDataFrame(full_df)
    out_table = "predictions_20250426_151421_1_calibrated"
    (
        spark_full.write
                  .format("jdbc")
                  .option("url", jdbc_url)
                  .option("dbtable", out_table)
                  .option("user", jdbc_properties["user"])
                  .option("driver", jdbc_properties["driver"])
                  .mode("overwrite")
                  .save()
    )
    logging.info(f"Wrote fully calibrated predictions (hist + fut) to {out_table}")
    spark.stop()
    logging.info("All done with calibration pipeline.")


if __name__ == "__main__":
    main()