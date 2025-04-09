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
    p_i = (scores_i)^(1/T) / sum_j (scores_j)^(1/T),
    computed stably via a log-sum-exp style approach.
    scores must be >= 0.
    """
    eps = 1e-12
    scores = np.array(scores, dtype=float)

    # Take log
    log_scores = np.log(scores + eps)
    # Multiply by (1/T)
    scaled_logs = (1.0 / T) * log_scores

    # Shift so max is 0 to avoid overflow
    shift = np.max(scaled_logs)
    shifted = np.exp(scaled_logs - shift)
    denom = np.sum(shifted) + eps

    return shifted / denom

def neg_log_likelihood_temperature(T, df):
    """
    Summed over all races: -log(prob_of_winner).
    df has columns: [race_key, score (>=0), winner (0/1)].
    For each race, p_i = score_i^(1/T) / sum_j score_j^(1/T).
    """
    eps = 1e-12
    total_nll = 0.0

    for _, grp in df.groupby('race_key', sort=False):
        scores = grp['score'].values
        winners = grp['winner'].values

        # compute probabilities for each horse
        probs = apply_temperature_scaling(scores, T)

        # negative log-likelihood for the winner
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
    # B) Query the calibration subset from `predictions_20250407_211045_1`
    #    => must have official_fin != null + > 0 to define who the winner is, plus score>0
    #
    conn = db_pool.getconn()
    cursor = conn.cursor()
    sql_cal = """
        SELECT
            course_cd,
            race_date,
            race_number,
            top_1_score AS score,
            CASE WHEN official_fin=1 THEN 1 ELSE 0 END AS winner
        FROM predictions_20250407_211045_1
        WHERE official_fin IS NOT NULL
          AND official_fin > 0
          AND top_1_score > 0
        ORDER BY course_cd, race_date, race_number
    """
    try:
        cursor.execute(sql_cal)
        rows = cursor.fetchall()
        df_cal = pd.DataFrame(rows, columns=[
            "course_cd","race_date","race_number","score","winner"
        ])
        logging.info(f"Loaded {len(df_cal)} calibration rows from predictions_20250407_211045_1.")
    except Exception as e:
        logging.error(f"Error loading calibration data: {e}", exc_info=True)
        sys.exit(1)
    finally:
        cursor.close()
        db_pool.putconn(conn)

    if df_cal.empty:
        logging.error("No calibration data found. Exiting.")
        sys.exit(1)

    # build a race_key
    df_cal["race_key"] = (
        df_cal["course_cd"].astype(str)
        + "_"
        + df_cal["race_date"].astype(str)
        + "_"
        + df_cal["race_number"].astype(str)
    )

    # Fit T
    T_best = fit_temperature_scaling(df_cal)

    #
    # C) Query the "future" subset => official_fin IS NULL, or race_date>=CURRENT_DATE
    #
    conn = db_pool.getconn()
    cursor = conn.cursor()

    sql_future = """
        SELECT
            course_cd,
            race_date,
            race_number,
            horse_id,
            saddle_cloth_number,
            top_1_score AS score,
            top_1_rank AS "rank",
            group_id,
            post_time,
            track_name,
            has_gps,
            horse_name,
            global_speed_score_iq,
            morn_odds,
            official_fin
        FROM predictions_20250407_211045_1
        WHERE official_fin IS NULL
          AND race_date >= CURRENT_DATE
        ORDER BY course_cd, race_date, race_number, saddle_cloth_number
    """
    try:
        cursor.execute(sql_future)
        rows = cursor.fetchall()
        future_cols = [
            "course_cd","race_date","race_number","horse_id","saddle_cloth_number",
            "score","rank","group_id","post_time","track_name","has_gps",
            "horse_name","global_speed_score_iq","morn_odds","official_fin"
        ]
        future_df = pd.DataFrame(rows, columns=future_cols)
        logging.info(f"Loaded {len(future_df)} future predictions from same table.")
    except Exception as e:
        logging.error(f"Error loading future data: {e}", exc_info=True)
        sys.exit(1)
    finally:
        cursor.close()
        db_pool.putconn(conn)

    if future_df.empty:
        logging.info("No future rows found to calibrate. Exiting.")
        sys.exit(0)

    # build race_key
    future_df["race_key"] = (
        future_df["course_cd"].astype(str)
        + "_"
        + future_df["race_date"].astype(str)
        + "_"
        + future_df["race_number"].astype(str)
    )

    # apply T-based scaling => final "calibrated_prob"
    def group_temp_scaling(scores, T):
        arr = scores.values
        p_i = apply_temperature_scaling(arr, T)
        return pd.Series(p_i, index=scores.index)

    future_df["calibrated_prob"] = (
        future_df.groupby("race_key", group_keys=False)["score"]
                 .apply(lambda s: group_temp_scaling(s, T_best))
    )

    # D) Write final => predictions_20250407_211045_1_calibrated
    spark_df = spark.createDataFrame(future_df)
    calibrate_table = "predictions_20250407_211045_1_calibrated"

    (
        spark_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", calibrate_table)
        .option("user", jdbc_properties["user"])
        .option("driver", jdbc_properties["driver"])
        .mode("overwrite")
        .save()
    )

    logging.info(f"Wrote calibrated future predictions to {calibrate_table}")
    spark.stop()
    logging.info("All done with calibration pipeline.")


if __name__ == "__main__":
    main()