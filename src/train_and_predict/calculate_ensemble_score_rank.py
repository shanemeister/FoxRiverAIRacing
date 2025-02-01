import logging
import os
import sys
import traceback
import time
import configparser
import pandas as pd
import psycopg2
from psycopg2 import sql, pool, DatabaseError
from pyspark.sql import SparkSession, Window
from src.data_preprocessing.tpd_agg_queries import tpd_sql_queries
from pyspark.sql.functions import (
    col, min as F_min, max as F_max, sum as F_sum, avg as F_avg,
    when, count, first, last, expr, ntile, lag, lead, stddev,
    stddev_samp, row_number, desc, exp
)

# If needed for merges or advanced windowing
# from pyspark.sql.window import Window
"""
     Load each model’s predictions from catboost_enriched_results,
     Pivot to compute an ensemble score & rank (one row per group_id+horse_id),
     Then join that back to catboost_enriched_results so the final result
     looks identical to the original table but with ensemble_score & ensemble_rank
     and it is in the ensemble_average_results table.
"""
# -------------
# Local imports
# -------------
from src.data_ingestion.ingestion_utils import update_ingestion_status
from src.data_preprocessing.data_prep1.data_utils import initialize_environment
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql, reload_parquet_files

def setup_logging(script_dir, log_file):
    """Sets up logging configuration to write logs to a file and the console."""
    try:
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

def read_config(script_dir, config_relative_path='../../config.ini'):
    """
    Reads the configuration file and returns the configuration object.
    """
    try:
        config = configparser.ConfigParser()
        config_file_path = os.path.abspath(os.path.join(script_dir, config_relative_path))
        if not os.path.exists(config_file_path):
            raise FileNotFoundError(f"Configuration file '{config_file_path}' does not exist.")
        config.read(config_file_path)
        if 'database' not in config:
            raise KeyError("The 'database' section is missing in the configuration file.")
        return config
    except Exception as e:
        logging.error(f"Error reading configuration file: {e}")
        sys.exit(1)


def get_db_pool(config):
    """
    Creates a connection pool to PostgreSQL.
    """
    try:
        db_pool_args = {
            'user': config['database']['user'],
            'host': config['database']['host'],
            'port': config['database']['port'],
            'database': config['database']['dbname']
        }
        
        password = config['database'].get('password')
        if password:
            db_pool_args['password'] = password
            logging.info("Password found in configuration. Using provided password.")
        else:
            logging.info("No password in config. Attempting .pgpass or other authentication.")

        db_pool = pool.SimpleConnectionPool(
            1, 20,  # min and max connections
            **db_pool_args
        )
        if db_pool:
            logging.info("Connection pool created successfully.")
        return db_pool
    except DatabaseError as e:
        logging.error(f"Database error creating connection pool: {e}")
        sys.exit(1)
    except KeyError as e:
        logging.error(f"Missing configuration key: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error creating connection pool: {e}")
        sys.exit(1)

def load_model_performace(conn, spark, jdbc_url, jdbc_properties):
    import time
    from pyspark.sql.functions import col, first, expr, row_number, desc
    from pyspark.sql.window import Window

    logging.info("Loading model performance from catboost_enriched_results...")
    start_time = time.time()
    try:
        # 1) Load all rows
        original_df = (
            spark.read
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", "catboost_enriched_results")
                .option("user", jdbc_properties["user"])
                .option("driver", jdbc_properties["driver"])
                .load()
        )

        # 2) Filter to your 10 model_keys for pivot
        predictions_df = original_df.filter(
            col("model_key").isin(
                "YetiRank:top=4_NDCG:top=3_20250130_223226",
                "YetiRank:top=3_NDCG:top=1_20250130_213102",
                "YetiRank:top=2_NDCG:top=4_20250130_212111",
                "YetiRank:top=3_NDCG:top=4_20250130_220147", 
                "YetiRank:top=2_NDCG:top=2_20250130_210109", 
                "YetiRank:top=4_NDCG:top=2_20250130_222148", 
                "YetiRank:top=3_NDCG:top=2_20250130_214059",
                "YetiRank:top=4_NDCG:top=1_20250130_221155", 
                "YetiRank:top=3_NDCG:top=3_20250130_215120",
                "YetiRank:top=1_NDCG:top=3_20250130_203047"   
            )
        )

        # 3) Pivot by model_key => columns score_A..score_J
        pivoted_df = (
            predictions_df
            .groupBy("group_id", "horse_id")
            .pivot("model_key", [
                "YetiRank:top=4_NDCG:top=3_20250130_223226",
                "YetiRank:top=3_NDCG:top=1_20250130_213102",
                "YetiRank:top=2_NDCG:top=4_20250130_212111",
                "YetiRank:top=3_NDCG:top=4_20250130_220147", 
                "YetiRank:top=2_NDCG:top=2_20250130_210109", 
                "YetiRank:top=4_NDCG:top=2_20250130_222148", 
                "YetiRank:top=3_NDCG:top=2_20250130_214059",
                "YetiRank:top=4_NDCG:top=1_20250130_221155", 
                "YetiRank:top=3_NDCG:top=3_20250130_215120",
                "YetiRank:top=1_NDCG:top=3_20250130_203047"           
            ])
            .agg(first("prediction").alias("score"))
        )

        # 4) Rename columns
        renamed_df = (
            pivoted_df
            .withColumnRenamed("YetiRank:top=4_NDCG:top=3_20250130_223226", "score_A")
            .withColumnRenamed("YetiRank:top=3_NDCG:top=1_20250130_213102", "score_B")
            .withColumnRenamed("YetiRank:top=2_NDCG:top=4_20250130_212111", "score_C")
            .withColumnRenamed("YetiRank:top=3_NDCG:top=4_20250130_220147", "score_D")
            .withColumnRenamed("YetiRank:top=2_NDCG:top=2_20250130_210109", "score_E")
            .withColumnRenamed("YetiRank:top=4_NDCG:top=2_20250130_222148", "score_F")
            .withColumnRenamed("YetiRank:top=3_NDCG:top=2_20250130_214059", "score_G")
            .withColumnRenamed("YetiRank:top=4_NDCG:top=1_20250130_221155", "score_H")
            .withColumnRenamed("YetiRank:top=3_NDCG:top=3_20250130_215120", "score_I")
            .withColumnRenamed("YetiRank:top=1_NDCG:top=3_20250130_203047", "score_J")
        )

        # 5) Compute ensemble_score = average of 10 model scores
        #    If some columns are missing => might be null, so handle that if needed
        ensemble_df = renamed_df.withColumn(
            "ensemble_score",
            (
              expr("score_A + score_B + score_C + score_D + score_E + "+
                   "score_F + score_G + score_H + score_I + score_J")
              / 10.0
            )
        )

        # 6) Convert ensemble_score => "ensemble_prob" via softmax (race-level)
        from pyspark.sql.functions import exp, sum as F_sum

        # exp_ens = e^(ensemble_score)
        ensemble_df = ensemble_df.withColumn("exp_ens", exp(col("ensemble_score")))

        # sum_exp_ens = sum of all e^(ensemble_score) for horses in same group_id
        w = Window.partitionBy("group_id")
        ensemble_df = ensemble_df.withColumn("sum_exp_ens", F_sum("exp_ens").over(w))

        # ensemble_prob = e^(ensemble_score) / sum(e^(ensemble_score))
        ensemble_df = ensemble_df.withColumn(
            "ensemble_prob",
            col("exp_ens") / col("sum_exp_ens")
        )

        # Optionally define ensemble_rank by ordering the ensemble_prob descending
        w2 = Window.partitionBy("group_id").orderBy(desc("ensemble_prob"))
        ranked_df = ensemble_df.withColumn("ensemble_rank", row_number().over(w2))

        # 7) Join back to original table
        final_df = (
            original_df.alias("orig")
            .join(
                ranked_df.alias("ens"),
                on=["group_id","horse_id"],
                how="left"
            )
            .select(
                "orig.*",
                "ens.ensemble_score",
                "ens.ensemble_prob",   # new column
                "ens.ensemble_rank"
            )
        )

        # 8) Overwrite final table
        logging.info("Writing final DataFrame to 'ensemble_average_results' with mode=overwrite...")
        (
            final_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "ensemble_average_results")
            .option("user", jdbc_properties["user"])
            .option("driver", jdbc_properties["driver"])
            .mode("overwrite")
            .save()
        )

        elapsed = time.time() - start_time
        logging.info(f"Ensemble model scores + softmax completed in {elapsed:.2f}s. "
                     f"Now stored in 'ensemble_average_results'.")
        
    except Exception as e:
        logging.error(f"Error generating ensemble_average_results: {e}")
        conn.rollback()
        raise
   
def main():
    """
    Main function to:
      - Initialize environment
      - Create SparkSession
      - Create DB connection pool
      - Run Spark-based sectionals aggregation
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = read_config(script_dir)

    # 1) Create DB pool
    db_pool = get_db_pool(config)
    
    # 2) Initialize Spark
    try:
        spark, jdbc_url, jdbc_properties, parquet_dir, log_file = initialize_environment()
        
        setup_logging(script_dir, log_file)
        conn = db_pool.getconn()
        try:
            load_model_performace(conn, spark, jdbc_url, jdbc_properties)
        finally:
            logging.info("Ingestion job succeeded")
        spark.catalog.clearCache()
    except Exception as e:
        logging.error(f"Error during Spark initialization: {e}")
        sys.exit(1)

    # 6) Cleanup
    if db_pool:
        db_pool.closeall()
    spark.stop()
    logging.info("All tasks completed. Spark session stopped and DB pool closed.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()