import logging
import os
import sys
import traceback
import time
import configparser
from pyspark.sql.functions import to_date

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession

from psycopg2 import pool, DatabaseError
from pyspark.sql.functions import (
    col, when, lit, row_number, expr,
    min as F_min, max as F_max, datediff,
    lag, count, trim
)

# ------------------------------------------------
# 1) Basic logging
# ------------------------------------------------
def setup_logging(script_dir, log_file):
    """Sets up logging configuration to write logs to a file."""
    try:
        # Truncate the log file first
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

        logging.info("Logging initialized.")
    except Exception as e:
        print(f"Failed to set up logging: {e}", file=sys.stderr)
        sys.exit(1)

# ------------------------------------------------
# 2) Config + DB pool
# ------------------------------------------------
def read_config(script_dir, config_relative_path='../../config.ini'):
    """Reads the configuration file and returns the configuration object."""
    try:
        config = configparser.ConfigParser()
        config_file_path = os.path.join(script_dir, config_relative_path)
        if not os.path.exists(config_file_path):
            raise FileNotFoundError(f"Configuration file '{config_file_path}' does not exist.")
        config.read(config_file_path)

        if 'database' not in config:
            raise KeyError("Missing 'database' section in the config file.")
        return config
    except Exception as e:
        logging.error(f"Error reading configuration file: {e}")
        sys.exit(1)

def get_db_pool(config):
    """Creates a connection pool to PostgreSQL."""
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
            logging.info("Using password authentication.")
        else:
            logging.info("No password in config; relying on .pgpass or other method.")

        db_pool = pool.SimpleConnectionPool(1, 10, **db_pool_args)
        if db_pool:
            logging.info("Connection pool created successfully.")
        return db_pool
    except DatabaseError as e:
        logging.error(f"Database error creating pool: {e}")
        sys.exit(1)
    except KeyError as e:
        logging.error(f"Missing config key: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        sys.exit(1)

# ------------------------------------------------
# 3) The main "recent form" logic
# ------------------------------------------------
def compute_recent_form_metrics(
    spark,
    race_df,
    workouts_df,
    jdbc_url,
    jdbc_properties,
    conn=None
):
    """
    For each row in race_df (each (horse_id, race_date, ...)),
    compute the horse's 'recent form' metrics based on the
    *earlier* races (3-race, 5-race averages, speed improvement, etc.),
    plus days_off & workout aggregates.

    Then write a row for each (horse_id, race_date) to horse_recent_form.
    This version adds avg_speed_3 and avg_speed_5 (averages of 'avg_speed_fullrace')
    over the last 3 or 5 *previous* races.
    """

    logging.info("Starting compute_recent_form_metrics (row-based).")

    # Suppose 'race_df' has:
    #  - horse_id
    #  - race_date
    #  - official_fin (finishing position)
    #  - speed_rating
    #  - dist_bk_gate4 or something for beaten_len
    #  - avg_speed_fullrace  (the speed measure we want to average)
    #  - etc.

    # We'll define an ascending window by (horse_id, race_date),
    # so earlier races have smaller row_number => the current row is the "future" race
    
    race_df = race_df.withColumn("race_date", to_date("race_date"))
    window_race = Window.partitionBy("horse_id").orderBy("race_date")

    # Convert relevant columns to double if not already
    race_df = race_df.withColumn("speed_rating", col("speed_rating").cast("double"))
    race_df = race_df.withColumn("avg_speed_fullrace", col("avg_speed_fullrace").cast("double"))

    # 1) row_number -> ascending by date
    race_df_asc = race_df.withColumn(
        "race_ordinal",
        row_number().over(window_race)
    )

    # 2) We'll define windows: the 3 prior rows and 5 prior rows
    #    i.e. rowBetween(-3, -1) for last 3; rowBetween(-5, -1) for last 5
    w_last_3 = window_race.rowsBetween(-3, -1)
    w_last_5 = window_race.rowsBetween(-5, -1)

    # 3) avg_fin_3, avg_fin_5
    race_df_asc = race_df_asc.withColumn(
        "avg_fin_3", F.avg("official_fin").over(w_last_3)
    ).withColumn(
        "avg_fin_5", F.avg("official_fin").over(w_last_5)
    )

    # 4) avg_speed_3, avg_speed_5 (based on 'avg_speed_fullrace')
    race_df_asc = race_df_asc.withColumn(
        "avg_speed_3", F.avg("avg_speed_fullrace").over(w_last_3)
    ).withColumn(
        "avg_speed_5", F.avg("avg_speed_fullrace").over(w_last_5)
    )

    # 5) beaten_len = dist_bk_gate4 or 0 if missing
    race_df_asc = race_df_asc.withColumn(
    "beaten_len", F.when(col("dist_bk_gate4").isNull(), 9999.0).otherwise(col("dist_bk_gate4")))  # Not filling with 0 because that means the horse won
    
    race_df_asc = race_df_asc.withColumn(
        "avg_beaten_3",
        F.avg("beaten_len").over(w_last_3)
    ).withColumn(
        "avg_beaten_5",
        F.avg("beaten_len").over(w_last_5)
    )

    # 6) Speed improvement vs. prior race
    race_df_asc = race_df_asc.withColumn(
        "prev_speed",
        lag("speed_rating", 1).over(window_race)
    ).withColumn(
        "speed_improvement",
        (col("speed_rating") - col("prev_speed"))
    )

    # 7) Days off = difference in race_date from prior race
    race_df_asc = race_df_asc.withColumn(
        "prev_race_date",
        lag("race_date", 1).over(window_race)
    ).withColumn(
        "days_off",
        F.when(
            col("prev_race_date").isNotNull(),
            datediff(col("race_date"), col("prev_race_date"))
        )
    )
    
    # layoff_cat
    race_df_asc = race_df_asc.withColumn(
        "layoff_cat",
        F.when(col("days_off") <= 14,  lit("short"))
         .when(col("days_off") <= 45,  lit("medium"))
         .when(col("days_off").isNull(), lit("N/A"))
         .otherwise(lit("long"))
    )

    # 8) Workouts aggregator
    # We'll do a simple aggregator keyed by (horse_id, race_date).
    # We assume your workouts_df has columns: [horse_id, race_date, days_back, ranking, ...]
    # We'll pick last 3 workouts => a window partitioned by (horse_id, race_date) ordered by days_back asc

    w_win = Window.partitionBy("horse_id", "race_date").orderBy(col("days_back").asc())
    workouts_df2 = workouts_df.withColumn("w_ordinal", row_number().over(w_win))

    w_agg = (
        workouts_df2.filter(col("w_ordinal") <= 3)
        .groupBy("horse_id", "race_date")
        .agg(
            F.avg("ranking").alias("avg_workout_rank_3"),
            count("*").alias("count_workouts_3")
        )
    )

    # 9) Merge w_agg into race_df_asc, so each row now has the workout aggregator
    final_df = race_df_asc.join(
        w_agg,
        on=["horse_id", "race_date"],
        how="left"
    )

    # 10) Write to horse_recent_form
    staging_table = "horse_recent_form"
    # Cast columns to match database schema
    final_df = final_df.withColumn("course_cd", trim(col("course_cd")).cast("string")) \
                    .withColumn("saddle_cloth_number", trim(col("saddle_cloth_number")).cast("string"))

    logging.info(f"Writing final per-race form metrics to {staging_table} ...")

    (
        final_df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", staging_table)
        .option("user", jdbc_properties["user"])
        .option("driver", jdbc_properties["driver"])
        .mode("overwrite")  # or "append"
        .save()
    )

    logging.info("Finished writing row-based recent form metrics.")
    if conn:
        conn.close()

# ------------------------------------------------
# 4) main
# ------------------------------------------------
def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = read_config(script_dir)
    log_file = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/recent_form.log"
    setup_logging(script_dir, log_file)

    # Create DB pool (optional)
    db_pool = get_db_pool(config)

    # 1) Initialize your SparkSession and load data
    from src.data_preprocessing.data_prep1.data_utils import initialize_environment
    from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql
    from src.data_preprocessing.horse_recent_form_queries import form_sql_queries

    spark, jdbc_url, jdbc_properties, parquet_dir, _ = initialize_environment()

    # Suppose we have a dictionary of queries
    queries = form_sql_queries()  # you define these queries

    # Load them
    dfs = load_data_from_postgresql(
        spark, jdbc_url, jdbc_properties,
        queries, parquet_dir
    )

    race_df = None
    workouts_df = None
    for name, df in dfs.items():
        logging.info(f"DataFrame '{name}' loaded. Schema:")
        df.printSchema()
        if name == "races":
            race_df = df
        elif name == "workouts":
            workouts_df = df

    if race_df is None:
        logging.error("No 'races' DataFrame found. Check queries.")
        sys.exit(1)
    if workouts_df is None:
        logging.warning("No 'workouts' DataFrame found. Using empty DF.")
        # Optionally create an empty DF with the same schema
        workouts_df = spark.createDataFrame([], race_df.schema)
    # 2) Compute
    compute_recent_form_metrics(
        spark=spark,
        race_df=race_df,
        workouts_df=workouts_df,
        jdbc_url=jdbc_url,
        jdbc_properties=jdbc_properties,
        conn=None
    )

    # 3) Cleanup
    spark.stop()
    logging.info("Spark session stopped.")
    if db_pool:
        db_pool.closeall()
        logging.info("DB connection pool closed.")

if __name__ == "__main__":
    main()