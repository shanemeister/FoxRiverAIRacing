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
    lag, count, trim, mean as F_mean
)
from src.data_preprocessing.data_prep1.data_utils import initialize_environment
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql
from src.data_preprocessing.horse_recent_form_queries import form_sql_queries

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
    race_df,
    workouts_df,
    jdbc_url,
    jdbc_properties,
    staging_table,
    conn=None,
    db_pool=None
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

    # 1) Convert race_date to DateType and define ascending window
    race_df = race_df.withColumn("race_date", F.to_date("race_date"))
    window_race = Window.partitionBy("horse_id").orderBy("race_date")

    # Convert relevant columns to double
    race_df = race_df.withColumn("speed_rating", F.col("speed_rating").cast("double"))
    race_df = race_df.withColumn("avg_speed_fullrace", F.col("avg_speed_fullrace").cast("double"))

    # 2) row_number -> ascending by date
    race_df_asc = race_df.withColumn("race_ordinal", F.row_number().over(window_race))

    # 3) Define windows *excluding the current row*:
    #    i.e., last 3 prior rows => rowsBetween(-3, -1)
    #    last 5 prior rows => rowsBetween(-5, -1)
    #  This ensures we do NOT include the current row's data in the average.
    w_last_3 = window_race.rowsBetween(-3, -1)
    w_last_5 = window_race.rowsBetween(-5, -1)

    # 4) Compute aggregates from the *last 3 or 5 prior races* (no current race)
    race_df_asc = (
        race_df_asc
        .withColumn("avg_fin_3", F.avg("official_fin").over(w_last_3))
        .withColumn("avg_fin_5", F.avg("official_fin").over(w_last_5))
        .withColumn("avg_speed_3", F.avg("avg_speed_fullrace").over(w_last_3))
        .withColumn("avg_speed_5", F.avg("avg_speed_fullrace").over(w_last_5))
    )

    # 5) Replace null dist_bk_gate4 with a global average to fill in missing
    filtered_df = race_df_asc.filter(
        (F.col("dist_bk_gate4").isNotNull()) & (F.col("dist_bk_gate4") != 0)
    )
    avg_dist = filtered_df.select(F.mean(F.col("dist_bk_gate4")).alias("mean_dist")) \
                          .collect()[0]["mean_dist"]
    race_df_asc = race_df_asc.withColumn(
        "beaten_len",
        F.when(F.col("dist_bk_gate4").isNull(), F.lit(avg_dist))
         .otherwise(F.col("dist_bk_gate4"))
    )

    # 6) Aggregates for beaten_len from prior races
    race_df_asc = (
        race_df_asc
        .withColumn(
            "avg_beaten_3",
            F.avg(
                F.when(
                    (F.col("beaten_len").isNotNull()) & (F.col("beaten_len") != 0),
                    F.col("beaten_len")
                )
            ).over(w_last_3)
        )
        .withColumn(
            "avg_beaten_5",
            F.avg(
                F.when(
                    (F.col("beaten_len").isNotNull()) & (F.col("beaten_len") != 0),
                    F.col("beaten_len")
                )
            ).over(w_last_5)
        )
    )

    # 7) Speed improvement => difference between the last 2 prior races, NOT the current race
    #    So we do lag1_speed = race i-1, lag2_speed = race i-2, and improvement = lag1 - lag2.
    race_df_asc = (
        race_df_asc
        .withColumn("lag1_speed", F.lag("speed_rating", 1).over(window_race))
        .withColumn("lag2_speed", F.lag("speed_rating", 2).over(window_race))
        .withColumn("speed_improvement",
            when(
                # exactly one prior race: lag2 null but lag1 exists
                col("lag2_speed").isNull() & col("lag1_speed").isNotNull(),
                lit(0)
            ).when(
                # two or more prior races: true delta
                col("lag1_speed").isNotNull() & col("lag2_speed").isNotNull(),
                col("lag1_speed") - col("lag2_speed")
            )
        )
    )
    
    # 8) Days off = difference in race_date from the prior race
    race_df_asc = (
        race_df_asc
        .withColumn("prev_race_date", F.lag("race_date", 1).over(window_race))
        .withColumn(
            "days_off",
            F.when(
                F.col("prev_race_date").isNotNull(),
                F.datediff(F.col("race_date"), F.col("prev_race_date"))
            )
        )
        .withColumn(
            "layoff_cat",
            F.when(F.col("days_off") <= 14,  F.lit("short"))
             .when(F.col("days_off") <= 45,  F.lit("medium"))
             .when(F.col("days_off").isNull(), F.lit("N/A"))
             .otherwise(F.lit("long"))
        )
    )

    # 9) Workouts aggregator: last 3 workouts for (horse_id, race_date)
    w_win = Window.partitionBy("horse_id", "race_date").orderBy(F.col("days_back").asc())
    workouts_df2 = workouts_df.withColumn("w_ordinal", F.row_number().over(w_win))

    w_agg = (
        workouts_df2.filter(F.col("w_ordinal") <= 3)
        .groupBy("horse_id", "race_date")
        .agg(
            F.avg("ranking").alias("avg_workout_rank_3"),
            F.count("*").alias("count_workouts_3")
        )
    )

    final_df = race_df_asc.join(
        w_agg,
        on=["horse_id", "race_date"],
        how="left"
    )

    # 10) Save to DB table
    final_df = (
        final_df
        .withColumn("course_cd", F.trim(F.col("course_cd")).cast("string"))
        .withColumn("saddle_cloth_number", F.trim(F.col("saddle_cloth_number")).cast("string"))
    )

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

    add_pk_and_indexes(db_pool, staging_table)
    logging.info(f"Finished writing aggregated form per horse per race {staging_table}.")

    if conn:
        conn.close()
        
def add_pk_and_indexes(db_pool, output_table):
        try:
            if output_table == "horse_form_agg":    
                ddl_statements = [
                    f"ALTER TABLE {output_table} ADD PRIMARY KEY (horse_id, as_of_date)",
                    f"CREATE INDEX idx_{output_table}_horse_id ON {output_table} (horse_id)",
                    f"CREATE INDEX idx_{output_table}_as_of_date ON {output_table} (as_of_date)"
                ]
            elif output_table == "horse_recent_form":
                ddl_statements = [
                    f"ALTER TABLE {output_table} ADD PRIMARY KEY (course_cd, race_date, race_number, saddle_cloth_number)",
                    f"CREATE INDEX idx_{output_table}_horse_id ON {output_table} (horse_id)"
                    ]
            else:
                logging.error(f"Unknown table name: {output_table}")
                return
                
            conn = None
            # Borrow a connection from the pool
            conn = db_pool.getconn()
            conn.autocommit = True
            with conn.cursor() as cursor:
                for ddl in ddl_statements:
                    print(f"Executing: {ddl}")
                    cursor.execute(ddl)
                    # no results, just a command
                print("DDL statements executed successfully.")            
        except Exception as e:
            print(f"Error executing DDL: {e}")
        finally:
            if conn:
                db_pool.putconn(conn)
            
def compute_horse_form_agg(
    spark,
    jdbc_url,
    jdbc_properties,
    conn=None,
    output_table="horse_form_agg",
    db_pool=None
):
    """
    Reads from 'horse_recent_form' (already built by compute_recent_form_metrics),
    and for each (horse_id, race_date) row, aggregates:
      - last 5 races' finishing position, speed rating, etc.
      - picks the single 'most recent' race info for prev_speed, days_off, etc.
    Writes the final DataFrame to `horse_form_agg` (or a specified table).
    """

    # 1) Load the entire 'horse_recent_form' from Postgres
    horse_rf_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "horse_recent_form")  # or schema.horse_recent_form
        .option("user", jdbc_properties["user"])
        .option("driver", jdbc_properties["driver"])
        .load()
    )

    # Convert race_date to actual date type
    horse_rf_df = horse_rf_df.withColumn("race_date", F.to_date(F.col("race_date")))

    # 2) We'll define a Window partitioned by (horse_id) and ordered by race_date ascending
    #    so for each row, we can find the "up to 5 prior races"
    w_asc = Window.partitionBy("horse_id").orderBy("race_date")

    # a) row_number -> ascending by date
    #    So row_number=1 is the earliest race, row_number=2 next, etc.
    df_asc = horse_rf_df.withColumn("rn_asc", F.row_number().over(w_asc))

    # b) We'll define another window for "the last 5 races up to (and including) the current row."
    #    rowBetween(-5, -1) => the previous row plus the 4 preceding to that row
    w_last_5 = w_asc.rowsBetween(-5, -1)

    # c) Compute aggregates over the last 5 (including current race). 
    #    If you strictly want "prior 5 races (excluding this one)," use rowsBetween(-5, -1).
    df_agg_5 = (
        df_asc
        .withColumn("count_races_5", F.count("*").over(w_last_5))
        .withColumn("avg_fin_5", F.avg("official_fin").over(w_last_5))
        .withColumn("avg_speed_5", F.avg("speed_rating").over(w_last_5))
        .withColumn("best_speed_5", F.max("speed_rating").over(w_last_5))
        .withColumn("avg_beaten_len_5", F.avg("beaten_len").over(w_last_5))
        .withColumn("min_race_date_5", F.min("race_date").over(w_last_5))
        .withColumn("max_race_date_5", F.max("race_date").over(w_last_5))

        .withColumn("avg_dist_bk_gate1_5", F.avg("dist_bk_gate1").over(w_last_5))
        .withColumn("avg_dist_bk_gate2_5", F.avg("dist_bk_gate2").over(w_last_5))
        .withColumn("avg_dist_bk_gate3_5", F.avg("dist_bk_gate3").over(w_last_5))
        .withColumn("avg_dist_bk_gate4_5", F.avg("dist_bk_gate4").over(w_last_5))
        .withColumn("avg_speed_fullrace_5", F.avg("avg_speed_fullrace").over(w_last_5))
        .withColumn("avg_stride_length_5", F.avg("avg_stride_length").over(w_last_5))
        .withColumn("avg_strfreq_q1_5", F.avg("strfreq_q1").over(w_last_5))
        .withColumn("avg_strfreq_q2_5", F.avg("strfreq_q2").over(w_last_5))
        .withColumn("avg_strfreq_q3_5", F.avg("strfreq_q3").over(w_last_5))
        .withColumn("avg_strfreq_q4_5", F.avg("strfreq_q4").over(w_last_5))
    )

    # 3) The single "most recent race" info for prev_speed, speed_improvement, days_off, etc.
    #    Actually, in each row, that info is *already* from this row in horse_recent_form,
    #    so we can just rename or keep them. We'll treat "race_date" as the "as_of_date."

    # 4) We'll select the columns we want in the final table, using the "as_of_date = race_date" approach
    final_cols = [
        "horse_id",
        F.col("race_date").alias("as_of_date"),

        # 5-race aggregates
        F.col("count_races_5").alias("total_races_5"),
        "avg_fin_5",
        "avg_speed_5",
        F.col("best_speed_5").alias("best_speed"),
        "avg_beaten_len_5",
        F.col("min_race_date_5").alias("first_race_date_5"),
        F.col("max_race_date_5").alias("most_recent_race_5"),

        "avg_dist_bk_gate1_5",
        "avg_dist_bk_gate2_5",
        "avg_dist_bk_gate3_5",
        "avg_dist_bk_gate4_5",
        "avg_speed_fullrace_5",
        "avg_stride_length_5",
        "avg_strfreq_q1_5",
        "avg_strfreq_q2_5",
        "avg_strfreq_q3_5",
        "avg_strfreq_q4_5",

        # Single-race columns from this row
        F.col("lag1_speed").alias("prev_speed"),
        "speed_improvement",
        "prev_race_date",
        "days_off",
        "layoff_cat",
        "avg_workout_rank_3",
        "count_workouts_3"
    ]

    final_df = df_agg_5.select(*final_cols)

    # 5) Write out to Postgres table "horse_form_agg" (or a name you pass in)
    logging.info(f"Writing aggregated horse form to {output_table} ...")

    (
        final_df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", output_table)
        .option("user", jdbc_properties["user"])
        .option("driver", jdbc_properties["driver"])
        .mode("overwrite")  # or "append"
        .save()
    )

    add_pk_and_indexes(db_pool, output_table)
    logging.info(f"Finished writing aggregated form to {output_table}.")
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
        race_df=race_df,
        workouts_df=workouts_df,
        jdbc_url=jdbc_url,
        jdbc_properties=jdbc_properties,
        staging_table="horse_recent_form",
        conn=None,
        db_pool=db_pool
    )

    compute_horse_form_agg(
        spark,
        jdbc_url=jdbc_url,
        jdbc_properties=jdbc_properties,
        conn=None,
        output_table="horse_form_agg",
        db_pool=db_pool
    )
    
    # 3) Cleanup
    spark.stop()
    logging.info("Spark session stopped.")
    if db_pool:
        db_pool.closeall()
        logging.info("DB connection pool closed.")

if __name__ == "__main__":
    main()
    
