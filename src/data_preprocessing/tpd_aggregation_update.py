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
from pyspark.sql.functions import (
    col, min as F_min, max as F_max, sum as F_sum, avg as F_avg,
    when, count, first, last, expr, ntile, lag, lead, stddev, stddev_samp
)

from src.data_preprocessing.tpd_agg_queries import tpd_sql_queries
# If needed for merges or advanced windowing
# from pyspark.sql.window import Window

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
              
def update_net_sentiment(conn):
    """
    Example function that updates net_sentiment in `runners` table
    based on textual comments in runner records.
    """
    logging.info("Updating net_sentiment beginning...")
    start_time = time.time()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE runners r
                SET net_sentiment = sentiment_counts.sentiment_diff
                FROM (
                    SELECT
                        r1.axciskey,
                        COUNT(CASE WHEN r1.horse_comm LIKE '%[+%' THEN 1 END)
                        - COUNT(CASE WHEN r1.horse_comm LIKE '%[-%' THEN 1 END) 
                        AS sentiment_diff
                    FROM runners r1
                    GROUP BY r1.axciskey
                ) AS sentiment_counts
                WHERE r.axciskey = sentiment_counts.axciskey;
            """)
            conn.commit()
            elapsed = time.time() - start_time
            logging.info(f"Net sentiment updated successfully in {elapsed:.2f} seconds.")
    except Exception as e:
        logging.error(f"Error updating net_sentiment: {e}")
        conn.rollback()
        raise

def update_previous_race_data(conn):
    """
    Updates the `previous_distance`, `previous_surface`, and `off_finish_last_race` columns in the `runners` table
    with data from the horse's previous race.
    """
    logging.info("Updating previous race data in runners beginning...")
    start_time = time.time()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                WITH previous_race AS (
                    SELECT
                        r2.course_cd,
                        r2.race_date,
                        r2.race_number,
                        r2.saddle_cloth_number,
                        LAG(r.class_rating) OVER (PARTITION BY h.horse_id ORDER BY r.race_date) AS previous_class,
                        LAG(r.distance_meters) OVER (PARTITION BY h.horse_id ORDER BY r.race_date) AS previous_distance,
                        LAG(r.surface) OVER (PARTITION BY h.horse_id ORDER BY r.race_date) AS previous_surface,
                        LAG(re.official_fin) OVER (PARTITION BY h.horse_id ORDER BY r.race_date) AS off_finish_last_race
                    FROM races r
                    JOIN runners r2 ON r.course_cd = r2.course_cd
                        AND r.race_date = r2.race_date
                        AND r.race_number = r2.race_number
                    JOIN results_entries re ON r2.course_cd = re.course_cd
                        AND r2.race_date = re.race_date
                        AND r2.race_number = re.race_number
                        AND r2.saddle_cloth_number = re.program_num
                    JOIN horse h ON r2.axciskey = h.axciskey
                )
                UPDATE runners r2
                SET previous_class = pr.previous_class,
                    previous_distance = pr.previous_distance,
                    previous_surface = pr.previous_surface,
                    off_finish_last_race = pr.off_finish_last_race
                FROM previous_race pr
                WHERE r2.course_cd = pr.course_cd
                    AND r2.race_date = pr.race_date
                    AND r2.race_number = pr.race_number
                    AND r2.saddle_cloth_number = pr.saddle_cloth_number;
            """)
            conn.commit()
            elapsed = time.time() - start_time
            logging.info(f"Previous race data updated successfully in {elapsed:.2f} seconds.")
    except Exception as e:
        logging.error(f"Error updating previous race data: {e}")
        conn.rollback()
        raise

def update_previous_race_data_and_race_count(conn):
    """
    Updates the `previous_distance`, 'previous_class', `previous_surface`, and `off_finish_last_race` columns in the `runners` table
    with data from the horse's previous race. Also populates these columns with -1 for horses that have only run one race,
    and adds a `race_count` column to store the number of races each horse has participated in.
    """
    logging.info("Updating previous race data and race count in runners beginning...")
    start_time = time.time()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                WITH previous_race AS (
                    SELECT
                        r2.course_cd,
                        r2.race_date,
                        r2.race_number,
                        r2.saddle_cloth_number,
                        LAG(r.class_rating) OVER (PARTITION BY h.horse_id ORDER BY r.race_date) AS previous_class,
                        LAG(r.distance_meters) OVER (PARTITION BY h.horse_id ORDER BY r.race_date) AS previous_distance,
                        LAG(r.surface) OVER (PARTITION BY h.horse_id ORDER BY r.race_date) AS previous_surface,
                        LAG(re.official_fin) OVER (PARTITION BY h.horse_id ORDER BY r.race_date) AS off_finish_last_race,
                        COUNT(*) OVER (PARTITION BY h.horse_id) AS race_count
                    FROM races r
                    JOIN runners r2 ON r.course_cd = r2.course_cd
                        AND r.race_date = r2.race_date
                        AND r.race_number = r2.race_number
                    JOIN results_entries re ON r2.course_cd = re.course_cd
                        AND r2.race_date = re.race_date
                        AND r2.race_number = re.race_number
                        AND r2.saddle_cloth_number = re.program_num
                    JOIN horse h ON r2.axciskey = h.axciskey
                )
                UPDATE runners r2
                SET previous_class = COALESCE(pr.previous_class, -1),
                    previous_distance = COALESCE(pr.previous_distance, -1),
                    previous_surface = COALESCE(pr.previous_surface, 'NONE'),
                    off_finish_last_race = COALESCE(pr.off_finish_last_race, -1),
                    race_count = pr.race_count
                FROM previous_race pr
                WHERE r2.course_cd = pr.course_cd
                    AND r2.race_date = pr.race_date
                    AND r2.race_number = pr.race_number
                    AND r2.saddle_cloth_number = pr.saddle_cloth_number;
            """)
            conn.commit()
            elapsed = time.time() - start_time
            logging.info(f"Previous race data and race count updated successfully in {elapsed:.2f} seconds.")
    except Exception as e:
        logging.error(f"Error updating previous race data and race count: {e}")
        conn.rollback()
        raise
        
def update_distance_meters(conn):
    """
    Updates the `distance_meters` column in the `racedata` table by converting the `distance` and `dist_unit` columns.
    """
    logging.info("Updating distance_meters in racedata beginning...")
    start_time = time.time()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE racedata
                SET distance_meters = CASE
                    WHEN dist_unit = 'F' THEN (distance/100) * 201.168
                    WHEN dist_unit = 'M' THEN distance
                    WHEN dist_unit = 'Y' THEN distance * 0.9144
                    ELSE NULL
                END;
            """)
            conn.commit()
            elapsed = time.time() - start_time
            logging.info(f"Distance_meters updated successfully in {elapsed:.2f} seconds.")
    except Exception as e:
        logging.error(f"Error updating distance_meters: {e}")
        conn.rollback()
        raise

def calculate_gps_metrics_quartile_and_write(
    spark,
    df,
    jdbc_url,
    jdbc_properties,
    conn=None
):
    """
    Calculates GPS-derived metrics (acceleration, jerk, distance covered, etc.)
    for each horse in each race, broken down by quartiles, and writes the result
    to 'gps_aggregated'.

    The final DataFrame has both:
      - quartile-specific columns (speed_q1, speed_q2, etc.)
      - overall metrics (avg_acceleration, distance_covered, etc.)

    :param spark: SparkSession
    :param df: Spark DataFrame with columns:
               [course_cd, race_date, race_number, saddle_cloth_number,
                time_stamp, speed, stride_frequency, progress, ...]
    :param jdbc_url: JDBC URL to write final output
    :param jdbc_properties: dict with keys user, password, driver, etc.
    :param conn: optional psycopg2 connection for direct DB ops
    :return: None
    """

    import logging
    import time
    from pyspark.sql.functions import (
        col, when, lag, first, last, avg as F_avg, stddev_samp, sum as F_sum,
        max as F_max, min as F_min, ntile
    )
    from pyspark.sql import Window

    logging.info("Starting quartile-based GPS metrics calculation...")

    start_time = time.time()

    # ----------------------------------------------------------------------
    # 1) Validate columns
    # ----------------------------------------------------------------------
    required_cols = [
        "course_cd", "race_date", "race_number",
        "saddle_cloth_number", "time_stamp",
        "speed", "stride_frequency", "progress"
    ]
    for rc in required_cols:
        if rc not in df.columns:
            raise ValueError(f"Missing required column: {rc}")

    # ----------------------------------------------------------------------
    # 2) Filter invalid rows
    # ----------------------------------------------------------------------
    filtered_df = df.filter(
        (col("stride_frequency").isNotNull()) &
        (col("progress") != 0)
    )

    # ----------------------------------------------------------------------
    # 3) Sort data, compute time deltas
    # ----------------------------------------------------------------------
    gps_window = Window.partitionBy(
        "course_cd", "race_date", "race_number", "saddle_cloth_number"
    ).orderBy("time_stamp")

    df_sorted = filtered_df.withColumn(
        "prev_ts", lag(col("time_stamp").cast("long")).over(gps_window)
    )

    df_sorted = df_sorted.withColumn(
        "delta_t",
        (col("time_stamp").cast("long") - col("prev_ts")).cast("double")
    )

    # ----------------------------------------------------------------------
    # 4) Calculate acceleration / jerk
    # ----------------------------------------------------------------------
    df_sorted = df_sorted.withColumn("prev_speed", lag("speed").over(gps_window))

    df_sorted = df_sorted.withColumn(
        "acceleration",
        when(
            (col("delta_t") > 0) & col("prev_speed").isNotNull(),
            (col("speed") - col("prev_speed")) / col("delta_t")
        )
    )

    df_sorted = df_sorted.withColumn("prev_acc", lag("acceleration").over(gps_window))
    df_sorted = df_sorted.withColumn(
        "jerk",
        when(
            (col("delta_t") > 0) & col("prev_acc").isNotNull(),
            (col("acceleration") - col("prev_acc")) / col("delta_t")
        )
    )

    # ----------------------------------------------------------------------
    # 5) Distance covered segment
    # ----------------------------------------------------------------------
    df_sorted = df_sorted.withColumn(
        "dist_segment",
        when(
            (col("delta_t") > 0) & col("speed").isNotNull(),
            col("speed") * col("delta_t")
        ).otherwise(0.0)
    )

    # ----------------------------------------------------------------------
    # 6) Quartile assignment with ntile(4)
    # ----------------------------------------------------------------------
    df_with_q = df_sorted.withColumn(
        "quartile",
        ntile(4).over(gps_window)
    )

    # ----------------------------------------------------------------------
    # 7) For each (horse + quartile), aggregate metrics
    # ----------------------------------------------------------------------
    quartile_agg = df_with_q.groupBy(
        "course_cd", "race_date", "race_number", "saddle_cloth_number", "quartile"
    ).agg(
        F_avg("speed").alias("avg_speed_q"),
        F_avg("acceleration").alias("avg_accel_q"),
        F_avg("jerk").alias("avg_jerk_q"),
        F_sum("dist_segment").alias("sum_dist_q"),
        F_avg("stride_frequency").alias("avg_strfreq_q")
    )

    # Pivot to get columns: speed_q1, speed_q2, etc.
    pivoted_quart = (
        quartile_agg
        .groupBy("course_cd", "race_date", "race_number", "saddle_cloth_number")
        .pivot("quartile", [1, 2, 3, 4])
        .agg(
            F_min("avg_speed_q").alias("avg_speed"),
            F_min("avg_accel_q").alias("avg_accel"),
            F_min("avg_jerk_q").alias("avg_jerk"),
            F_min("sum_dist_q").alias("sum_dist"),
            F_min("avg_strfreq_q").alias("avg_strfreq")
        )
    )

    for q in [1, 2, 3, 4]:
        pivoted_quart = (
            pivoted_quart
            .withColumnRenamed(f"{q}_avg_speed",      f"speed_q{q}")
            .withColumnRenamed(f"{q}_avg_accel",      f"accel_q{q}")
            .withColumnRenamed(f"{q}_avg_jerk",       f"jerk_q{q}")
            .withColumnRenamed(f"{q}_sum_dist",       f"dist_q{q}")
            .withColumnRenamed(f"{q}_avg_strfreq",    f"strfreq_q{q}")
        )

    # ----------------------------------------------------------------------
    # 8) Overall (whole-race) aggregator
    # ----------------------------------------------------------------------
    race_horse = Window.partitionBy(
        "course_cd", "race_date", "race_number", "saddle_cloth_number"
    )
    df_sorted = df_sorted.withColumn(
        "speed_variability",
        stddev_samp("speed").over(race_horse)
    )

    overall_agg = df_sorted.groupBy(
        "course_cd", "race_date", "race_number", "saddle_cloth_number"
    ).agg(
        F_avg("acceleration").alias("avg_acceleration"),
        F_max("acceleration").alias("max_acceleration"),
        F_avg("jerk").alias("avg_jerk"),
        F_max("jerk").alias("max_jerk"),
        F_sum("dist_segment").alias("total_dist_covered"),
        F_avg("speed_variability").alias("speed_var"),
        F_avg(
            when(col("stride_frequency") > 0, col("speed") / col("stride_frequency"))
        ).alias("avg_stride_length"),
        F_avg("speed").alias("avg_speed_fullrace"),
    )

    df_sorted = df_sorted.withColumn(
        "first_progress",
        first("progress").over(race_horse.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
    ).withColumn(
        "last_progress",
        last("progress").over(race_horse.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
    )

    net_progress_df = df_sorted.groupBy(
        "course_cd", "race_date", "race_number", "saddle_cloth_number"
    ).agg(
        (F_max("last_progress") - F_max("first_progress")).alias("net_progress_gain")
    )

    overall_agg2 = overall_agg.join(
        net_progress_df,
        on=["course_cd", "race_date", "race_number", "saddle_cloth_number"],
        how="left"
    )

    # ----------------------------------------------------------------------
    # 9) Combine quartile pivot with overall aggregator
    # ----------------------------------------------------------------------
    final_result = pivoted_quart.join(
        overall_agg2,
        on=["course_cd", "race_date", "race_number", "saddle_cloth_number"],
        how="left"
    )

    # ----------------------------------------------------------------------
    # 10) Write final_result to gps_aggregated
    # ----------------------------------------------------------------------
    staging_table = "gps_aggregated"
    logging.info(f"Writing quartile + overall GPS metrics to {staging_table} ...")

    (
        final_result.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", staging_table)
        .option("user", jdbc_properties["user"])
        .option("driver", jdbc_properties["driver"])
        .mode("overwrite")
        .save()
    )

    elapsed = time.time() - start_time
    logging.info(f"GPS quartile metrics aggregated and written in {elapsed:.2f} seconds.")

    if conn:
        conn.close()
   
def spark_aggregate_sectionals_and_write(conn, df, jdbc_url, jdbc_properties):
    """
    1) Reads raw `sectionals` data via Spark JDBC or from parquet.
    2) Aggregates to produce early/late pace times, total_race_time, etc.
    3) Writes results back into `sectionals_aggregated` using Overwrite or Upsert logic.

    NOTE:
      - Adjust column references for gating/quarter times if needed.
      - Possibly join to `results_entries` if you only want matching records.
    """
    logging.info("Starting Spark-based aggregation for sectionals...")
    start_time = time.time()

    # Step 1: Count the number of gates for each race
    gate_counts = df.groupBy("course_cd", "race_date", "race_number", "saddle_cloth_number").agg(
        count("gate_numeric").alias("num_gates")
    )
    
    # Step 2: Join the gate counts back to the original DataFrame
    df_with_counts = df.join(gate_counts, on=["course_cd", "race_date", "race_number", "saddle_cloth_number"])

    # Step 3: Divide the gates into four parts using ntile
    df_with_ntile = df_with_counts.withColumn("quartile", ntile(4).over(Window.partitionBy("course_cd", "race_date", "race_number", "saddle_cloth_number").orderBy("gate_numeric")))
    
    # Step 4: Calculate the total distance_ran for each group
    total_distance_ran = df_with_ntile.groupBy("course_cd", "race_date", "race_number", "saddle_cloth_number").agg(
    F_sum("distance_ran").alias("total_distance_ran"),
    F_sum("sectional_time").alias("running_time"))
    
    # Step 5: Calculate the aggregates for each quartile
    quartile_aggregates = df_with_ntile.groupBy("course_cd", "race_date", "race_number", "saddle_cloth_number", "quartile").agg(
        F_avg("sectional_time").alias("avg_running_time"),
        last("distance_back").alias("distance_back"),
        F_sum("number_of_strides").alias("number_of_strides")
    )
    
    # Step 6: Pivot the quartile aggregates to get the desired columns
    # result1 = quartile_aggregates.groupBy("course_cd", "race_date", "race_number", "saddle_cloth_number").pivot("quartile").agg(
    #     first("avg_running_time").alias("avg_time_per_gate"),
    #     first("distance_back").alias("distance_back"),
    #     first("number_of_strides").alias("number_of_strides")
    # ).withColumnRenamed("first_quarter_pace","1").withColumnRenamed("second_quarter_pace", "2").withColumnRenamed("third_quarter_pace", "3").withColumnRenamed("fourth_quarter_pace", "4")

    result1 = quartile_aggregates.groupBy("course_cd", "race_date", "race_number", "saddle_cloth_number").pivot("quartile").agg(
        first("avg_running_time").alias("avg_time_per_gate"),
        first("distance_back").alias("distance_back"),
        first("number_of_strides").alias("number_of_strides")
    )

    # Rename the columns to place the quartile number after the name
    result1 = result1.withColumnRenamed("1_avg_time_per_gate", "avgtime_gate1") \
                    .withColumnRenamed("1_distance_back", "dist_bk_gate1") \
                    .withColumnRenamed("1_number_of_strides", "numstrides_gate1") \
                    .withColumnRenamed("2_avg_time_per_gate", "avgtime_gate2") \
                    .withColumnRenamed("2_distance_back", "dist_bk_gate2") \
                    .withColumnRenamed("2_number_of_strides", "numstrides_gate2") \
                    .withColumnRenamed("3_avg_time_per_gate", "avgtime_gate3") \
                    .withColumnRenamed("3_distance_back", "dist_bk_gate3") \
                    .withColumnRenamed("3_number_of_strides", "numstrides_gate3") \
                    .withColumnRenamed("4_avg_time_per_gate", "avgtime_gate4") \
                    .withColumnRenamed("4_distance_back", "dist_bk_gate4") \
                    .withColumnRenamed("4_number_of_strides", "numstrides_gate4")


    # Step 7: Join the total distance_ran back to the result
    result = result1.join(total_distance_ran, on=["course_cd", "race_date", "race_number", "saddle_cloth_number"])

    # ---------------
    # Write to `sectionals_aggregated`
    # ---------------
    staging_table = "sectionals_aggregated"

    logging.info("Writing aggregated results to staging table (overwrite)...")
    # Overwrite the staging table
    (
        result.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", staging_table)
        .option("user", jdbc_properties["user"])
        .option("driver", jdbc_properties["driver"])
        .mode("overwrite")
        .save()
    )
    logging.info(f"Staging table {staging_table} written successfully.")

    if conn:
        conn.close()

    elapsed = time.time() - start_time
    logging.info(f"Spark-based sectionals aggregation and write completed in {elapsed:.2f} seconds.")

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

    # 1) Create DB pool
    db_pool = get_db_pool(config)
    
    # 2) Initialize Spark
    try:
        spark, jdbc_url, jdbc_properties, parquet_dir, log_file = initialize_environment()
        
        setup_logging(script_dir, log_file)

        # Load and write data to parquet
        queries = tpd_sql_queries()
        dfs = load_data_from_postgresql(spark, jdbc_url, jdbc_properties, queries, parquet_dir)
    
        # Print schemas dynamically
        for name, df in dfs.items():
            print(f"DataFrame '{name}' Schema:")
            if name == "sectionals":
                conn = db_pool.getconn()
                try:
                    spark_aggregate_sectionals_and_write(conn, df, jdbc_url, jdbc_properties)
                finally:
                    db_pool.putconn(conn)
            if name == "gpspoint":
                conn = db_pool.getconn()
                try:
                    calculate_gps_metrics_quartile_and_write(conn, df, jdbc_url, jdbc_properties)
                finally:
                    db_pool.putconn(conn)        
        logging.info("Ingestion job succeeded")
        spark.catalog.clearCache()
    except Exception as e:
        logging.error(f"Error during Spark initialization: {e}")
        sys.exit(1)

    # 4) net sentiment update
    # Optionally run your net_sentiment update logic
    try:
        conn = db_pool.getconn()
        try:
            #update_net_sentiment(conn)
            #update_distance_meters(conn)
            update_previous_race_data_and_race_count(conn)
            # update_previous_race_data(conn)
        finally:
            db_pool.putconn(conn)
    except Exception as e:
        logging.error(f"Error updating net sentiment: {e}")
        traceback.print_exc()

    # 5) Update distance_meters
    try:
        conn = db_pool.getconn()
        try:
            update_distance_meters(conn)
        finally:
            db_pool.putconn(conn)
    except Exception as e:
        logging.error(f"Error updating distance_meters: {e}")
        traceback.print_exc()

    # 6) Cleanup
    if db_pool:
        db_pool.closeall()
    spark.stop()
    logging.info("All tasks completed. Spark session stopped and DB pool closed.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()