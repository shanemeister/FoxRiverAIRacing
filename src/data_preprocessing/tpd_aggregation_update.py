import logging
import os
import sys
import traceback
import time
from pyspark.sql import Window
from datetime import datetime
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
from src.data_ingestion.ingestion_utils import update_ingestion_status
from src.data_preprocessing.data_prep1.data_utils import initialize_environment
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql, reload_parquet_files

def setup_logging(sript_dir, log_dir=None):
    """Sets up logging configuration to write logs to a file and the console."""
    try:
        # Default log directory
        if not log_dir:
            log_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs'
        
        # Ensure the log directory exists
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'tpd_aggregation.log')
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
            print("Net sentiment updated successfully.")
            elapsed = time.time() - start_time
            logging.info(f"Net sentiment updated successfully in {elapsed:.2f} seconds.")
    except Exception as e:
        logging.error(f"Error updating net_sentiment: {e}")
        conn.rollback()
        raise

def update_previous_surface(conn):
    cursor = conn.cursor()
    update_query = """
        UPDATE runners r
        SET previous_surface = (
            SELECT r2.surface
            FROM races r2
            WHERE r2.course_cd = r.course_cd
            AND r2.race_date < r.race_date  -- Ensure we get a past race
            AND EXISTS (
                SELECT 1 
                FROM results_entries re
                WHERE re.course_cd = r2.course_cd
                    AND re.race_date = r2.race_date
                    AND re.race_number = r2.race_number
                    AND re.program_num = r.saddle_cloth_number
            )
            ORDER BY r2.race_date DESC, r2.race_number DESC  -- Get the most recent past race
            LIMIT 1
        )
        WHERE r.previous_surface IS NULL
"""
    
    try:
        cursor.execute(update_query)
        conn.commit()
        logging.info("Update previous_surface successful.")
    except Exception as e:
        logging.error("Error during update: %s", e)
        conn.rollback()

def update_results_entries_speed_rating():
    config = configparser.ConfigParser()
    config.read('config.ini')
    conn = psycopg2.connect(
        host=config['database']['host'],
        port=config['database']['port'],
        database=config['database']['dbname'],
        user=config['database']['user'],
        password=config['database']['password']
    )
    cursor = conn.cursor()
    query = """
    UPDATE results_entries re
    SET speed_rating = (
        SELECT re2.speed_rating
        FROM results_entries re2
        WHERE re2.axciskey = re.axciskey
          AND (re2.race_date, re2.race_number) < (re.race_date, re.race_number)
          AND re2.speed_rating IS NOT NULL
        ORDER BY re2.race_date DESC, re2.race_number DESC
        LIMIT 1
    )
    WHERE re.speed_rating IS NULL;
    """
    try:
        cursor.execute(query)
        conn.commit()
        logging.info("Updated results_entries: filled missing speed_rating via LOCF.")
    except Exception as e:
        logging.error("Error updating results_entries: %s", e)
        conn.rollback()

def update_runners_off_fin_last_race(conn):
    cursor = conn.cursor()
    query = """
    UPDATE runners r
    SET prev_official_fin = (
        SELECT re.official_fin
        FROM results_entries re
        WHERE re.program_num = r.saddle_cloth_number  -- Match horse
        AND re.course_cd = r.course_cd
        AND re.race_date < r.race_date  -- Ensure it's a previous race
        ORDER BY re.race_date DESC  -- Get the most recent past race
        LIMIT 1
    )
    WHERE EXISTS (
        SELECT 1
        FROM results_entries re
        WHERE re.program_num = r.saddle_cloth_number
        AND re.course_cd = r.course_cd
        AND re.race_date < r.race_date
    )
    """
    try:
        cursor.execute(query)
        conn.commit()
        logging.info("Updated runners: prev_speed_rating set from results_entries.speed_rating.")
    except Exception as e:
        logging.error("Error updating runners: %s", e)
        conn.rollback()
    return

def update_runners_prev_speed_rating(conn):
    cursor = conn.cursor()
    query = """
    UPDATE runners r
    SET prev_speed_rating = (
        SELECT re.speed_rating
        FROM results_entries re
        WHERE re.program_num = r.saddle_cloth_number  -- Match horse
        AND re.course_cd = r.course_cd
        AND re.race_date < r.race_date  -- Ensure it's a previous race
        ORDER BY re.race_date DESC  -- Get the most recent past race
        LIMIT 1
    )
    WHERE EXISTS (
        SELECT 1
        FROM results_entries re
        WHERE re.program_num = r.saddle_cloth_number
        AND re.course_cd = r.course_cd
        AND re.race_date < r.race_date
    )
    """
    try:
        cursor.execute(query)
        conn.commit()
        logging.info("Updated runners: prev_speed_rating set from results_entries.speed_rating.")
    except Exception as e:
        logging.error("Error updating runners: %s", e)
        conn.rollback()
    return
        
def update_speed_rating(conn):  
    cursor = conn.cursor()
    update_query = """
    UPDATE results_entries re1
    SET speed_rating = (
        SELECT re2.speed_rating
        FROM results_entries re2
        WHERE re2.axciskey = re1.axciskey
          AND (re2.race_date, re2.race_number) > (re1.race_date, re1.race_number)
          AND re2.speed_rating IS NOT NULL
        ORDER BY re2.race_date ASC, re2.race_number ASC
        LIMIT 1
    )
    WHERE re1.speed_rating IS NULL;
    """
    try:
        cursor.execute(update_query)
        conn.commit()
        logging.info("Speed_rating updated using forward-fill (LOCF) successfully.")
    except Exception as e:
        logging.error("Error updating speed_rating: %s", e)
        conn.rollback()
                
def update_previous_race_data_and_race_count(conn):
    """
    Updates the runners table so that each row has data about that horse's most recent PRIOR race:
      - previous_class        (from the prior race's runners.todays_cls)
      - previous_distance     (from races.distance_meters)
      - previous_surface      (from races.surface)
      - prev_speed_rating     (from results_entries.speed_rating)
      - race_count            (the total number of starts for that horse)

    If a prior race doesn’t exist (e.g. only 1 career start),
    set numeric fields to -1 and surface to 'NONE'.
    """
    logging.info("Updating previous race data and race count in runners beginning...")
    start_time = time.time()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            WITH base AS (
                SELECT
                    r2.course_cd AS curr_course_cd,
                    r2.race_date AS curr_race_date,
                    r2.race_number AS curr_race_number,
                    r2.saddle_cloth_number AS curr_saddle_cloth_number,
                    -- Fetch previous race details, but ensure they are from past races only
                    LAG(r2.todays_cls) OVER w AS previous_class,
                    LAG(r.distance_meters) OVER w AS previous_distance,
                    LAG(r.surface) OVER w AS previous_surface,
                    LAG(re.speed_rating) OVER w AS prev_speed_rating,
                    -- Total number of races (including future races)
                    COUNT(*) OVER (PARTITION BY h.horse_id) AS total_race_count
                FROM runners r2
                JOIN races r 
                    ON r2.course_cd = r.course_cd
                    AND r2.race_date = r.race_date
                    AND r2.race_number = r.race_number
                LEFT JOIN results_entries re  -- Use LEFT JOIN so future races don't break the query
                    ON r2.course_cd = re.course_cd
                    AND r2.race_date = re.race_date
                    AND r2.race_number = re.race_number
                    AND r2.saddle_cloth_number = re.program_num
                JOIN horse h
                    ON r2.axciskey = h.axciskey
                -- Ensure LAG() only pulls from past races
                -- Partition by horse_id, ordered by race date and race number
                WINDOW w AS (
                    PARTITION BY h.horse_id
                    ORDER BY r2.race_date ASC, r2.race_number ASC
                )
            )
            UPDATE runners r2
            SET
                previous_class       = COALESCE(base.previous_class, -1),   -- Default to -1 if no past race
                previous_distance    = COALESCE(base.previous_distance, -1),
                previous_surface     = COALESCE(base.previous_surface, 'NONE'),
                prev_speed_rating    = COALESCE(base.prev_speed_rating, -1),
                race_count           = base.total_race_count
            FROM base
            WHERE
                r2.course_cd = base.curr_course_cd
                AND r2.race_date = base.curr_race_date
                AND r2.race_number = base.curr_race_number
                AND r2.saddle_cloth_number = base.curr_saddle_cloth_number
    """)
        conn.commit()
        logging.info("Previous race data and race count updated successfully.")
    except Exception as e:
        logging.error(f"Error updating previous race data and race count: {e}")
        conn.rollback()
        raise
    finally:
        end_time = time.time()
        logging.info(f"Time taken to update previous race data and race count: {end_time - start_time} seconds")
        
def update_distance_meters(conn):
    """
    Updates the `distance_meters` column in the `racedata` table by converting the `distance` and `dist_unit` columns.
    """
    logging.info("Updating distance_meters in racedata beginning...")
    start_time = time.time()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            UPDATE races
            SET distance_meters = ROUND(
                CASE
                    WHEN dist_unit = 'F' THEN (distance/100) * 201.168
                    WHEN dist_unit = 'M' THEN distance
                    WHEN dist_unit = 'Y' THEN distance * 0.9144
                    ELSE NULL
                END
            )
            """)
            conn.commit()
            elapsed = time.time() - start_time
            print("Distance_meters updated successfully.")
            logging.info(f"Distance_meters updated successfully in {elapsed:.2f} seconds.")
    except Exception as e:
        logging.error(f"Error updating distance_meters: {e}")
        conn.rollback()
        raise

def update_rr_par_time(conn):
    """
    Updates the `rr_par_time` column in the `races` table by computing the average
    rr_par_time for each combination of course_cd, distance_meters, and trk_cond.
    
    The function performs the following steps:
      1. Selects only rows where rr_par_time is not null and not zero.
      2. For the courses of interest, groups the data by course_cd, distance_meters, 
         and trk_cond, and computes the average rr_par_time.
      3. Updates each row in the races table so that its rr_par_time is set to the computed
         average for its track, distance, and track condition.
    
    This function is designed to be run nightly so that the `rr_par_time` values remain current.
    
    Parameters:
      conn: A psycopg2 database connection.
    """
    logging.info("Updating rr_par_time in races beginning...")
    start_time = time.time()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            UPDATE races r
            SET rr_par_time = ROUND((sub.avg_rr_par_time)::numeric, 2)::numeric(10,2)
            FROM (
                SELECT course_cd, distance_meters, 
                    AVG(rr_par_time) AS avg_rr_par_time
                FROM races
                WHERE rr_par_time IS NOT NULL 
                AND rr_par_time <> 0
                AND course_cd IN (
                        'CNL','SAR','PIM','TSA','BEL','MVR','TWO','CLS','KEE',
                        'TAM','TTP','TKD','ELP','PEN','HOU','DMR','TLS','AQU',
                        'MTH','TGP','TGG','CBY','LRL','TED','IND','CTD','ASD',
                        'TCD','LAD','TOP'
                      )
                GROUP BY course_cd, distance_meters
            ) sub
            WHERE r.course_cd = sub.course_cd
            AND r.distance_meters = sub.distance_meters
            """)
            conn.commit()
            elapsed = time.time() - start_time
            logging.info(f"rr_par_time updated successfully in {elapsed:.2f} seconds.")
            print("rr_par_time updated successfully.")
    except Exception as e:
        logging.error(f"Error updating rr_par_time: {e}")
        conn.rollback()
        raise
    
def update_finish_time(conn):
    """
    Updates the `finish_time` column in the `results_entries` table by taking the sectionals running_time
    for each horse in every race where available in the sectionals data.
    
    The function performs the following steps:
      1. Selects only rows where rr_par_time is not null and not zero.
      2. For the courses of interest, groups the data by course_cd, distance_meters, 
         and trk_cond, and computes the average rr_par_time.
      3. Updates each row in the races table so that its rr_par_time is set to the computed
         average for its track, distance, and track condition.
    
    This function is designed to be run nightly so that the `rr_par_time` values remain current.
    
    Parameters:
      conn: A psycopg2 database connection.
    """
    logging.info("Updating finish_time in results_entries beginning...")
    start_time = time.time()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE results_entries re
                SET finish_time = s.running_time::text
                FROM (
                    /* Gather the MAX(running_time) for each horse in sectionals */
                    SELECT 
                        course_cd,
                        race_date,
                        race_number,
                        saddle_cloth_number,
                        MAX(running_time) AS running_time
                    FROM sectionals
                    GROUP BY 
                        course_cd,
                        race_date,
                        race_number,
                        saddle_cloth_number
                ) s
                WHERE 
                    re.course_cd = s.course_cd
                    AND re.race_date = s.race_date
                    AND re.race_number = s.race_number
                    AND re.program_num = s.saddle_cloth_number;
            """)
            conn.commit()
            elapsed = time.time() - start_time
            logging.info(f"finish_time updated successfully in {elapsed:.2f} seconds.")
            print("finish_time updated successfully.")
    except Exception as e:
        logging.error(f"Error updating finish_time: {e}")
        conn.rollback()
        raise
    
def calculate_gps_metrics_quartile_and_write(conn, gpspoint_df, runners_df, horse_df, jdbc_url, jdbc_properties):
    """
    Calculates GPS-derived metrics (acceleration, jerk, distance covered, etc.)
    for each horse in each race, broken down by quartiles, and writes the result
    to 'gps_aggregated' -- now including 'horse_id' for each row.

    Arguments:
    ----------
    conn : psycopg2 connection (unused in this snippet, can be optional)
    gpspoint_df : Spark DataFrame with columns
        [course_cd, race_date, race_number, saddle_cloth_number,
         time_stamp, speed, stride_frequency, progress, ...]
    runners_df : Spark DataFrame with columns:
        [course_cd, race_date, race_number, saddle_cloth_number, axciskey, ...]
        i.e. it does NOT have horse_id directly, but has axciskey
    horse_df : Spark DataFrame with columns:
        [axciskey, horse_id, horse_name, ...]
        so we can join on axciskey to finally get horse_id
    jdbc_url, jdbc_properties : connection info for writing out
    """
    import time
    import logging
    from pyspark.sql.functions import (
        col, lag, when, avg as F_avg, sum as F_sum, min as F_min,
        stddev_samp, first, last, ntile, lit, max as F_max
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
        if rc not in gpspoint_df.columns:
            raise ValueError(f"Missing required column: {rc}")

    # ----------------------------------------------------------------------
    # 2) Filter invalid rows
    # ----------------------------------------------------------------------
    filtered_df = gpspoint_df.filter(
        (col("stride_frequency").isNotNull()) &
        (col("progress") != 0)
    )

    # ----------------------------------------------------------------------
    # 3) Sort data within each (course_cd, date, race_no, saddle_cloth), compute time deltas
    # ----------------------------------------------------------------------
    gps_window = Window.partitionBy(
        "course_cd", "race_date", "race_number", "saddle_cloth_number"
    ).orderBy("time_stamp")

    df_sorted = filtered_df.withColumn(
        "prev_ts", lag(col("time_stamp").cast("long")).over(gps_window)
    ).withColumn(
        "delta_t",
        (col("time_stamp").cast("long") - col("prev_ts")).cast("double")
    )

    # ----------------------------------------------------------------------
    # 4) Calculate acceleration / jerk
    # ----------------------------------------------------------------------
    df_sorted = df_sorted.withColumn(
        "prev_speed",
        lag("speed").over(gps_window)
    )
    df_sorted = df_sorted.withColumn(
        "acceleration",
        when(
            (col("delta_t") > 0) & col("prev_speed").isNotNull(),
            (col("speed") - col("prev_speed")) / col("delta_t")
        )
    )
    df_sorted = df_sorted.withColumn(
        "prev_acc",
        lag("acceleration").over(gps_window)
    ).withColumn(
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
    # 7) Group (course_cd, date, race_no, cloth_no, quartile) => aggregate
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

    # ----------------------------------------------------------------------
    # 8) Pivot the quartiles => speed_q1, speed_q2, ...
    # ----------------------------------------------------------------------
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
        pivoted_quart = pivoted_quart.withColumnRenamed(f"{q}_avg_speed", f"speed_q{q}")
        pivoted_quart = pivoted_quart.withColumnRenamed(f"{q}_avg_accel", f"accel_q{q}")
        pivoted_quart = pivoted_quart.withColumnRenamed(f"{q}_avg_jerk", f"jerk_q{q}")
        pivoted_quart = pivoted_quart.withColumnRenamed(f"{q}_sum_dist", f"dist_q{q}")
        pivoted_quart = pivoted_quart.withColumnRenamed(f"{q}_avg_strfreq", f"strfreq_q{q}")

    # ----------------------------------------------------------------------
    # 9) Overall aggregator (whole race)
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
        F_avg(when(col("stride_frequency") > 0, col("speed") / col("stride_frequency"))).alias("avg_stride_length"),
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
    # 10) Combine pivoted quartiles + overall aggregator => final_result
    # ----------------------------------------------------------------------
    final_result = pivoted_quart.join(
        overall_agg2,
        on=["course_cd", "race_date", "race_number", "saddle_cloth_number"],
        how="left"
    )

    # ----------------------------------------------------------------------
    # 11) Join with `runners_df` to get `axciskey`
    # 
    #   We assume runners_df has at least:
    #   [course_cd, race_date, race_number, saddle_cloth_number, axciskey]
    # 
    final_with_axcis = final_result.join(
        runners_df.select(
            "course_cd","race_date","race_number","saddle_cloth_number","axciskey"
        ),
        on=["course_cd","race_date","race_number","saddle_cloth_number"],
        how="left"
    )

    # ----------------------------------------------------------------------
    # 12) Join that with `horse_df` to finally get horse_id
    # 
    #   We assume horse_df has [axciskey, horse_id, horse_name, etc.]
    #   If you only need horse_id, just select those two columns.
    # 
    final_with_horse = final_with_axcis.join(
        horse_df.select("axciskey","horse_id"),  # or add "horse_name" if needed
        on="axciskey",
        how="left"
    )

    # ----------------------------------------------------------------------
    # 13) Write final_with_horse => gps_aggregated
    # ----------------------------------------------------------------------
    staging_table = "gps_aggregated"
    logging.info(f"Writing quartile + overall GPS metrics (with horse_id) to {staging_table} ...")

    (
        final_with_horse.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", staging_table)
        .option("user", jdbc_properties["user"])
        .option("driver", jdbc_properties["driver"])
        .mode("overwrite")
        .save()
    )

    elapsed = time.time() - start_time
    logging.info(
        f"GPS quartile metrics aggregated (with axciskey => horse_id) "
        f"and written in {elapsed:.2f} seconds."
    )    
    
def append_gps_locf_columns(spark, jdbc_url, jdbc_properties):
    """
    Reads gps_aggregated from DB, appends '_prev' columns via LOCF 
    (Last Observation Carried Forward) across multiple races for each horse_id,
    and writes to gps_aggregated_locf.
    ...
    """
    # 1) Read the gps_aggregated table into a DataFrame
    df_agg = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "gps_aggregated")
        .option("user", jdbc_properties["user"])
        .option("driver", jdbc_properties["driver"])
        .load()
    )

    # 2) Now we truly have 'horse_id' in df_agg, so we can do:
    partition_cols = ["horse_id"]
    order_cols = ["race_date", "race_number"]  # or add 'course_cd' if needed

    window_spec = (
        Window.partitionBy(*partition_cols)
              .orderBy(*order_cols)
              .rowsBetween(Window.unboundedPreceding, -1)
    )

    # Exclude non-numeric or ID columns from the LOCF transformations
    skip_cols = set(partition_cols + order_cols + 
                    ["course_cd","race_date","race_number","saddle_cloth_number"])

    numeric_cols = [
        f.name for f in df_agg.schema.fields
        if f.dataType.typeName() in ("integer","double","float","long","decimal")
           and f.name not in skip_cols
    ]

    for c in numeric_cols:
        df_agg = df_agg.withColumn(
            f"{c}_prev",
            last(col(c), ignorenulls=True).over(window_spec)
        )

    staging_table = "gps_aggregated_locf"
    
    (
        df_agg.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", staging_table)
        .option("user", jdbc_properties["user"])
        .option("driver", jdbc_properties["driver"])
        .mode("overwrite")
        .save()
    )
    
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

    elapsed = time.time() - start_time
    logging.info(f"Spark-based sectionals aggregation and write completed in {elapsed:.2f} seconds.")

def add_pk_and_indexes(db_pool, output_table):
        try:
            if output_table == "sectionals_aggregated":    
                ddl_statements = [
                    f"ALTER TABLE {output_table} ADD PRIMARY KEY (course_cd, race_date, race_number, saddle_cloth_number)",
                ]
            elif output_table == "sectionals_aggregated_locf":
                ddl_statements =[
                    f"CREATE INDEX idx_sectionals_aggregated_locf ON public.sectionals_aggregated_locf USING btree (as_of_date)",
                    f"CREATE INDEX idx_sectionals_aggregated_locf_horse_id ON public.sectionals_aggregated_locf USING btree (horse_id)"
                ]
            elif output_table == "gps_aggregated_locf":
                ddl_statements = [
                    f"ALTER TABLE {output_table} ADD PRIMARY KEY (course_cd, race_date, race_number, saddle_cloth_number)",
                    f"CREATE INDEX idx_gps_aggregated_locf ON public.gps_aggregated_locf USING btree (race_date)",
                    f"CREATE INDEX gps_aggregated_locf_horse_id ON public.gps_aggregated_locf USING btree (horse_id)"
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

def spark_aggregate_sectionals_and_write_locf_keyed_horse_date(
    conn,
    df_sectionals,
    df_runners,
    df_horse,
    jdbc_url,
    jdbc_properties
):
    """
    1) Aggregates sectionals by (course_cd, race_date, race_number, saddle_cloth_number).
    2) Joins with runners (to get axciskey) and horse (to get horse_id).
    3) Renames race_date -> as_of_date, effectively keying each row by (horse_id, as_of_date).
    4) Partitions by horse_id, orders by as_of_date => LAG(...) to shift aggregator columns from previous date (LOCF).
    5) Writes final table with columns: (horse_id, as_of_date, aggregator columns, aggregator columns _prev, ...).
    """

    logging.info("Starting Spark-based aggregation for sectionals keyed by (horse_id, as_of_date) with LOCF ...")
    start_time = time.time()

    # ----------------------------------------------------------------------------
    # Step A: Aggregate raw sectionals as usual
    # ----------------------------------------------------------------------------

    # 1) Count gates
    gate_counts = (
        df_sectionals.groupBy("course_cd", "race_date", "race_number", "saddle_cloth_number")
                     .agg(count("gate_numeric").alias("num_gates"))
    )

    # 2) Join gate counts back
    df_with_counts = df_sectionals.join(
        gate_counts,
        on=["course_cd", "race_date", "race_number", "saddle_cloth_number"]
    )

    # 3) Ntile(4) across gates
    window_ntile = Window.partitionBy(
        "course_cd", "race_date", "race_number", "saddle_cloth_number"
    ).orderBy("gate_numeric")

    df_with_ntile = df_with_counts.withColumn(
        "quartile",
        ntile(4).over(window_ntile)
    )

    # 4) Sum distance/time for entire race
    total_distance_ran = (
        df_with_ntile
        .groupBy("course_cd","race_date","race_number","saddle_cloth_number")
        .agg(
            F_sum("distance_ran").alias("total_distance_ran"),
            F_sum("sectional_time").alias("running_time")
        )
    )

    # 5) Quartile aggregates
    quartile_aggregates = (
        df_with_ntile
        .groupBy("course_cd","race_date","race_number","saddle_cloth_number","quartile")
        .agg(
            F_avg("sectional_time").alias("avg_running_time"),
            last("distance_back").alias("distance_back"),
            F_sum("number_of_strides").alias("number_of_strides")
        )
    )

    # 6) Pivot quartile aggregates
    pivoted = (
        quartile_aggregates
        .groupBy("course_cd","race_date","race_number","saddle_cloth_number")
        .pivot("quartile")
        .agg(
            first("avg_running_time").alias("avg_time_per_gate"),
            first("distance_back").alias("distance_back"),
            first("number_of_strides").alias("number_of_strides")
        )
    )

    # 7) Rename pivoted columns
    result1 = (
        pivoted
        .withColumnRenamed("1_avg_time_per_gate", "avgtime_gate1")
        .withColumnRenamed("1_distance_back",     "dist_bk_gate1")
        .withColumnRenamed("1_number_of_strides", "numstrides_gate1")
        .withColumnRenamed("2_avg_time_per_gate", "avgtime_gate2")
        .withColumnRenamed("2_distance_back",     "dist_bk_gate2")
        .withColumnRenamed("2_number_of_strides", "numstrides_gate2")
        .withColumnRenamed("3_avg_time_per_gate", "avgtime_gate3")
        .withColumnRenamed("3_distance_back",     "dist_bk_gate3")
        .withColumnRenamed("3_number_of_strides", "numstrides_gate3")
        .withColumnRenamed("4_avg_time_per_gate", "avgtime_gate4")
        .withColumnRenamed("4_distance_back",     "dist_bk_gate4")
        .withColumnRenamed("4_number_of_strides", "numstrides_gate4")
    )

    # 8) Join total_distance_ran
    aggregated_sectionals = (
        result1.join(
            total_distance_ran,
            on=["course_cd","race_date","race_number","saddle_cloth_number"]
        )
    )

    # ----------------------------------------------------------------------------
    # Step B: Join to runners => get axciskey, then to horse => get horse_id
    # ----------------------------------------------------------------------------
    # runners has (course_cd, race_date, race_number, saddle_cloth_number, axciskey, ...)
    # horse   has (axciskey, horse_id, ...)
    sec_with_axciskey = aggregated_sectionals.alias("sec").join(
        df_runners.alias("run"),
        on=["course_cd","race_date","race_number","saddle_cloth_number"]
    ).select("sec.*","run.axciskey")
    
    sec_with_horse_id = sec_with_axciskey.alias("swx").join(
        df_horse.alias("h"),
        on=["axciskey"]
    ).select("swx.*","h.horse_id")
    # ----------------------------------------------------------------------------
    # Step C: Key on (horse_id, as_of_date) => rename race_date -> as_of_date
    # ----------------------------------------------------------------------------
    # We'll keep race_date for reference, but also define as_of_date = race_date
    final_keyed = sec_with_horse_id.withColumn("as_of_date", col("race_date"))

    # ----------------------------------------------------------------------------
    # Step D: LOCF SHIFT by (horse_id) ordering by as_of_date
    # ----------------------------------------------------------------------------
    w_horse = Window.partitionBy("horse_id").orderBy("as_of_date")

    # columns to shift
    shift_cols = [
        "avgtime_gate1","dist_bk_gate1","avgtime_gate2","dist_bk_gate2",
        "avgtime_gate3","dist_bk_gate3","avgtime_gate4","dist_bk_gate4",
        "total_distance_ran","running_time"
    ]

    shifted = final_keyed
    for c in shift_cols:
        shifted = shifted.withColumn(
            f"{c}_prev",
            lag(c, 1).over(w_horse)
        )

    # ----------------------------------------------------------------------------
    # Step E: Write final
    # ----------------------------------------------------------------------------
    # We now have columns:
    #   [horse_id, as_of_date, (optional) race_date, race_number, saddle_cloth_number, aggregator columns, aggregator columns _prev, ...]
    # keyed primarily by (horse_id, as_of_date).
    staging_table = "sectionals_aggregated_locf"

    logging.info(f"Writing aggregator keyed by (horse_id, as_of_date) with LOCF to {staging_table} (overwrite).")

    (
        shifted.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", staging_table)
        .option("user", jdbc_properties["user"])
        .option("driver", jdbc_properties["driver"])
        .mode("overwrite")
        .save()
    )

    elapsed = time.time() - start_time
    logging.info(f"Done. Wrote table={staging_table} in {elapsed:.2f} sec, keyed by (horse_id, as_of_date).")
                
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
    dataset_log_file = os.path.join(script_dir, f"../../logs/{datetime.now().strftime('%Y-%m-%d')}_dataset.log")
    setup_logging(script_dir, log_dir=dataset_log_file)
    # 1) Create DB pool
    db_pool = get_db_pool(config)
    
    # 2) Initialize Spark
    try:
        spark, jdbc_url, jdbc_properties, parquet_dir, _ = initialize_environment()
        
        setup_logging(script_dir, log_dir=dataset_log_file)

        # Load and write data to parquet
        queries = tpd_sql_queries()
        dfs = load_data_from_postgresql(spark, jdbc_url, jdbc_properties, queries, parquet_dir)
        # Suppose we have a dictionary of queries
        for name, df in dfs.items():
            logging.info(f"DataFrame '{name}' loaded. Schema:")
            df.printSchema()
            if name == "sectionals":
                sectionals_df = df
            elif name == "runners":
                runners_df = df
            elif name == "horse":
                horse_df = df
            elif name == "gpspoint":
                gpspoint_df = df
            else:
                logging.error(f"Unknown DataFrame name: {name}")
                continue

        conn = db_pool.getconn()
        try:
            spark_aggregate_sectionals_and_write(conn, sectionals_df, jdbc_url, jdbc_properties)
            spark_aggregate_sectionals_and_write_locf_keyed_horse_date(conn,sectionals_df,runners_df,horse_df,jdbc_url,jdbc_properties)
            add_pk_and_indexes(db_pool, "sectionals_aggregated_locf")
            add_pk_and_indexes(db_pool, "sectionals_aggregated")
            calculate_gps_metrics_quartile_and_write(conn, gpspoint_df, runners_df, horse_df, jdbc_url, jdbc_properties)
            append_gps_locf_columns(spark, jdbc_url, jdbc_properties)
            add_pk_and_indexes(db_pool, "gps_aggregated_locf")
        finally:
            db_pool.putconn(conn)
                
        logging.info("Ingestion job succeeded")
        spark.catalog.clearCache()

        # 4) net sentiment update                    
        conn = db_pool.getconn()
        try:
            update_net_sentiment(conn)
        except Exception as e:
            logging.error(f"Error updating net sentiment: {e}")
            conn.rollback()
        try:
            update_distance_meters(conn)
        except Exception as e:
            logging.error(f"Error updating distance_meters: {e}")
        try:
            update_rr_par_time(conn)
        except Exception as e:
            logging.error(f"Error updating rr_par_time: {e}")
        try:
            update_previous_race_data_and_race_count(conn)
        except Exception as e:
            logging.error(f"Error updating update_previous_race_data_and_race_count: {e}")
        try:
            update_runners_off_fin_last_race(conn)
        except Exception as e:
            logging.error(f"Error updating update_runners_off_fin_last_race: {e}")
        try:    
            update_speed_rating(conn)
        except Exception as e:
            logging.error(f"Error updating speed_rating: {e}")
        try:
            update_runners_prev_speed_rating(conn)
        except Exception as e:
            logging.error(f"Error updating runners_prev_speed_rating: {e}")    
        try:
            update_previous_surface(conn)
        except Exception as e:
            logging.error(f"Error updating previous_surface: {e}")
        try:
            update_finish_time(conn)
        except Exception as e:
            logging.error(f"Error updating update_finish_time: {e}")
    except Exception as e:
        logging.error(f"Error during Spark initialization: {e}")
        sys.exit(1)
    
        logging.info("All tasks completed. Spark session stopped and DB pool closed.")
    finally:
        if spark:
            spark.stop()
        if db_pool:
            db_pool.closeall()
            
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
