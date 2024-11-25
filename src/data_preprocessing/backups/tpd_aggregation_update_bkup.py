import logging
import os
import sys
import traceback
import psycopg2
from psycopg2 import sql, pool, DatabaseError
from datetime import date
from src.data_ingestion.ingestion_utils import get_db_connection, update_ingestion_status
import configparser
import time

def setup_logging(script_dir, log_dir=None):
    """Sets up logging configuration to write logs to a file and the console."""
    try:
        # Use the absolute path for the logs directory, configurable via config.ini
        if not log_dir:
            log_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs'
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'tpd_aggregation_update.log')

        # Create a logger
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Define a common format
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        logger.info("Logging has been set up successfully.")
    except Exception as e:
        print(f"Failed to set up logging: {e}", file=sys.stderr)
        sys.exit(1)

def read_config(script_dir, config_relative_path='../../config.ini'):
    """Reads the configuration file and returns the configuration object."""
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
    """Creates a connection pool."""
    try:
        db_pool_args = {
            'user': config['database']['user'],
            'host': config['database']['host'],
            'port': config['database']['port'],
            'database': config['database']['dbname']
        }
        
        # Attempt to get 'password' from config, default to None if not present
        password = config['database'].get('password')
        if password:
            db_pool_args['password'] = password
            logging.info("Password found in configuration. Using provided password for authentication.")
        else:
            logging.info("No password found in configuration. Attempting to use .pgpass for authentication.")

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

def update_gps_aggregated_results(conn):
    """Updates the gps_aggregated_results table."""
    logging.info("Updating gps_aggregated_results beginning...")
    start_time = time.time()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                WITH raw_data AS (
                    SELECT
                        gp.course_cd,
                        gp.race_date,
                        gp.race_number,
                        gp.saddle_cloth_number,
                        gp.speed,
                        gp.stride_frequency,
                        gp.time_stamp,
                        LAG(gp.speed) OVER w AS prev_speed,
                        LAG(gp.time_stamp) OVER w AS prev_time,
                        (gp.speed - LAG(gp.speed) OVER w) / NULLIF(EXTRACT(EPOCH FROM (gp.time_stamp - LAG(gp.time_stamp) OVER w)), 0) AS acceleration
                    FROM gpspoint gp
                    WINDOW w AS (
                        PARTITION BY gp.course_cd, gp.race_date, gp.race_number, gp.saddle_cloth_number
                        ORDER BY gp.time_stamp
                    )
                ),
                calculated_data AS (
                    SELECT
                        course_cd,
                        race_date,
                        race_number,
                        saddle_cloth_number,
                        speed,
                        stride_frequency,
                        acceleration
                    FROM raw_data
                ),
                aggregated_data AS (
                    SELECT
                        course_cd,
                        race_date,
                        race_number,
                        saddle_cloth_number,
                        AVG(speed) AS avg_speed,
                        MAX(speed) AS max_speed,
                        MIN(speed) AS min_speed,
                        AVG(acceleration) AS avg_acceleration,
                        MAX(acceleration) AS max_acceleration,
                        AVG(stride_frequency) AS avg_stride_freq,
                        MAX(stride_frequency) AS max_stride_freq,
                        STDDEV(speed) AS speed_stddev  -- Calculated here
                    FROM calculated_data
                    GROUP BY course_cd, race_date, race_number, saddle_cloth_number
                )
                INSERT INTO gps_aggregated_results (
                    course_cd, race_date, race_number, saddle_cloth_number,
                    avg_speed, max_speed, min_speed, avg_acceleration,
                    max_acceleration, avg_stride_freq, max_stride_freq,
                    speed_stddev
                )
                SELECT
                    course_cd, race_date, race_number, saddle_cloth_number,
                    avg_speed, max_speed, min_speed, avg_acceleration,
                    max_acceleration, avg_stride_freq, max_stride_freq,
                    speed_stddev
                FROM aggregated_data
                ON CONFLICT (course_cd, race_date, race_number, saddle_cloth_number)
                DO UPDATE SET
                    avg_speed = EXCLUDED.avg_speed,
                    max_speed = EXCLUDED.max_speed,
                    min_speed = EXCLUDED.min_speed,
                    avg_acceleration = EXCLUDED.avg_acceleration,
                    max_acceleration = EXCLUDED.max_acceleration,
                    avg_stride_freq = EXCLUDED.avg_stride_freq,
                    max_stride_freq = EXCLUDED.max_stride_freq,
                    speed_stddev = EXCLUDED.speed_stddev;
            """)
            conn.commit()
            elapsed = time.time() - start_time
            logging.info(f"gps_aggregated_results updated successfully in {elapsed:.2f} seconds.")
    except Exception as e:
        logging.error(f"Error updating gps_aggregated_results: {e}")
        conn.rollback()
        raise

def update_gps_aggregated_enhanced(conn):
    """Optimized function to update gps_aggregated_results with enhanced metrics."""
    logging.info("Updating gps_aggregated_enhanced beginning...")
    start_time = time.time()
    try:
        with conn.cursor() as cursor:
            # Step 1: Create Temporary Tables
            cursor.execute("""
                CREATE TEMP TABLE temp_max_accel_time (
                    course_cd bpchar(3),
                    race_date date,
                    race_number integer,
                    saddle_cloth_number bpchar(3),
                    max_acceleration_time timestamp
                ) ON COMMIT DROP;
            """)
            cursor.execute("""
                CREATE TEMP TABLE temp_max_stride_freq_time (
                    course_cd bpchar(3),
                    race_date date,
                    race_number integer,
                    saddle_cloth_number bpchar(3),
                    max_stride_freq_time timestamp
                ) ON COMMIT DROP;
            """)
            conn.commit()
            logging.info("Temporary tables created.")
            
            # Step 2: Populate temp_max_accel_time using ROW_NUMBER()
            cursor.execute("""
                INSERT INTO temp_max_accel_time (course_cd, race_date, race_number, saddle_cloth_number, max_acceleration_time)
                SELECT
                    course_cd,
                    race_date,
                    race_number,
                    saddle_cloth_number,
                    time_stamp AS max_acceleration_time
                FROM (
                    SELECT
                        course_cd,
                        race_date,
                        race_number,
                        saddle_cloth_number,
                        time_stamp,
                        acceleration,
                        ROW_NUMBER() OVER (
                            PARTITION BY course_cd, race_date, race_number, saddle_cloth_number
                            ORDER BY acceleration DESC, time_stamp DESC
                        ) AS rn
                    FROM raw_data
                ) sub
                WHERE rn = 1;
            """)
            conn.commit()
            logging.info("temp_max_accel_time populated.")
            
            # Step 3: Populate temp_max_stride_freq_time using ROW_NUMBER()
            cursor.execute("""
                INSERT INTO temp_max_stride_freq_time (course_cd, race_date, race_number, saddle_cloth_number, max_stride_freq_time)
                SELECT
                    course_cd,
                    race_date,
                    race_number,
                    saddle_cloth_number,
                    time_stamp AS max_stride_freq_time
                FROM (
                    SELECT
                        course_cd,
                        race_date,
                        race_number,
                        saddle_cloth_number,
                        time_stamp,
                        stride_frequency,
                        ROW_NUMBER() OVER (
                            PARTITION BY course_cd, race_date, race_number, saddle_cloth_number
                            ORDER BY stride_frequency DESC, time_stamp DESC
                        ) AS rn
                    FROM gpspoint
                ) sub
                WHERE rn = 1;
            """)
            conn.commit()
            logging.info("temp_max_stride_freq_time populated.")
            
            # Step 4: Update gps_aggregated_results using JOIN
            cursor.execute("""
                UPDATE gps_aggregated_results gar
                SET
                    max_acceleration_time = tma.max_acceleration_time,
                    max_stride_freq_time = tmsf.max_stride_freq_time
                FROM
                    temp_max_accel_time tma
                    JOIN temp_max_stride_freq_time tmsf
                        ON tma.course_cd = tmsf.course_cd
                        AND tma.race_date = tmsf.race_date
                        AND tma.race_number = tmsf.race_number
                        AND tma.saddle_cloth_number = tmsf.saddle_cloth_number
                WHERE
                    gar.course_cd = tma.course_cd
                    AND gar.race_date = tma.race_date
                    AND gar.race_number = tma.race_number
                    AND gar.saddle_cloth_number = tma.saddle_cloth_number;
            """)
            conn.commit()
            logging.info("gps_aggregated_results updated successfully.")
    except Exception as e:
        logging.error(f"Error updating gps_aggregated_enhanced: {e}")
        conn.rollback()
        raise
    elapsed_total = time.time() - start_time
    logging.info(f"gps_aggregated_results updated successfully in {elapsed_total:.2f} seconds.")

def update_sectionals_aggregated(conn, batch_size=1000):
    """Updates the sectionals_aggregated table in batches based on unique group keys."""
    logging.info("Updating sectionals_aggregated beginning...")
    start_time = time.time()
    try:
        with conn.cursor() as cursor:
            # Retrieve distinct group keys including gate_name
            cursor.execute("""
                SELECT DISTINCT course_cd, race_date, race_number, saddle_cloth_number, gate_name
                FROM sectionals
                ORDER BY course_cd, race_date, race_number, saddle_cloth_number, gate_name;
            """)
            groups = cursor.fetchall()
            total_groups = len(groups)
            logging.info(f"Total groups to process: {total_groups}")

            for i in range(0, total_groups, batch_size):
                batch = groups[i:i + batch_size]
                logging.info(f"Processing batch {i // batch_size + 1}: Groups {i + 1} to {i + len(batch)}")
                
                # Construct WHERE clause for current batch
                conditions = []
                params = []
                for group in batch:
                    conditions.append("(s.course_cd = %s AND s.race_date = %s AND s.race_number = %s AND s.saddle_cloth_number = %s AND s.gate_name = %s)")
                    params.extend(group)
                where_clause = " OR ".join(conditions)
                
                batch_sql = sql.SQL("""
                    INSERT INTO sectionals_aggregated (
                        course_cd, race_date, race_number, saddle_cloth_number,
                        early_pace_time, late_pace_time, total_race_time,
                        pace_differential, total_strides, avg_stride_length,
                        gate_name
                    )
                    SELECT
                        s.course_cd,
                        s.race_date,
                        s.race_number,
                        s.saddle_cloth_number,
                        MIN(CASE WHEN s.gate_name = 'early' THEN s.running_time END) AS early_pace_time,
                        MIN(CASE WHEN s.gate_name = 'late' THEN s.running_time END) AS late_pace_time,
                        MAX(s.running_time) - MIN(s.running_time) AS total_race_time,
                        MAX(s.running_time) - MIN(CASE WHEN s.gate_name = 'early' THEN s.running_time END) AS pace_differential,
                        SUM(s.number_of_strides) AS total_strides,
                        AVG(s.distance_ran / NULLIF(s.number_of_strides, 0)) AS avg_stride_length,
                        s.gate_name  -- Use the actual gate_name from data
                    FROM sectionals s
                    WHERE {where_clause}
                    GROUP BY s.course_cd, s.race_date, s.race_number, s.saddle_cloth_number, s.gate_name
                    ON CONFLICT (course_cd, race_date, race_number, saddle_cloth_number, gate_name)
                    DO UPDATE SET
                        early_pace_time = EXCLUDED.early_pace_time,
                        late_pace_time = EXCLUDED.late_pace_time,
                        total_race_time = EXCLUDED.total_race_time,
                        pace_differential = EXCLUDED.pace_differential,
                        total_strides = EXCLUDED.total_strides,
                        avg_stride_length = EXCLUDED.avg_stride_length;
                """).format(where_clause=sql.SQL(where_clause))
                
                cursor.execute(batch_sql, params)
                conn.commit()
                logging.info(f"Batch {i // batch_size + 1} processed successfully.")
    except Exception as e:
        logging.error(f"Error updating sectionals_aggregated: {e}")
        conn.rollback()
        raise
    elapsed_total = time.time() - start_time
    logging.info(f"sectionals_aggregated updated successfully in {elapsed_total:.2f} seconds.")

def update_tpd_features(conn, batch_size=1000):
    """Updates the tpd_features table in batches."""
    logging.info("Updating tpd_features beginning...")
    start_time = time.time()
    try:
        with conn.cursor() as cursor:
            # Fetch distinct group keys from gps_aggregated_results
            cursor.execute("""
                SELECT course_cd, race_date, race_number, saddle_cloth_number
                FROM gps_aggregated_results
                ORDER BY course_cd, race_date, race_number, saddle_cloth_number;
            """)
            groups = cursor.fetchall()
            total_groups = len(groups)
            logging.info(f"Total groups to process: {total_groups}")

            for i in range(0, total_groups, batch_size):
                batch = groups[i:i + batch_size]
                logging.info(f"Processing batch {i // batch_size + 1}: Groups {i + 1} to {i + len(batch)}")
                
                # Construct WHERE clause for current batch
                conditions = []
                params = []
                for group in batch:
                    conditions.append("(g.course_cd = %s AND g.race_date = %s AND g.race_number = %s AND g.saddle_cloth_number = %s)")
                    params.extend(group)
                where_clause = " OR ".join(conditions)
                
                batch_sql = sql.SQL("""
                    INSERT INTO tpd_features (
                        course_cd, race_date, race_number, saddle_cloth_number,
                        avg_speed, max_speed, min_speed, avg_acceleration,
                        max_acceleration, avg_stride_freq, max_stride_freq,
                        early_pace_time, late_pace_time, pace_differential,
                        total_race_time, total_strides, avg_stride_length
                    )
                    SELECT
                        g.course_cd, g.race_date, g.race_number, g.saddle_cloth_number,
                        g.avg_speed, g.max_speed, g.min_speed, g.avg_acceleration,
                        g.max_acceleration, g.avg_stride_freq, g.max_stride_freq,
                        s.early_pace_time, s.late_pace_time, s.pace_differential,
                        s.total_race_time, s.total_strides, s.avg_stride_length
                    FROM gps_aggregated_results g
                    LEFT JOIN sectionals_aggregated s
                    ON g.course_cd = s.course_cd
                    AND g.race_date = s.race_date
                    AND g.race_number = s.race_number
                    AND g.saddle_cloth_number = s.saddle_cloth_number
                    AND g.gate_name = s.gate_name  -- Ensure matching gate_name
                    WHERE {where_clause}
                    ON CONFLICT (course_cd, race_date, race_number, saddle_cloth_number, gate_name)
                    DO UPDATE SET
                        avg_speed = EXCLUDED.avg_speed,
                        max_speed = EXCLUDED.max_speed,
                        min_speed = EXCLUDED.min_speed,
                        avg_acceleration = EXCLUDED.avg_acceleration,
                        max_acceleration = EXCLUDED.max_acceleration,
                        avg_stride_freq = EXCLUDED.avg_stride_freq,
                        max_stride_freq = EXCLUDED.max_stride_freq,
                        early_pace_time = EXCLUDED.early_pace_time,
                        late_pace_time = EXCLUDED.late_pace_time,
                        pace_differential = EXCLUDED.pace_differential,
                        total_race_time = EXCLUDED.total_race_time,
                        total_strides = EXCLUDED.total_strides,
                        avg_stride_length = EXCLUDED.avg_stride_length;
                """).format(where_clause=sql.SQL(where_clause))
                
                cursor.execute(batch_sql, params)
                conn.commit()
                logging.info(f"Batch {i // batch_size + 1} processed successfully.")
    except Exception as e:
        logging.error(f"Error updating tpd_features: {e}")
        conn.rollback()
        raise
    elapsed_total = time.time() - start_time
    logging.info(f"tpd_features updated successfully in {elapsed_total:.2f} seconds.")

def update_net_sentiment(conn):
    """Updates net sentiment for horses based on runner comments."""
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
                        COUNT(CASE WHEN r1.horse_comm LIKE '%[+%' THEN 1 END) -
                        COUNT(CASE WHEN r1.horse_comm LIKE '%[-%' THEN 1 END) AS sentiment_diff
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

def main():
    """Main function to execute all updates."""
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    setup_logging(script_dir)
    config = read_config(script_dir)
    db_pool = get_db_pool(config)

    try:
        conn = db_pool.getconn()
        try:
            # List of update functions to execute
            update_functions = [
                update_gps_aggregated_results,
                update_gps_aggregated_enhanced,
                #update_sectionals_aggregated,
                #update_tpd_features,
                #update_net_sentiment
            ]
            
            for func in update_functions:
                try:
                    func(conn)
                except Exception as e:
                    logging.error(f"Error in {func.__name__}: {e}")
                    traceback.print_exc()
                    # Continue with the next function despite the error
                    continue
            logging.info("All updates completed with some errors.")
        finally:
            db_pool.putconn(conn)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        traceback.print_exc()
    finally:
        if db_pool:
            db_pool.closeall()

if __name__ == "__main__":
    main()