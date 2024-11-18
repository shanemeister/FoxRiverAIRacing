#!/usr/bin/env python3

import logging
import os
import sys
from datetime import datetime
import configparser
from psycopg2 import sql
from src.data_ingestion.ingestion_utils import get_db_connection
import traceback
import re
import psycopg2
from psycopg2 import sql

def setup_logging(log_file_path):
    """
    Configures logging to write logs to the specified file with timestamp and log level.
    """
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info("Logging has been set up successfully.")

def read_config(config_file_path):
    """
    Reads the configuration file and returns the configuration object.
    """
    config = configparser.ConfigParser()
    if not config.read(config_file_path):
        logging.error(f"Configuration file '{config_file_path}' not found or is empty.")
        sys.exit(1)
    
    if 'database' not in config:
        logging.error("The 'database' section is missing in the configuration file.")
        sys.exit(1)
    
    return config


def update_stat_type(conn):
    """
    Updates the stat_type field in the runners table based on predefined mapping conditions.
    
    Parameters:
    - conn: psycopg2 database connection object.
    
    The function performs the following steps:
    1. Iterates through the stat_type_mapping list.
    2. For each stat_type, constructs a dynamic WHERE clause based on specified conditions.
    3. Executes an UPDATE query to assign the stat_type to records matching the conditions.
    4. Assigns a default stat_type ('OTHER') to any remaining records with NULL stat_type.
    5. Logs the number of records updated for each stat_type.
    """
    
    # Define the mapping logic for stat types
    stat_type_mapping = [
        {"stat_type": "TURF_SPRNT", "conditions": {"previous_surface": "Turf", "morn_odds": "<8"}},
        {"stat_type": "DIRT_SPRNT", "conditions": {"previous_surface": "Dirt", "morn_odds": "<8"}},
        {"stat_type": "TURF_RTE", "conditions": {"previous_surface": "Turf", "morn_odds": ">=8"}},
        {"stat_type": "DIRT_RTE", "conditions": {"previous_surface": "Dirt", "morn_odds": ">=8"}},
        {"stat_type": "ALL_WEATHR", "conditions": {"previous_surface": "All Weather"}},
        {"stat_type": "ODDSGT5", "conditions": {"morn_odds": ">5"}},
        {"stat_type": "ODDSLE5", "conditions": {"morn_odds": "<=5"}},
        {"stat_type": "FAVORITE", "conditions": {"ae_flag": True}},
        # Default category to assign to remaining records
        {"stat_type": "OTHER", "conditions": {}}  # Assign to all remaining records
    ]
    # print(stat_type_mapping)
    try:
        with conn.cursor() as cur:
            for mapping in stat_type_mapping:
                stat_type = mapping["stat_type"]
                conditions = mapping["conditions"]
                where_clauses = []
                params = {"stat_type": stat_type}

                # Dynamically build the WHERE clause based on conditions
                for key, value in conditions.items():
                    print(key, value)
                    if isinstance(value, str):
                        print(f"Value1: {value}")
                        # Check for operators in the value (e.g., '>=8', '<5')
                        operator_match = re.match(r'(>=|<=|>|<)\s*(\d+)', value)
                        if operator_match:
                            operator, val = operator_match.groups()
                            where_clauses.append(sql.SQL("{} {} %({})s").format(
                                sql.Identifier(key),
                                sql.SQL(operator),
                                sql.Identifier(key)
                            ))
                            params[key] = val
                        else:
                            # Exact match condition
                            where_clauses.append(sql.SQL("{} = %({})s").format(
                                sql.Identifier(key),
                                sql.Identifier(key)
                            ))
                            params[key] = value
                    elif isinstance(value, bool):
                        print(f"Value2: {value}")
                        # Boolean condition
                        where_clauses.append(sql.SQL("{} = %({})s").format(
                            sql.Identifier(key),
                            sql.Identifier(key)
                        ))
                        params[key] = value
                    else:
                        # Handle other types if necessary
                        where_clauses.append(sql.SQL("{} = %({})s").format(
                            sql.Identifier(key),
                            sql.Identifier(key)
                        ))
                        params[key] = value

                if where_clauses:
                    where_clause = sql.SQL(" AND ").join(where_clauses)
                else:
                    # For the default category ('OTHER'), target records where stat_type is NULL
                    where_clause = sql.SQL("stat_type IS NULL")

                # Construct the UPDATE query
                update_query = sql.SQL("""
                    UPDATE runners
                    SET stat_type = %(stat_type)s
                    WHERE {where_clause};
                """).format(where_clause=where_clause)

                # Execute the UPDATE query with parameters
                cur.execute(update_query, params)

                # Log the number of records updated
                logging.info(f"Updated {cur.rowcount} rows for stat_type '{stat_type}'.")

        # Commit the transaction after all updates
        conn.commit()
        logging.info("runners.stat_type field updated successfully.")

    except Exception as e:
        # Rollback in case of error
        conn.rollback()
        logging.error("Error updating stat_type:")
        logging.error(e)
        logging.error(traceback.format_exc())
        raise
    
def update_net_sentiment(conn):
    """
    Updates the net_sentiment column in the runners table based on horse_comm comments.
    """
    try:
        with conn.cursor() as cur:
            # Update net_sentiment based on positive and negative comments
            update_sentiment_query = """
                UPDATE runners r
                SET net_sentiment = (
                    SELECT
                        COUNT(*) FILTER (WHERE c LIKE '[+]%') - COUNT(*) FILTER (WHERE c LIKE '[-]%')
                    FROM
                        regexp_split_to_table(r.horse_comm, E'\n') AS c
                )
                WHERE
                    r.horse_comm IS NOT NULL;
            """
            cur.execute(update_sentiment_query)
            updated_rows = cur.rowcount
            logging.info(f"Updated net_sentiment for {updated_rows} runners based on horse_comm.")
            
            # Set net_sentiment to 0 where it is NULL
            set_default_query = """
                UPDATE runners
                SET net_sentiment = 0
                WHERE net_sentiment IS NULL;
            """
            cur.execute(set_default_query)
            default_rows = cur.rowcount
            logging.info(f"Set net_sentiment to 0 for {default_rows} runners with no comments.")
        
        # Commit the transaction
        conn.commit()
        logging.info("net_sentiment update transaction committed successfully.")
    
    except Exception as e:
        # Rollback in case of error
        conn.rollback()
        logging.error(f"Error updating net_sentiment: {e}")
        raise

def update_sectionals_aggregated(conn):
    """
    Updates the sectionals_aggregated table with new sectional data.
    Assumes that new sectional data has been loaded into the source sectionals table.
    """
    try:
        with conn.cursor() as cur:
            # Example SQL to refresh sectionals_aggregated
            # This may vary based on your exact aggregation logic
            refresh_sectionals_aggregated_query = """
                TRUNCATE TABLE sectionals_aggregated;
                
                INSERT INTO sectionals_aggregated (course_cd, race_date, race_number, saddle_cloth_number, early_pace_time, late_pace_time, pace_differential, total_race_time, total_strides, avg_stride_length)
                SELECT
                    s.course_cd,
                    s.race_date,
                    s.race_number,
                    s.saddle_cloth_number,
                    SUM(CASE WHEN cs.pace_category = 'early' THEN cs.sectional_time ELSE 0 END) AS early_pace_time,
                    SUM(CASE WHEN cs.pace_category = 'late' THEN cs.sectional_time ELSE 0 END) AS late_pace_time,
                    SUM(CASE WHEN cs.pace_category = 'early' THEN cs.sectional_time ELSE 0 END) - 
                    SUM(CASE WHEN cs.pace_category = 'late' THEN cs.sectional_time ELSE 0 END) AS pace_differential,
                    SUM(cs.sectional_time) AS total_race_time,
                    SUM(cs.number_of_strides) AS total_strides,
                    SUM(cs.distance_ran) / NULLIF(SUM(cs.number_of_strides), 0) AS avg_stride_length
                FROM
                    sectionals s
                JOIN
                    sectionals_historical cs
                    ON s.course_cd = cs.course_cd
                    AND s.race_date = cs.race_date
                    AND s.race_number = cs.race_number
                    AND s.saddle_cloth_number = cs.saddle_cloth_number
                GROUP BY
                    s.course_cd,
                    s.race_date,
                    s.race_number,
                    s.saddle_cloth_number;
            """
            cur.execute(refresh_sectionals_aggregated_query)
            logging.info("sectionals_aggregated table refreshed successfully.")
        
        # Commit the transaction
        conn.commit()
        logging.info("sectionals_aggregated update transaction committed successfully.")
    
    except Exception as e:
        # Rollback in case of error
        conn.rollback()
        logging.error(f"Error updating sectionals_aggregated: {e}")
        raise

def update_gps_aggregated_results(conn):
    """
    Updates the gps_aggregated_results table with new GPS data.
    Assumes that new GPS data has been loaded into the source gps_data table.
    """
    try:
        with conn.cursor() as cur:
            # Example SQL to refresh gps_aggregated_results
            # Modify according to your actual aggregation logic
            refresh_gps_aggregated_query = """
                TRUNCATE TABLE gps_aggregated_results;
                
                INSERT INTO gps_aggregated_results (course_cd, race_date, race_number, saddle_cloth_number, avg_speed, max_speed, min_speed, avg_acceleration, max_acceleration, avg_stride_freq, max_stride_freq)
                SELECT
                    g.course_cd,
                    g.race_date,
                    g.race_number,
                    g.saddle_cloth_number,
                    AVG(g.speed) AS avg_speed,
                    MAX(g.speed) AS max_speed,
                    MIN(g.speed) AS min_speed,
                    AVG(g.acceleration) AS avg_acceleration,
                    MAX(g.acceleration) AS max_acceleration,
                    AVG(g.stride_freq) AS avg_stride_freq,
                    MAX(g.stride_freq) AS max_stride_freq
                FROM
                    gps_data g
                GROUP BY
                    g.course_cd,
                    g.race_date,
                    g.race_number,
                    g.saddle_cloth_number;
            """
            cur.execute(refresh_gps_aggregated_query)
            logging.info("gps_aggregated_results table refreshed successfully.")
        
        # Commit the transaction
        conn.commit()
        logging.info("gps_aggregated_results update transaction committed successfully.")
    
    except Exception as e:
        # Rollback in case of error
        conn.rollback()
        logging.error(f"Error updating gps_aggregated_results: {e}")
        raise

def update_tpd_features(conn):
    """
    Updates the tpd_features table by joining gps_aggregated_results and sectionals_aggregated.
    Assumes that both aggregated tables are already up-to-date.
    """
    try:
        with conn.cursor() as cur:
            # Example SQL to refresh tpd_features
            refresh_tpd_features_query = """
                TRUNCATE TABLE tpd_features;
                
                INSERT INTO tpd_features (course_cd, race_date, race_number, saddle_cloth_number, avg_speed, max_speed, min_speed, avg_acceleration, max_acceleration, avg_stride_freq, max_stride_freq, early_pace_time, late_pace_time, pace_differential, total_race_time, total_strides, avg_stride_length)
                SELECT
                    g.course_cd,
                    g.race_date,
                    g.race_number,
                    g.saddle_cloth_number,
                    g.avg_speed,
                    g.max_speed,
                    g.min_speed,
                    g.avg_acceleration,
                    g.max_acceleration,
                    g.avg_stride_freq,
                    g.max_stride_freq,
                    s.early_pace_time,
                    s.late_pace_time,
                    s.pace_differential,
                    s.total_race_time,
                    s.total_strides,
                    s.avg_stride_length
                FROM
                    gps_aggregated_results g
                JOIN
                    sectionals_aggregated s
                    ON g.course_cd = s.course_cd
                    AND g.race_date = s.race_date
                    AND g.race_number = s.race_number
                    AND g.saddle_cloth_number = s.saddle_cloth_number;
            """
            cur.execute(refresh_tpd_features_query)
            logging.info("tpd_features table refreshed successfully.")
        
        # Commit the transaction
        conn.commit()
        logging.info("tpd_features update transaction committed successfully.")
    
    except Exception as e:
        # Rollback in case of error
        conn.rollback()
        logging.error(f"Error updating tpd_features: {e}")
        raise

def main():
    """
    Main function to execute all data metric updates.
    """
    # Determine script directory and root directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.abspath(os.path.join(script_dir, '..', '..'))  # Assuming src/data_preprocessing
    
    # Define paths
    config_file_path = os.path.join(root_dir, 'config.ini')
    log_file_path = os.path.join(root_dir, 'logs', 'data_metrics_updates.log')
    
    # Ensure the log directory exists
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    
    # Setup logging
    setup_logging(log_file_path)
    logging.info("Starting data_metrics_updates process.")
    
    # Read configuration
    config = read_config(config_file_path)
    
    # Establish database connection
    try:
        conn = get_db_connection(config)
        logging.info("Database connection established successfully.")
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        sys.exit(1)
    
    # Perform the update operations
    try:
        update_stat_type(conn)
        update_net_sentiment(conn)
        update_sectionals_aggregated(conn)
        update_gps_aggregated_results(conn)
        update_tpd_features(conn)
        logging.info("All data metric updates completed successfully.")
    except Exception as e:
        logging.error(f"Data metrics update process failed: {e}")
    finally:
        # Close the database connection
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()