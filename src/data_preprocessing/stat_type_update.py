import os
import sys
import logging
import traceback
import datetime
import decimal
import configparser
from collections import defaultdict

import psycopg2
from psycopg2 import sql

from src.data_ingestion.ingestion_utils import (
    get_db_connection,
    update_ingestion_status,
    update_stat_type_last_processed,
    initialize_stat_types,
    get_unprocessed_stat_types,
    ensure_stat_type_exists,
    get_last_processed_date
)


def setup_logging(script_dir, log_dir=None):
    """Sets up logging configuration to write logs to a file and the console."""
    try:
        # Default log directory
        if not log_dir:
            log_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs'
        
        # Ensure the log directory exists
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'ingestion.log')

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

def read_config(script_dir):
    """Reads the configuration file and returns the configuration object."""
    try:
        config = configparser.ConfigParser()
        root_dir = os.path.abspath(os.path.join(script_dir, '../../'))
        config_file_path = os.path.join(root_dir, 'config.ini')
        if not os.path.exists(config_file_path):
            raise FileNotFoundError(f"Configuration file '{config_file_path}' does not exist.")
        config.read(config_file_path)
        if 'database' not in config:
            raise KeyError("The 'database' section is missing in the configuration file.")
        return config
    except Exception as e:
        logging.error(f"Error reading configuration file: {e}")
        sys.exit(1)


def process_stat_type(conn, cursor, stat_type, surface=None, max_distance=None, min_distance=None, filter_condition=None):
    """
    Processes a specific stat type and updates the horse_accum_stats table.
    Returns the last race_date processed.
    """
    last_race_date_processed = None
    try:
        # Ensure the stat_type exists in stat_type_code
        ensure_stat_type_exists(conn, stat_type)
        #logging.debug(f"Ensured stat_type {stat_type} exists in stat_type_code.")

        # Get the last_processed date
        last_processed_date = get_last_processed_date(conn, stat_type)
        logging.info(f"Last processed date for {stat_type}: {last_processed_date}")

        # Construct the query conditionally based on last_processed_date
        if last_processed_date:
            start_date_condition = "AND rr.race_date > %s"
            params = [stat_type, surface, last_processed_date]
            #logging.debug(f"Fetching race_dates after {last_processed_date}.")
        else:
            start_date_condition = ""
            params = [stat_type, surface]
            #logging.debug("Fetching all race_dates.")

        # Fetch race dates that haven't been processed for this stat_type
        query = f"""
            SELECT DISTINCT rr.race_date
            FROM race_results rr
            LEFT JOIN ingestion_files if2 
                ON rr.race_date::text = if2.file_name
                AND if2.message = %s
                AND if2.status = 'processed'
            WHERE rr.surface = %s
            {start_date_condition}
            AND if2.file_name IS NULL
            ORDER BY rr.race_date ASC
        """
        cursor.execute(query, tuple(params))
        race_dates = [row[0] for row in cursor.fetchall()]
        logging.info(f"Fetched {len(race_dates)} race_dates for {stat_type}.")

        if not race_dates:
            logging.info(f"No new race dates to process for stat type: {stat_type}.")
            return last_race_date_processed

        logging.info(f"Processing stat type: {stat_type} for {len(race_dates)} new race dates.")

        # Fetch all horses from the existing 'horse' table
        cursor.execute("SELECT axciskey FROM horse")
        all_horses = [row[0] for row in cursor.fetchall()]
        logging.info(f"Total horses fetched from 'horse' table: {len(all_horses)}.")
        if not all_horses:
            logging.warning("No horses found in the 'horse' table. Exiting processing.")
            return last_race_date_processed

        for race_date in race_dates:
            try:
                # Create a savepoint before processing each race_date
                cursor.execute("SAVEPOINT before_race_date_processing;")
                logging.info(f"Processing race_date: {race_date} for stat_type: {stat_type}")

                # Fetch all horses that actually ran the race
                cursor.execute("""
                    SELECT DISTINCT re.axciskey
                    FROM results_entries re
                    JOIN race_results rr ON
                        re.course_cd = rr.course_cd
                        AND re.race_date = rr.race_date
                        AND re.race_number = rr.race_number
                    WHERE rr.race_date = %s
                    AND rr.surface = %s
                """, (race_date, surface))
                actual_horses = [row[0] for row in cursor.fetchall()]
                logging.info(f"Race_date {race_date}: {len(actual_horses)} actual_horses found.")
                if not actual_horses:
                    logging.warning(f"No actual_horses found for race_date {race_date}. Skipping.")
                    continue

                # Process horses that ran the race
                for horse in actual_horses:
                    try:
                        # Fetch all races before or on the current race date for this horse
                        cursor.execute("""
                            SELECT
                                rr.race_date,
                                re.official_fin,
                                COALESCE(re2.earnings, 0.00) AS earnings
                            FROM results_entries re
                            JOIN race_results rr ON
                                re.course_cd = rr.course_cd
                                AND re.race_date = rr.race_date
                                AND re.race_number = rr.race_number
                            LEFT JOIN results_earnings re2 ON
                                re.course_cd = re2.course_cd
                                AND re.race_date = re2.race_date
                                AND re.race_number = re2.race_number
                                AND re.official_fin = re2.split_num
                            WHERE re.axciskey = %s
                            AND rr.race_date <= %s
                            ORDER BY rr.race_date ASC
                        """, (horse, race_date))
                        all_races = cursor.fetchall()
                        logging.debug(f"Race_date {race_date}, horse {horse}: {len(all_races)} races found.")

                        if not all_races:
                            # Insert initial record with zeros if no previous races exist
                            cursor.execute("""
                                INSERT INTO horse_accum_stats (
                                    axciskey,
                                    stat_type,
                                    as_of_date,
                                    starts,
                                    win,
                                    place,
                                    "show",
                                    fourth,
                                    earnings
                                ) VALUES (%s, %s, %s, 0, 0, 0, 0, 0, 0.0)
                                ON CONFLICT (axciskey, stat_type, as_of_date)
                                DO NOTHING;
                            """, (horse, stat_type, race_date))
                            logging.info(f"Inserted initial horse_accum_stats for horse {horse} on date {race_date} with zeros.")
                            continue

                        # Accumulate stats excluding the current race
                        cumulative_stats = {
                            'starts': 0,
                            'win': 0,
                            'place': 0,
                            'show': 0,
                            'fourth': 0,
                            'earnings': decimal.Decimal(0),
                        }

                        for past_race in all_races[:-1]:  # Exclude the current race
                            past_race_date, official_fin, earnings = past_race
                            cumulative_stats['starts'] += 1
                            if official_fin == 1:
                                cumulative_stats['win'] += 1
                            elif official_fin == 2:
                                cumulative_stats['place'] += 1
                            elif official_fin == 3:
                                cumulative_stats['show'] += 1
                            elif official_fin == 4:
                                cumulative_stats['fourth'] += 1
                            cumulative_stats['earnings'] += decimal.Decimal(earnings)

                        logging.debug(f"Cumulative stats for horse {horse} up to {race_date}: {cumulative_stats}")

                        # Insert cumulative stats for the current race date
                        cursor.execute("""
                            INSERT INTO horse_accum_stats (
                                axciskey,
                                stat_type,
                                as_of_date,
                                starts,
                                win,
                                place,
                                "show",
                                fourth,
                                earnings
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (axciskey, stat_type, as_of_date)
                            DO UPDATE SET
                                starts = EXCLUDED.starts,
                                win = EXCLUDED.win,
                                place = EXCLUDED.place,
                                "show" = EXCLUDED."show",
                                fourth = EXCLUDED.fourth,
                                earnings = EXCLUDED.earnings;
                        """, (
                            horse,
                            stat_type,
                            race_date,
                            cumulative_stats['starts'],
                            cumulative_stats['win'],
                            cumulative_stats['place'],
                            cumulative_stats['show'],
                            cumulative_stats['fourth'],
                            float(cumulative_stats['earnings'])
                        ))
                        logging.info(f"Updated horse_accum_stats for horse {horse} on date {race_date}.")

                    except Exception as e:
                        logging.error(f"Error processing horse {horse} for race date {race_date}: {e}")
                        traceback.print_exc()
                        # Roll back to the savepoint to continue processing other horses
                        cursor.execute("ROLLBACK TO SAVEPOINT before_race_date_processing;")
                        logging.info(f"Rolled back to savepoint before processing horse {horse} for race date {race_date}.")

            except Exception as e:
                logging.error(f"Error processing race date {race_date} for stat type {stat_type}: {e}")
                traceback.print_exc()
                # Roll back to the savepoint to continue processing other race_dates
                cursor.execute("ROLLBACK TO SAVEPOINT before_race_date_processing;")
                logging.info(f"Rolled back to savepoint before processing race date {race_date}.")

            finally:
                # Release the savepoint
                cursor.execute("RELEASE SAVEPOINT before_race_date_processing;")

            # After processing all entries for the race_date, mark it as processed
            try:
                update_ingestion_status(conn, str(race_date), "processed", stat_type)
                logging.info(f"Marked race_date {race_date} as processed for stat type {stat_type}.")
            except Exception as e:
                logging.error(f"Error updating ingestion status for race date {race_date} and stat type {stat_type}: {e}")
                traceback.print_exc()
                conn.rollback()

            # Update last_race_date_processed
            last_race_date_processed = race_date

    except Exception as e:
        logging.error(f"Error processing stat type {stat_type}: {e}")
        traceback.print_exc()
        conn.rollback()

    return last_race_date_processed


def update_horse_accum_stats(script_dir):
    """Main function to update horse_accum_stats for multiple stat types."""
    try:
        setup_logging(script_dir)
        config = read_config(script_dir)
        conn = get_db_connection(config)
        cursor = conn.cursor()

        # Define stat types to process
        stat_types = [
            # Dirt
            ('DIRT_SPRNT', 'D', None, 700, None),
            ('DIRT_RTE', 'D', 701, None, None),
            ('MUDDY_SPRNT', 'D', None, 700, "rr.track_condition = 'Muddy'"),
            ('MUDDY_RTE', 'D', 701, None, "rr.track_condition = 'Muddy'"),
            
            # Turf
            ('TURF_SPRNT', 'T', None, 700, None),
            ('TURF_RTE', 'T', 701, None, None),
            
            # All Weather
            ('ALL_WEATHER_SPRNT', 'A', None, 700, None),
            ('ALL_WEATHER_RTE', 'A', 701, None, None),
            
            # Race Types
            ('ALLOWANCE_SPRNT', 'D', None, 701, "rr.type = 'Allowance'"),
            ('ALLOWANCE_RTE', 'D', 701, None, "rr.type = 'Allowance'"),
            ('CLAIMING_SPRNT', 'D', None, 701, "rr.type = 'Claiming'"),
            ('CLAIMING_RTE', 'D', 701, None, "rr.type = 'Claiming'"),
            ('STAKES_SPRNT', 'D', None, 701, "rr.type = 'Stakes'"),
            ('STAKES_RTE', 'D', 701, None, "rr.type = 'Stakes'")
        ]
        
        # Initialize stat types in the database
        logging.info("Initializing stat types in stat_type_code table...")
        initialize_stat_types(conn, stat_types)
        logging.info("Stat types initialized successfully.")

        # Get the list of stat types that need processing
        stat_types_to_process = get_unprocessed_stat_types(conn)
        logging.info(f"Unprocessed stat types: {stat_types_to_process}")
        
        # Process each stat_type
        for stat_type, surface, min_distance, max_distance, filter_condition in stat_types:
            if stat_type not in stat_types_to_process:
                logging.info(f"Skipping already processed stat type: {stat_type}")
                continue

            logging.info(f"Starting processing for stat type: {stat_type}")

            # Process the stat_type
            last_race_date = process_stat_type(
                conn=conn,
                cursor=cursor,
                stat_type=stat_type,
                surface=surface,
                max_distance=max_distance,
                min_distance=min_distance,
                filter_condition=filter_condition
            )

            # Update the last_processed_date with the actual last race date processed
            if last_race_date:
                update_stat_type_last_processed(conn, stat_type, last_race_date)
                logging.info(f"Updated last processed date for stat type: {stat_type} to {last_race_date}")
            else:
                logging.info(f"No race dates were processed for stat type: {stat_type}")

        # Commit all changes
        conn.commit()
        logging.info("Successfully updated horse_accum_stats for all stat types.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        traceback.print_exc()
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        logging.info("Database connection closed.")


if __name__ == "__main__":
    script_directory = os.path.dirname(os.path.abspath(__file__))
    logging.info(f"script_directory: {script_directory}")
    update_horse_accum_stats(script_directory)