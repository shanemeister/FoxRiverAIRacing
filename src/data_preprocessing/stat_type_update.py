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
    """Sets up logging configuration to write logs to a file."""
    try:
        # Default log directory
        if not log_dir:
            log_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs'
        
        # Ensure the log directory exists
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'stat_type_update.log')

        # Clear the log file by opening it in write mode
        with open(log_file, 'w'):
            pass  # truncate file

        # Create a logger and clear existing handlers
        logger = logging.getLogger()
        if logger.hasHandlers():
            logger.handlers.clear()

        logger.setLevel(logging.INFO)

        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        # Define a common format
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
    For ALL_RACES stat_type, no surface or filter conditions are applied.
    """
    last_race_date_processed = None
    try:
        # Ensure the stat_type exists in stat_type_code
        ensure_stat_type_exists(conn, stat_type)

        # Get the last_processed date
        last_processed_date = get_last_processed_date(conn, stat_type)
        logging.info(f"Last processed date for {stat_type}: {last_processed_date}")

        # Build the conditions for fetching race_dates
        conditions = []
        params = [stat_type]  # always have stat_type for ingestion_files check

        # Only check ingestion_files for processed race_dates for this stat_type
        # If last_processed_date is given, fetch only after that date
        if last_processed_date:
            conditions.append("rr.race_date > %s")
            params.append(last_processed_date)

        # If surface is specified (not ALL_RACES), add surface condition
        if surface is not None:
            conditions.append("rr.surface = %s")
            params.append(surface)

        # Add filter_condition if provided
        # (Not currently used to filter race_dates directly, but you could if needed)
        
        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
            SELECT DISTINCT rr.race_date
            FROM race_results rr
            LEFT JOIN ingestion_files if2 
                ON rr.race_date::text = if2.file_name
                AND if2.message = %s
                AND if2.status = 'processed'
            {where_clause}
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

                # Fetch all horses that actually ran the race for this stat_type
                # If surface is specified, filter by surface and possibly filter_condition
                # If surface is None (ALL_RACES), do not restrict by surface
                race_conditions = []
                race_params = [race_date]

                if surface is not None:
                    race_conditions.append("rr.surface = %s")
                    race_params.append(surface)
                if filter_condition:
                    race_conditions.append(filter_condition)

                race_where = ""
                if race_conditions:
                    race_where = "AND " + " AND ".join(race_conditions)

                cursor.execute(f"""
                    SELECT DISTINCT re.axciskey
                    FROM results_entries re
                    JOIN race_results rr ON
                        re.course_cd = rr.course_cd
                        AND re.race_date = rr.race_date
                        AND re.race_number = rr.race_number
                    WHERE rr.race_date = %s
                    {race_where}
                """, tuple(race_params))
                actual_horses = [row[0] for row in cursor.fetchall()]
                logging.info(f"Race_date {race_date}: {len(actual_horses)} actual_horses found.")
                if not actual_horses:
                    logging.warning(f"No actual_horses found for race_date {race_date}. Skipping.")
                    continue

                # Process horses that ran the race
                for horse in actual_horses:
                    try:
                        # Fetch all races before or on the current race date for this horse
                        # If we are ALL_RACES, no conditions; else filter by surface and condition if needed
                        # Actually, we just fetch all races anyway because we are calculating cumulative stats
                        # for that specific stat_type definition. The script is currently not filtering by surface
                        # in this section, which might be desired for stat_type-specific accumulation.
                        # For ALL_RACES, we don't filter by surface or condition here at all.
                        base_query = """
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
                        """

                        filters = []
                        filter_params = [horse, race_date]

                        # If stat_type is not ALL_RACES, and we have defined conditions:
                        if stat_type != 'ALL_RACES':
                            if surface is not None:
                                filters.append("rr.surface = %s")
                                filter_params.append(surface)
                            if filter_condition:
                                filters.append(filter_condition)

                        if filters:
                            base_query += " AND " + " AND ".join(filters)

                        base_query += " ORDER BY rr.race_date ASC"

                        cursor.execute(base_query, tuple(filter_params))
                        all_races = cursor.fetchall()
                        logging.debug(f"Race_date {race_date}, horse {horse}: {len(all_races)} races found for stat_type {stat_type}.")

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
                            logging.info(f"Inserted initial horse_accum_stats for horse {horse} on date {race_date} with zeros for stat_type {stat_type}.")
                            continue

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

                        logging.debug(f"Cumulative stats for horse {horse} up to {race_date} for stat_type {stat_type}: {cumulative_stats}")

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
                        logging.info(f"Updated horse_accum_stats for horse {horse} on date {race_date} stat_type {stat_type}.")

                    except Exception as e:
                        logging.error(f"Error processing horse {horse} for race date {race_date} stat_type {stat_type}: {e}")
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
                logging.info(f"Marked race_date {race_date} as processed for stat_type {stat_type}.")
            except Exception as e:
                logging.error(f"Error updating ingestion status for race date {race_date} and stat_type {stat_type}: {e}")
                traceback.print_exc()
                conn.rollback()

            last_race_date_processed = race_date

    except Exception as e:
        logging.error(f"Error processing stat type {stat_type}: {e}")
        traceback.print_exc()
        conn.rollback()

    return last_race_date_processed


def update_horse_accum_stats(script_dir):
    """Main function to update horse_accum_stats for multiple stat types, including ALL_RACES."""
    try:
        setup_logging(script_dir)
        config = read_config(script_dir)
        conn = get_db_connection(config)
        cursor = conn.cursor()

        # Add ALL_RACES stat type with no conditions
        stat_types = [
            ('ALL_RACES', None, None, None, None),
            # Dirt
            ('DIRT_SPRNT', 'D', None, 700, None),
            ('DIRT_RTE', 'D', 701, None, None),
            ('MUDDY_SPRNT', 'D', None, 700, "trk_cond = 'Muddy'"),
            ('MUDDY_RTE', 'D', 701, None, "trk_cond = 'Muddy'"),
            
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
        logging.info("Successfully updated horse_accum_stats for all stat types, including ALL_RACES.")
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