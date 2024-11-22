import logging
import traceback
import os
import sys
import datetime
import psycopg2
from psycopg2 import sql
from collections import defaultdict
from src.data_ingestion.ingestion_utils import get_db_connection, update_ingestion_status, update_stat_type_last_processed, initialize_stat_types, get_unprocessed_stat_types, ensure_stat_type_exists, get_last_processed_date
import configparser
import decimal

def setup_logging(script_dir):
    """Sets up logging configuration to write logs to a file and the console."""
    try:
        # Use the absolute path for the logs directory
        log_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs'
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'horse_accum_stats_sequential.log')

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
        logging.info(f"Failed to set up logging: {e}")
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
    """
    try:
        # Ensure the stat_type exists in stat_type_code
        ensure_stat_type_exists(conn, stat_type)

        # Get the last_processed date
        last_processed_date = get_last_processed_date(conn, stat_type)
        start_date_condition = "AND rr.race_date > %s" if last_processed_date else ""
        params = [surface]
        if last_processed_date:
            params.append(last_processed_date)

        # Fetch race dates that haven't been processed for this stat_type
        cursor.execute(f"""
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
        """, (stat_type, *params))
        race_dates = [row[0] for row in cursor.fetchall()]

        if not race_dates:
            logging.info(f"No new race dates to process for stat type: {stat_type}.")
            return

        logging.info(f"Processing stat type: {stat_type} for {len(race_dates)} new race dates.")

        for race_date in race_dates:
            try:
                logging.info(f"Processing races on date: {race_date} for stat type: {stat_type}")

                # Get all horses participating on this race date
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
                horses = [row[0] for row in cursor.fetchall()]

                if not horses:
                    logging.info(f"No horses found for race date: {race_date}.")
                    update_ingestion_status(conn, str(race_date), "processed", stat_type)
                    continue

                for horse in horses:
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
                            ON CONFLICT DO NOTHING;
                        """, (horse, stat_type, race_date))
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
                update_ingestion_status(conn, str(race_date), "processed", stat_type)
            except Exception as e:
                logging.error(f"Error processing race date {race_date} for stat type {stat_type}: {e}")
                traceback.print_exc()

        update_stat_type_last_processed(conn, stat_type, race_date)
        logging.info(f"Processed stat type {stat_type} successfully for all new race dates.")
    except Exception as e:
        logging.info(f"Error processing stat type {stat_type}: {e}")
        traceback.print_exc()
        conn.rollback()
                
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
        
        # Get the list of stat types that need processing
        stat_types_to_process = get_unprocessed_stat_types(conn)
        logging.info(f"Unprocessed stat types: {stat_types_to_process}")
        
        # Process each unprocessed stat type
        for stat_type, surface, min_distance, max_distance, filter_condition in stat_types:
            if stat_type not in stat_types_to_process:
                continue

            process_stat_type(
                conn=conn,
                cursor=cursor,
                stat_type=stat_type,
                surface=surface,
                max_distance=max_distance,
                min_distance=min_distance,
                filter_condition=filter_condition
            )

            # Update the last processed date
            update_stat_type_last_processed(conn, stat_type, datetime.date.today())

        conn.commit()
        logging.info("Successfully updated horse_accum_stats for all stat types.")
        logging.info(f"Updated last processed date for stat type: {stat_type}")
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

if __name__ == "__main__":
    script_directory = os.path.dirname(os.path.abspath(__file__))
    logging.info (f"script_directory: {script_directory}")
    update_horse_accum_stats(script_directory)