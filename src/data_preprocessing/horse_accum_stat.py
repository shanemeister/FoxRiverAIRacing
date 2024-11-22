import logging
import traceback
import os
import sys
import datetime
import psycopg2
from psycopg2 import sql
from src.data_ingestion.ingestion_utils import get_db_connection
import configparser
from collections import defaultdict

def setup_logging(script_dir):
    """
    Sets up logging configuration to write logs to a file.
    
    Args:
        script_dir (str): Directory where the script is located.
    """
    try:
        # Define log directory and file
        log_dir = os.path.join(script_dir, '../../logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'horse_accum_stat.log')
        
        # Configure logging
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        print("Logging has been set up successfully.")
        logging.info("Logging has been set up successfully.")
    except Exception as e:
        print(f"Failed to set up logging: {e}")
        sys.exit(1)

def read_config(script_dir):
    """
    Reads the configuration file and returns the configuration object.
    
    Args:
        script_dir (str): Directory where the script is located.
    
    Returns:
        configparser.ConfigParser: The configuration object.
    """
    try:
        config = configparser.ConfigParser()
        
        # Construct the absolute path to config.ini
        root_dir = os.path.abspath(os.path.join(script_dir, '../../'))
        config_file_path = os.path.join(root_dir, 'config.ini')
        
        logging.info(f"Reading configuration from {config_file_path}")
        print(f"Reading configuration from {config_file_path}")
        
        if not os.path.exists(config_file_path):
            logging.error(f"Configuration file '{config_file_path}' does not exist.")
            print(f"Configuration file '{config_file_path}' does not exist.")
            raise FileNotFoundError(f"Configuration file '{config_file_path}' does not exist.")
        
        config.read(config_file_path)
        
        if 'database' not in config:
            logging.error("The 'database' section is missing in the configuration file.")
            print("The 'database' section is missing in the configuration file.")
            raise KeyError("The 'database' section is missing in the configuration file.")
        
        logging.info("Configuration file read successfully.")
        print("Configuration file read successfully.")
        return config
    except Exception as e:
        logging.error(f"Error reading configuration file: {e}")
        print(f"Error reading configuration file: {e}")
        sys.exit(1)

def update_horse_accum_stats(script_dir):
    """
    Updates the horse_accum_stats table with DIRT_SPRNT stats.
    Inserts zeroed records for non-participating horses and updates/inserts records for participating horses.
    
    Args:
        script_dir (str): Directory where the script is located.
    """
    try:
        # Setup logging
        setup_logging(script_dir)
        
        # Read configuration
        config = read_config(script_dir)
        
        # Establish database connection
        conn = get_db_connection(config)
        cursor = conn.cursor()
        logging.info("Database connection established.")
        print("Database connection established.")
        
        # Get today's date
        today = datetime.date.today()
        logging.info(f"Today's date: {today}")
        print(f"Today's date: {today}")
        
        # Fetch new DIRT_SPRNT races for today
        logging.info("Fetching new DIRT_SPRNT races for today.")
        print("Fetching new DIRT_SPRNT races for today.")
        cursor.execute("""
            SELECT
                re.axciskey,
                rr.race_date,
                rr.race_number,
                re.official_fin,
                COALESCE(re2.earnings, 0.00) AS earnings
            FROM results_entries re
            JOIN race_results rr ON
                re.course_cd = rr.course_cd AND
                re.race_date = rr.race_date AND
                re.race_number = rr.race_number
            LEFT JOIN results_earnings re2 ON
                re.course_cd = re2.course_cd AND
                re.race_date = re2.race_date AND
                re.race_number = re2.race_number AND
                re.official_fin = re2.split_num
            WHERE
                rr.surface = 'D'
                AND rr.distance <= 700
                AND rr.dist_unit = 'F'
                AND re.official_fin > 0
                AND rr.race_date = %s;
        """, (today,))
        
        new_races = cursor.fetchall()
        logging.info(f"Fetched {len(new_races)} new DIRT_SPRNT races for {today}.")
        print(f"Fetched {len(new_races)} new DIRT_SPRNT races for {today}.")
        
        if not new_races:
            logging.info("No new DIRT_SPRNT races found for today.")
            print("No new DIRT_SPRNT races found for today.")
        else:
            # Aggregate new races per horse
            stats = defaultdict(lambda: {'starts':0, 'win':0, 'place':0, 'show':0, 'fourth':0, 'earnings':0.00})
            
            for race in new_races:
                axciskey, race_date, race_number, official_fin, earnings = race
                stats[axciskey]['starts'] += 1
                if official_fin == 1:
                    stats[axciskey]['win'] += 1
                elif official_fin == 2:
                    stats[axciskey]['place'] += 1
                elif official_fin == 3:
                    stats[axciskey]['show'] += 1
                elif official_fin == 4:
                    stats[axciskey]['fourth'] += 1
                stats[axciskey]['earnings'] += earnings
            
            logging.info(f"Aggregated stats for {len(stats)} horses.")
            print(f"Aggregated stats for {len(stats)} horses.")
            
            # Update or insert stats for participating horses
            for axciskey, data in stats.items():
                logging.info(f"Processing horse {axciskey}: starts={data['starts']}, win={data['win']}, place={data['place']}, show={data['show']}, fourth={data['fourth']}, earnings={data['earnings']}")
                print(f"Processing horse {axciskey}: starts={data['starts']}, win={data['win']}, place={data['place']}, show={data['show']}, fourth={data['fourth']}, earnings={data['earnings']}")
                
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
                    ) VALUES (%s, 'DIRT_SPRNT', %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (axciskey, stat_type, as_of_date) 
                    DO UPDATE SET
                        starts = horse_accum_stats.starts + EXCLUDED.starts,
                        win = horse_accum_stats.win + EXCLUDED.win,
                        place = horse_accum_stats.place + EXCLUDED.place,
                        "show" = horse_accum_stats."show" + EXCLUDED."show",
                        fourth = horse_accum_stats.fourth + EXCLUDED.fourth,
                        earnings = horse_accum_stats.earnings + EXCLUDED.earnings;
                """, (
                    axciskey,
                    today,
                    data['starts'],
                    data['win'],
                    data['place'],
                    data['show'],
                    data['fourth'],
                    data['earnings']
                ))
                logging.info(f"Updated stats for horse {axciskey}.")
                print(f"Updated stats for horse {axciskey}.")
        
        # Insert zeroed records for non-participating horses
        logging.info("Inserting zeroed records for non-participating horses.")
        print("Inserting zeroed records for non-participating horses.")
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
            )
            SELECT
                h.axciskey,
                'DIRT_SPRNT',
                %s,
                0,
                0,
                0,
                0,
                0,
                0.00
            FROM horse h
            WHERE NOT EXISTS (
                SELECT 1
                FROM horse_accum_stats hs
                WHERE hs.axciskey = h.axciskey
                  AND hs.stat_type = 'DIRT_SPRNT'
                  AND hs.as_of_date = %s
            );
        """, (today, today))
        logging.info("Inserted zeroed records for non-participating horses.")
        print("Inserted zeroed records for non-participating horses.")
        
        # Commit the transaction
        conn.commit()
        logging.info("Successfully committed all changes to the database.")
        print("Successfully committed all changes to the database.")
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")
        traceback_str = ''.join(traceback.format_tb(e.__traceback__))
        logging.error(f"Traceback: {traceback_str}")
        print(f"Traceback: {traceback_str}")
        if 'conn' in locals():
            conn.rollback()
            logging.info("Rolled back the transaction due to error.")
            print("Rolled back the transaction due to error.")
    finally:
        if 'cursor' in locals():
            cursor.close()
            logging.info("Database cursor closed.")
            print("Database cursor closed.")
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")
            print("Database connection closed.")

if __name__ == "__main__":
    # Determine the directory where the script is located
    script_directory = os.path.dirname(os.path.abspath(__file__))
    
    # Execute the update function
    update_horse_accum_stats(script_directory)