import configparser
import logging
import os
import sys
import pandas as pd
from psycopg2 import sql, pool, DatabaseError
import psycopg2
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


def setup_logging(sript_dir, log_dir=None):
    """Sets up logging configuration to write logs to a file and the console."""
    try:
        # Default log directory
        if not log_dir:
            log_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs'
        
        # Ensure the log directory exists
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'wagering.log')
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

def parse_winners_str(winners_str: str):
    """
    Examples:
      "6-2-5"             -> [["6"], ["2"], ["5"]]
      "3-6-1/4/7/9"       -> [["3"], ["6"], ["1","4","7","9"]]
      "Pick 3 => 6-1/4/7/9-5"   -> Remove text or parse carefully
    """
    if not winners_str:
        return []

    # Optional: if there's extraneous text like "Pick 3 =>", strip it out.
    # For instance, if some data includes "Pick 3 => 6-1-4", you might do:
    # winners_str = winners_str.split('=>')[-1].strip()

    parts = winners_str.split('-')  # split by '-'
    parsed = [p.split('/') for p in parts]  # each part may have slashes
    return parsed

def convert_timestamp_columns(spark_df, timestamp_format="yyyy-MM-dd HH:mm:ss"):
                """
                Finds all TimestampType columns in a Spark DataFrame, converts them to strings using the specified format,
                and returns the modified DataFrame and a list of the names of the columns that were converted.
                """
                # Get list of timestamp columns from the schema.
                timestamp_cols = [field.name for field in spark_df.schema.fields if isinstance(field.dataType, TimestampType)]
                print("Timestamp columns found in Spark DataFrame:", timestamp_cols)
                
                # For each timestamp column, convert to string using date_format.
                for col in timestamp_cols:
                    spark_df = spark_df.withColumn(col, F.date_format(F.col(col), timestamp_format))
                return spark_df, timestamp_cols

def gather_bet_metrics(race, combos, cost, payoff, my_wager, actual_combo, field_size):
    """
    Builds a single dict of metrics for a (multi-)race bet outcome,
    returning only str, float, int, or None for each column
    to avoid Spark's type inference issues.
    """
    import math

    net = payoff - cost
    hit_flag = 1 if payoff > 0 else 0

    # Convert race fields to strings/floats
    course_cd = str(race.course_cd) if race.course_cd else None
    race_date = str(race.race_date) if race.race_date else None
    race_number = int(race.race_number) if race.race_number is not None else None
    surface = str(race.surface) if race.surface else None
    track_condition = str(race.track_condition) if race.track_condition else None
    race_type = str(race.race_type) if race.race_type else None

    distance_meters = float(race.distance_meters) if race.distance_meters else None
    avg_purse_val_calc = float(race.avg_purse_val_calc) if race.avg_purse_val_calc else None

    # my_wager => only store base_amount as float, not the object itself
    base_amount = float(my_wager.base_amount) if my_wager else 2.0

    # combos => turn into a single string
    # e.g. combos = [('3','1','5'), ('3','2','5')] => ["3-1-5", "3-2-5"] => "3-1-5|3-2-5"
    combo_str_list = ['-'.join(c) for c in combos]
    generated_combos_str = '|'.join(combo_str_list)

    # actual_combo => also string
    # e.g. "[['3'], ['1'], ['5']]"
    actual_winning_combo_str = str(actual_combo)

    # float or int for cost/payoff etc.
    cost_float = float(cost)
    payoff_float = float(payoff)
    net_float = float(net)
    combos_generated = len(combos)
    field_size_int = int(field_size)
    roi_float = net_float / cost_float if cost_float > 0 else 0.0

    row_data = {
        "course_cd":         course_cd,            # str or None
        "race_date":         race_date,            # str or None
        "race_number":       race_number,          # int or None
        "surface":           surface,              # str or None
        "distance_meters":   distance_meters,      # float or None
        "track_condition":   track_condition,      # str or None
        "avg_purse_val_calc":avg_purse_val_calc,   # float or None
        "race_type":         race_type,            # str or None

        "base_amount":       base_amount,          # float
        "combos_generated":  combos_generated,      # int
        "cost":              cost_float,           # float
        "payoff":            payoff_float,         # float
        "net":               net_float,            # float
        "hit_flag":          hit_flag,             # int

        "actual_winning_combo":  actual_winning_combo_str,  # str
        "generated_combos":      generated_combos_str,      # str

        "roi":               roi_float,            # float
        "field_size":        field_size_int        # int
    }

    return row_data

def save_results_to_parquet(rows, filename="my_bet_results.parquet"):
    """
    Takes a list of dict rows (each row from gather_bet_metrics) 
    and writes them to a Parquet file.
    """
    if not rows:
        logging.info("No rows to save. Exiting.")
        return
    
    df = pd.DataFrame(rows)
    df.to_parquet(filename, index=False)
    logging.info(f"Saved {len(df)} bet results to {filename}")

# def get_base_choice():
#     # 1) Confirm y/n for posted base
#     while True:
#         use_posted_str = input("Use each race's posted base? (y/n): ").strip().lower()
#         if use_posted_str in ('y', 'n'):
#             break
#         else:
#             print("Please enter 'y' or 'n'.")

#     use_posted_base = (use_posted_str == 'y')

    # 2) If user says "no" (n), we ask for a numeric base_amount
    if not use_posted_base:
        base_input = input("Enter the base wager amount (e.g. 2.0): ").strip()
        if not base_input:
            # If user just pressed Enter, default to 2.0
            print("No amount entered. Defaulting to 2.0")
            base_amount = 2.0
        else:
            try:
                base_amount = float(base_input)
            except ValueError:
                print("Invalid amount. Defaulting to 2.0")
                base_amount = 2.0
    else:
        # 3) If user says "yes" (y), we won't ask for numeric base
        # Let your code handle posted_base from the data
        base_amount = None

    return use_posted_base, base_amount
    
def get_user_wager_preferences():
    """
    Interactively prompts the user for:
      1) Wager Type
      2) Base amount
      3) Whether it's a 'box' or not (for single-race exotics)
      4) Possibly more fields later (key horse, partial wheel, etc.)

    Returns a dict containing the chosen options.
    """
    print("Please select a Wager Type from the following list:")
    wager_types = [
        "Exacta",
        "Daily Double",
        "Trifecta",
        "Superfecta",
        "Pick 3",
        "Pick 4",
        "Pick 5",
        "Pick 6",
        "Quinella",
        # etc. add more if needed
    ]
    for i, wt in enumerate(wager_types, start=1):
        print(f"{i}. {wt}")

    # Prompt user
    while True:
        choice = input("Enter the number corresponding to the Wager Type: ")
        try:
            choice_idx = int(choice) - 1
            if choice_idx < 0 or choice_idx >= len(wager_types):
                raise ValueError
            selected_wager_type = wager_types[choice_idx]
            break
        except ValueError:
            print("Invalid input. Select a number.")
            
    # 1) Ask for wager amount
    while True:
        get_wager_amnt = input("Enter the amount you wish to wager (Whole Number, 1,2,3, etc.): ").strip()
        try:
            wager_amount = int(get_wager_amnt)
            if wager_amount > 0:
                break
            else:
                print("Please enter a positive integer.")
        except ValueError:
            print("Invalid input. Please enter a whole number (e.g., 1, 2, 3).")    
    
    # 2) Prompt user with the default in parentheses
    TOP_N_DEFAULTS = {
    "Exacta": 2,
    "Trifecta": 3,
    "Superfecta": 4,
    "Daily Double": 1,
    "Pick 3": 1,
    "Pick 4": 1,
    "Pick 5": 1,
    "Pick 6": 1,
    "Quinella": 2,
    }

    default_top_n = TOP_N_DEFAULTS.get(selected_wager_type, 1)

    while True:
        msg = f"Enter the number of top horses to consider (default {default_top_n}): "
        raw = input(msg).strip()
        if not raw:
            top_n = default_top_n
            break
        try:
            top_n = int(raw)
            if top_n > 0:
                break
            else:
                print("Please enter a positive integer.")
        except ValueError:
            print("Invalid input. Please enter a whole number or press Enter for the default.")

            # 3) Use the default if user pressed Enter, otherwise convert to int
            return int(raw) if raw else default_top_n
    
    # If single-race exotic, ask about box
    # (Daily Double, Pick 3, etc. won't box in the same sense, but let's keep it simple)
    is_box = False
    while True:
        is_box = input("Box this wager? (y/n): ").strip().lower()
        if is_box in ('y', 'n'):
            break
        else:
            print("Please enter 'y' or 'n'.")
            
    if is_box == "y":
         is_box = True
    else:
         is_box = False

    if selected_wager_type == "Daily Double":
        num_legs = 2
        
    if selected_wager_type in ["Pick 3", "Pick 4", "Pick 5", "Pick 6"]:
        num_legs = int(selected_wager_type[-1])  # Extract the last character and convert it to an integer        box_choice = input("Box this wager? (y/n): ").strip().lower()
    # (Optional) Key horse prompt
    # For now, skip or implement a simple version:
    # key_horse_input = input("Enter a key horse program number or leave blank for none: ").strip()
    # key_horse = key_horse_input if key_horse_input else None

    # Return a dictionary of choices
    return {
        "wager_type": selected_wager_type,
        "wager_amount": wager_amount,
        "is_box": is_box,
        "num_legs": num_legs if selected_wager_type in ["Daily Double", "Pick 3", "Pick 4", "Pick 5", "Pick 6"] else None,
        "top_n": top_n,
        # "key_horse": key_horse,  # If you implement that logic
    }

def group_races_for_pick3(all_races):
    """
    Group or identify consecutive sets of 3 races (leg1, leg2, leg3)
    based on (course_cd, race_date, consecutive race_number).
    Returns a list of [ ( [race1, race2, race3], wagers_key ), ... ]
    so we know which actual combos to check in wagers_dict, etc.
    """
    grouped = {}
    # Build a dict keyed by (course_cd, race_date) => sorted list of Race objects
    for race in all_races:
        key = (race.course_cd, race.race_date)
        grouped.setdefault(key, []).append(race)

    # Sort each group by race_number
    for k in grouped:
        grouped[k].sort(key=lambda r: r.race_number)
    for k, race_list in grouped.items():
        race_nums = [r.race_number for r in race_list]

    # Now produce triple sets
    pick3_sets = []
    for (course, rdate), races_list in grouped.items():
        # e.g., if there's 7 races, we can have pick3 sets (race1,2,3), (race2,3,4) etc.
        for i in range(len(races_list) - 2):
            r1 = races_list[i]
            r2 = races_list[i+1]
            r3 = races_list[i+2]
            pick3_sets.append([r1, r2, r3])
    return pick3_sets