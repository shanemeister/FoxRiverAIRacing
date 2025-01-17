import logging
import os
import sys
import configparser
import pandas as pd
import numpy as np
from psycopg2 import sql, pool, DatabaseError
from catboost import CatBoostRegressor, Pool
from src.data_preprocessing.data_prep1.data_utils import save_parquet, initialize_environment
from src.data_preprocessing.fox_query import full_query_df

def setup_logging(script_dir, log_dir=None):
    """Sets up logging configuration to write logs to a file and the console."""
    try:
        # Default log directory
        if not log_dir:
            log_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs'
        
        # Ensure the log directory exists
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'fox_speed_figure.log')

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
        
def create_custom_speed_figure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Suppose your df has columns like:
      - 'time_behind' = running_time - win_time
      - 'pace_delta_time' = running_time - par_time
      - 'speed_rating'
      - 'class_rating'
      - 'previous_class'
      - 'power'
      - plus trainer/jockey stats, etc.
      - 'official_fin' as the final position
    We'll define a small 'performance target' => e.g. 1->100, 2->95, etc.
    Then we feed these columns into a CatBoostRegressor to learn a single numeric figure.
    """

    # 1) Define a mapped target
    def finishing_pos_to_score(pos: float) -> float:
        # example
        if pos == 1.0:
            return 100
        elif pos == 2.0:
            return 95
        elif pos == 3.0:
            return 90
        elif pos == 4.0:
            return 85
        else:
            return max(0, 80 - 2*(pos - 4))

    df = df.copy()
    df["perf_target"] = df["official_fin"].apply(finishing_pos_to_score)

    # 2) Choose features
    # numeric_features = ["time_behind", "pace_delta_time", "speed_rating", "class_rating", "previous_class", "power", ...]
    # plus trainer stats columns, if numeric
    numeric_features = [
       "distance_meters",
       "time_behind",
       "pace_delta_time",
       "speed_rating",
       "class_rating",
       "previous_class",
       "power",
       "starts", 
       "horse_itm_pecentage"
    ]

    # *categorical* columns (like track_conditions?)
    cat_cols = ["course_cd", "track_conditions"]  

    # 3) Build train Pool
    X = df[numeric_features]
    y = df["perf_target"]

    # For a simple approach, let's train on *all* data. 
    # In production, you might do a train/val split or cross‐validation for hyperparameters.
    train_pool = Pool(X, label=y, cat_features=cat_cols)

    # 4) Fit a small CatBoostRegressor
    model = CatBoostRegressor(
        iterations=500,
        depth=4,
        learning_rate=0.1,
        loss_function="RMSE",
        random_seed=42,
        verbose=50
    )
    model.fit(train_pool)

    # 5) Predict the custom speed figure
    df["custom_speed_figure"] = model.predict(train_pool)
    
    return df

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
        conn = db_pool.getconn()
        # Load and write data to parquet
        queries = full_query_df()
        speed = None
    
        for name, query in queries.items():
            if name == "speed_figure":
                conn = db_pool.getconn()
                speed = spark.read.jdbc(
                    url=jdbc_url,
                    table=f"({query}) AS subquery",
                    properties=jdbc_properties
                )
                speed.printSchema()
                try:
                    enhanced_df = create_custom_speed_figure(speed)
                    save_parquet(spark, enhanced_df, "speed_figure", parquet_dir)
                    logging.info("Ingestion job succeeded")
                    spark.catalog.clearCache()

                finally:
                    db_pool.putconn(conn)
                # Now 'enhanced_df' has a new column 'custom_speed_figure'.
                # You can feed 'custom_speed_figure' into your final ranker as a single numeric feature.
    except Exception as e:
        logging.error(f"Error during Spark initialization: {e}")
        sys.exit(1)
    if db_pool:
        db_pool.closeall()
    spark.stop()
    logging.info("All tasks completed. Spark session stopped and DB pool closed.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()