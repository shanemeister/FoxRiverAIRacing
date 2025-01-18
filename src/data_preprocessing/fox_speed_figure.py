import logging
import os
import sys
import configparser
import pandas as pd
from psycopg2 import pool, DatabaseError
from sklearn.preprocessing import LabelEncoder
from catboost import CatBoostRegressor, Pool
from src.data_preprocessing.data_prep1.data_utils import save_parquet, initialize_environment
from src.data_preprocessing.fox_query import full_query_df
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql

def setup_logging(log_dir='/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs'):
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

        # Add handlers to the logger
        logger.addHandler(file_handler)

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
    df = df.toPandas()

    # Convert decimal to doubles:
    decimal_cols = ['distance_meters', 'class_rating', 'previous_class', 'power', 'horse_itm_percentage',
                    'starts', 'official_fin', 'time_behind', 'pace_delta_time']
    for col_name in decimal_cols:
        df[col_name] = df[col_name].astype(float)
    
    # Handle NaN values in relevant columns
    df[decimal_cols] = df[decimal_cols].fillna(0)  # Fill NaN values with 0 or another appropriate value
    
    logging.info("Decimal columns converted to float and filled NaN with 0.")
    logging.info(f"Columns: {df.dtypes}")
    
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
            return max(0, 80 - (5 * pos))
    
    df["perf_target"] = df["official_fin"].apply(finishing_pos_to_score)
    # Define a function to calculate perf_target
    df.columns
    # 2) Choose features
    numeric_features = [
        "distance_meters",
        "time_behind",
        "pace_delta_time",
        "speed_rating",
        "class_rating",
        "previous_class",
        "power",
        "starts",
        "horse_itm_percentage"
    ]

    # *categorical* columns (like track_conditions?)
    df["horse_id_original"] = df["horse_id"]
    
    cat_cols = ["horse_id", "course_cd", "track_conditions"]
    # Encode categorical columns
    for c in cat_cols:
        lbl = LabelEncoder()
        df[c] = lbl.fit_transform(df[c].astype(str))
           
     # 3) Build train Pool
    X = df[numeric_features + cat_cols]
    y = df["perf_target"]

    # For a simple approach, let's train on *all* data.
    # In production, you might want to use a train-test split or cross-validation for hyperparameters.
    train_pool = Pool(X, label=y, cat_features=cat_cols)

    # 4) Fit a small CatBoostRegressor
    model = CatBoostRegressor(
        iterations=1000,
        depth=4,
        learning_rate=0.1,
        loss_function="RMSE",
        random_seed=42,
        verbose=50
    )
    model.fit(train_pool)

    # 5) Predict the custom speed figure
    df["custom_speed_figure"] = model.predict(train_pool)
    
    corr = df["speed_rating"].corr(df["custom_speed_figure"])
    print(f"Correlation between speed_rating and custom_speed_figure: {corr}")
    
    corr = df["perf_target"].corr(df["custom_speed_figure"])
    print(f"Correlation between perf_target and custom_speed_figure: {corr}")

    corr = df["time_behind"].corr(df["custom_speed_figure"])
    print(f"Correlation between time_behind and custom_speed_figure: {corr}")
    return df

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = read_config(script_dir)
    log_file = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/recent_form.log"
    setup_logging()

    # Create DB pool (optional)
    db_pool = get_db_pool(config)

    # 1) Initialize your SparkSession and load data
    spark, jdbc_url, jdbc_properties, parquet_dir, _ = initialize_environment()

    # Suppose we have a dictionary of queries
    queries = full_query_df()  # you define these queries

    # Load them
    dfs = load_data_from_postgresql(
        spark, jdbc_url, jdbc_properties,
        queries, parquet_dir
    )

    for name, df in dfs.items():
        logging.info(f"DataFrame '{name}' loaded. Schema:")
        df.printSchema()
        if name == "speed_figure":
            speed = df
            logging.info(f"DataFrame '{name}' loaded. Schema:")
        elif name == "workouts":
            logging.info(f"DataFrame '{name}' not founc.")
    # 2) Compute
        # Process the DataFrame
        enhanced_df = create_custom_speed_figure(speed)
        
        # Convert pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(enhanced_df)
        
        # Select specific columns and show the first 5 rows
        top_4_move_up = spark_df.filter(
            (spark_df["official_fin"] <= 4) & (spark_df["class_rating"] >= spark_df["previous_class"])
        ).select(
            "horse_id", 
            "official_fin", 
            "class_rating", 
            "previous_class", 
            "speed_rating", 
            "horse_itm_percentage", 
            "perf_target", 
            "custom_speed_figure"
        ).show(30)
        logging.info(f"Top 5 finishers with moving up in class: {top_4_move_up}")
                    
        top_4_move_down = spark_df.filter(
            (spark_df["official_fin"] <= 4) & (spark_df["class_rating"] <= spark_df["previous_class"])
        ).select(
            "horse_id", 
            "official_fin", 
            "class_rating", 
            "previous_class", 
            "speed_rating", 
            "horse_itm_percentage", 
            "perf_target", 
            "custom_speed_figure"
        ).show(30)
        logging.info(f"Top 5 finishers with moving up in class: {top_4_move_down}")
        
        botton_move_up = spark_df.filter(
            (spark_df["official_fin"] >= 5) & (spark_df["class_rating"] >= spark_df["previous_class"])
        ).select(
            "horse_id", 
            "official_fin", 
            "class_rating", 
            "previous_class", 
            "speed_rating", 
            "horse_itm_percentage", 
            "perf_target", 
            "custom_speed_figure"
        ).show(30)       
        logging.info(f"Top 5 finishers with moving up in class: {botton_move_up}")
        
        bottom_move_down = spark_df.filter(
            (spark_df["official_fin"] <= 5) & (spark_df["class_rating"] <= spark_df["previous_class"])
        ).select(
            "horse_id", 
            "official_fin", 
            "class_rating", 
            "previous_class", 
            "speed_rating", 
            "horse_itm_percentage", 
            "perf_target", 
            "custom_speed_figure"
        ).show(30)
        logging.info(f"Top 5 finishers with moving up in class: {bottom_move_down}")
        
        # Save the Spark DataFrame as a Parquet file
        save_parquet(spark, spark_df, "speed_figure", "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet")
        logging.info("Ingestion job succeeded")
        
    # 3) Cleanup
    spark.stop()
    logging.info("Spark session stopped.")
    if db_pool:
        db_pool.closeall()
        logging.info("DB connection pool closed.")

if __name__ == "__main__":
    main()