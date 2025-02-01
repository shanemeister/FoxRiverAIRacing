import os
import sys
import time
import logging
import pprint
import pandas as pd
import argparse
import configparser
from psycopg2 import sql, pool, DatabaseError
from catboost import CatBoostRanker, Pool, CatBoostError
from src.data_preprocessing.data_prep1.data_utils import initialize_environment, save_parquet
"""
This code will take data loaded from a DataFrame and run its predictions against the models defined below
and then populate ensemble_average_results. The problem is that the group numbers are not guaranteed to
be unique across datasets, so it likely will not run unless you delete all records from the ensemble_average_results
table, and then run this program. 

Deleting the records from ensemble_average_results is not a problem because it can also be repopulated from catboost_enriched_results
with the calculate_ensemble_scode.py program. 
"""
def setup_logging():
    """Sets up logging configuration to write logs to a file and the console."""
    try:
        # Default log directory
        log_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs'
        
        # Ensure the log directory exists
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'inference_prep.log')

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
    config = configparser.ConfigParser()
    
    # Construct the absolute path to config.ini
    root_dir = os.path.abspath(os.path.join(script_dir, '../../'))
    config_file_path = os.path.join(root_dir, 'config.ini')
    
    logging.info(f"Reading configuration from {config_file_path}")
    
    if not os.path.exists(config_file_path):
        logging.error(f"Configuration file '{config_file_path}' does not exist.")
        raise FileNotFoundError(f"Configuration file '{config_file_path}' does not exist.")
    
    config.read(config_file_path)
    
    if 'database' not in config:
        logging.error("The 'database' section is missing in the configuration file.")
        logging.error(f"Available sections: {config.sections()}")
        raise KeyError("The 'database' section is missing in the configuration file.")
    
    return config

def get_db_pool(config):
    """Creates a connection pool to PostgreSQL."""
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
        
def prediction_data_prep(spark, horse_embedding, jdbc_url, jdbc_properties):
    """
    1. Loads `horse_embedding` Spark DF, converts to Pandas
    2. Splits data -> train/valid/holdout
    3. Marks certain columns as categorical (esp. `course_cd`)
    4. Builds CatBoost Pools, calling main_script(...) to do the actual training & writing
    """

    # Log the Spark schema
    logging.info(f"Schema of horse_embedding_df: {horse_embedding.schema}")
    logging.info(f"Horse_embedding count: {horse_embedding.count()}")

    # Convert Spark DF to Pandas
    df = horse_embedding.toPandas()

    # If present, drop "official_fin"
    df.drop(columns=["official_fin"], inplace=True, errors="ignore")

    logging.info(f"Columns in raw df: {df.columns.tolist()}")
    logging.info(f"Dtypes:\n{df.dtypes}")

    # Print initial data types and sample data
    logging.info("Initial DataFrame dtypes:")
    logging.info(df.dtypes)
    logging.info("Initial DataFrame sample:")
    logging.info(df.head())

    # Create a combined `race_id` + group_id for ordering
    df["race_id"] = (
        df["course_cd"].astype(str)
        + "_"
        + df["race_date"].astype(str)
        + "_"
        + df["race_number"].astype(str)
    )
    df["group_id"] = df["race_id"].astype("category").cat.codes
    df = df.sort_values("group_id", ascending=True).reset_index(drop=True)

    # Print data types after creating race_id and group_id
 
    # Convert selected datetime columns to numeric
    datetime_columns = ["first_race_date_5", "most_recent_race_5", "prev_race_date"]
    new_numeric_cols = {}
    for col in datetime_columns:
        df[col] = pd.to_datetime(df[col])
        new_numeric_cols[col + "_numeric"] = (df[col] - pd.Timestamp("1970-01-01")).dt.days

    df.drop(columns=datetime_columns, inplace=True, errors="ignore")
    df = pd.concat([df, pd.DataFrame(new_numeric_cols, index=df.index)], axis=1)

    # Convert main `race_date` to datetime so we can do date splits
    df["race_date"] = pd.to_datetime(df["race_date"])

    # The label column
    label_col = "perf_target"

    # Categorical features - we will cast them to "category" type
    cat_cols = [
        "course_cd", "trk_cond", "sex", "equip", "surface", "med",
        "race_type", "stk_clm_md", "turf_mud_mark", "layoff_cat",
        "previous_surface"
    ]

    # If you have some embedding columns
    embed_cols = [f"embed_{i}" for i in range(4)]

    # Exclude textual/ID columns
    excluded_cols = ["horse_name", "axciskey", "race_date", "race_number", "horse_id", "race_id", "track_name", label_col]

    # Force cat_cols to "category" in the main DataFrame
    for c in cat_cols:
        if c in df.columns:
            df[c] = df[c].astype("category")

    # Print data types after converting categorical columns
    logging.info("DataFrame dtypes after converting categorical columns:")
    logging.info(df.dtypes)
    logging.info("DataFrame sample after converting categorical columns:")
    logging.info(df.head())

    # Splits
    train_end_date = pd.to_datetime("2023-12-31")
    valid_data_end_date = pd.to_datetime("2024-06-30")
    holdout_start = pd.to_datetime("2024-07-01")

    train_data = df[df["race_date"] <= train_end_date].copy()
    valid_data = df[(df["race_date"] > train_end_date) & (df["race_date"] <= valid_data_end_date)].copy()
    holdout_data = df[df["race_date"] >= holdout_start].copy()

    # Verify uniqueness of horse_id and group_id
    def check_uniqueness(data, data_name):
        horse_id_unique = data["horse_id"].is_unique
        group_id_unique = data["group_id"].is_unique
        logging.info(f"{data_name} - horse_id unique: {horse_id_unique}, group_id unique: {group_id_unique}")
        if not horse_id_unique:
            logging.info(f"Duplicate horse_id in {data_name}:")
            logging.info(data[data.duplicated(subset=["horse_id"], keep=False)])
        if not group_id_unique:
            logging.info(f"Duplicate group_id in {data_name}:")
            logging.info(data[data.duplicated(subset=["group_id"], keep=False)])

    check_uniqueness(train_data, "train_data")
    check_uniqueness(valid_data, "valid_data")
    check_uniqueness(holdout_data, "holdout_data")
            
    # Verify uniqueness of horse_id and group_id combination
    def check_combination_uniqueness(data, data_name):
        combination_unique = data[["horse_id", "group_id"]].drop_duplicates().shape[0] == data.shape[0]
        logging.info(f"{data_name} - horse_id and group_id combination unique: {combination_unique}")
        if not combination_unique:
            logging.info(f"Duplicate horse_id and group_id combination in {data_name}:")
            logging.info(data[data.duplicated(subset=["horse_id", "group_id"], keep=False)])

    check_combination_uniqueness(holdout_data, "holdout_data")
    check_combination_uniqueness(train_data, "train_data")
    check_combination_uniqueness(valid_data, "valid_data")
    
    # Build the final feature list
    all_cols = df.columns.tolist()
    # Exclude text columns + label col
    base_feature_cols = [c for c in all_cols if c not in excluded_cols]
    # Force-include cat_cols + embed_cols
    final_feature_cols = list(set(base_feature_cols + cat_cols + embed_cols))
    final_feature_cols.sort()

    logging.info(f"Final feature columns for training:\n{final_feature_cols}")

    # Build X,y for train
    X_train = train_data[final_feature_cols].copy()
    y_train = train_data[label_col].values
    train_group_id = train_data["group_id"].values

    # Build X,y for valid
    X_valid = valid_data[final_feature_cols].copy()
    y_valid = valid_data[label_col].values
    valid_group_id = valid_data["group_id"].values

    # Build X,y for holdout
    X_holdout = holdout_data[final_feature_cols].copy()
    y_holdout = holdout_data[label_col].values
    holdout_group_id = holdout_data["group_id"].values

    logging.info(f"X_train shape: {X_train.shape}, y_train length: {len(y_train)}")
    logging.info(f"X_valid shape: {X_valid.shape}, y_valid length: {len(y_valid)}")
    logging.info(f"X_holdout shape: {X_holdout.shape}, y_holdout length: {len(y_holdout)}")

    # Print data types and sample data for each split after final feature selection
    logging.info("Train DataFrame dtypes after final feature selection:")
    logging.info(X_train.dtypes)
    logging.info("Train DataFrame sample after final feature selection:")
    logging.info(X_train.head())

    logging.info("Valid DataFrame dtypes after final feature selection:")
    logging.info(X_valid.dtypes)
    logging.info("Valid DataFrame sample after final feature selection:")
    logging.info(X_valid.head())

    logging.info("Holdout DataFrame dtypes after final feature selection:")
    logging.info(X_holdout.dtypes)
    logging.info("Holdout DataFrame sample after final feature selection:")
    logging.info(X_holdout.head())

    # Build cat_features_idx
    cat_features_idx = [X_train.columns.get_loc(c) for c in cat_cols if c in X_train.columns]

    logging.info(f"cat_features_idx: {cat_features_idx}")
    logging.info(f"cat_features: {[X_train.columns[i] for i in cat_features_idx]}")

    # Print the first few rows of X_holdout to identify the problematic column
    logging.info("X_holdout sample before creating CatBoost Pool:")
    logging.info(X_holdout.head())

    # Build CatBoost Pools
    train_pool = Pool(X_train, label=y_train, group_id=train_group_id, cat_features=cat_features_idx)
    valid_pool = Pool(X_valid, label=y_valid, group_id=valid_group_id, cat_features=cat_features_idx)
    holdout_pool = Pool(X_holdout, label=y_holdout, group_id=holdout_group_id, cat_features=cat_features_idx)

    # single final table
    db_table = "ensemble_average_results"
    model_path = {"model1A" : "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/catboost/catboost_YetiRank:top=4_NDCG:top=1_20250130_003113.cbm",
                  "model1B" : "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/catboost/catboost_YetiRank:top=3_NDCG:top=4_20250130_002454.cbm",
                  "model1C" : "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/catboost/catboost_YetiRank:top=4_NDCG:top=3_20250130_004503.cbm"
    }
    
    evaluate_existing_models_on_holdout(spark, holdout_data, cat_features_idx, final_feature_cols, model_path, jdbc_url, jdbc_properties, db_table)
    
def evaluate_existing_models_on_holdout(
    spark,
    holdout_data: pd.DataFrame,   # final holdout DF in Pandas
    cat_features_idx: list,       # from training
    final_feature_cols: list,     # same as training
    model_paths: dict,            # e.g. { "modelA": "/path/to/modelA.cbm", ... }
    db_url: str,
    db_properties: dict,
    db_table: str
):
    from catboost import CatBoostRanker, Pool
    
    # 1) Build holdout Pool
    X_holdout = holdout_data[final_feature_cols].copy()
    y_holdout = holdout_data["perf_target"].values
    holdout_group_id = holdout_data["group_id"].values

    holdout_pool = Pool(X_holdout, label=y_holdout, group_id=holdout_group_id, cat_features=cat_features_idx)

    # 2) For each model, load & predict
    for model_key, path in model_paths.items():
        model = CatBoostRanker()
        model.load_model(path)

        preds = model.predict(holdout_pool)

        # 3) Build a Pandas DF
        df_out = pd.DataFrame({
            "model_key": model_key,
            "group_id": holdout_group_id,
            "horse_id": holdout_data["horse_id"].values,
            "prediction": preds,
            "true_label": y_holdout
        })

        # Merge in race_date, course_cd, etc. if you want them
        df_out = df_out.merge(holdout_data, on=["group_id", "horse_id"], how="left")

        # 4) Sort & rank by predictions if you want:
        df_out = df_out.sort_values(by=["group_id","prediction"], ascending=[True,False])
        df_out["rank"] = df_out.groupby("group_id").cumcount() + 1

        # 5) Write to DB
        spark_df = spark.createDataFrame(df_out)
        (
            spark_df.write
            .format("jdbc")
            .option("url", db_url)
            .option("dbtable", db_table)
            .option("user", db_properties["user"])
            .option("driver", db_properties["driver"])
            .mode("append")
            .save()
        )
        print(f"Wrote holdout predictions for {model_key} to {db_table}")
        
def main():
    """Main function to execute data ingestion tasks."""
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = read_config(script_dir)

    # 1) Create DB pool
    db_pool = get_db_pool(config)
    conn = db_pool.getconn()
    
    # Determine the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Script_dir: {script_dir}")

    spark = None
    try:
        spark, jdbc_url, jdbc_properties, parquet_dir, log_file = initialize_environment()
        spark.catalog.clearCache()
        setup_logging()     
        time_start = time.time()       
        # Load Horse Embedding data
        horse_embedding = spark.read.parquet("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/horse_embedding_data-20250128_1240.parquet")
        prediction_data_prep(spark, horse_embedding, jdbc_url, jdbc_properties)    
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}")

    finally:
        if spark:
            spark.stop()
            logging.info("Spark session stopped.")

if __name__ == "__main__":
    main()