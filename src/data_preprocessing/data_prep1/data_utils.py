import os
import logging
from pyspark.sql.functions import (col, count, abs, unix_timestamp, when, lit, min as F_min, max as F_max , 
                                   mean, countDistinct, expr, datediff, when, trim, udf,
                                   isnull, avg as F_avg, upper, length, collect_list, row_number, radians, sin, cos, sqrt, atan2)
import configparser
import math
from pyspark.sql import SparkSession
from src.data_preprocessing.data_prep1.sql_queries import sql_queries
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.ml import Pipeline
from pyspark.sql.types import StringType, DoubleType, IntegerType
from sedona.utils import KryoSerializer, SedonaKryoRegistrator
from sedona.register import SedonaRegistrator
import pandas as pd
import numpy as np
from pyspark.ml.feature import StringIndexer, OneHotEncoder, MinMaxScaler, StandardScaler, VectorAssembler
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window
from sedona.spark import SedonaContext

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points
    on the Earth specified in decimal degrees.
    """
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None

    R = 6371.0  # Earth radius in kilometers

    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance

haversine_udf = udf(haversine, DoubleType())

def save_parquet(spark, df, name, parquet_dir):
    """
    Save a PySpark DataFrame as a Parquet file.
    
    :param df: PySpark DataFrame to save
    :param name: Name of the DataFrame (used for the file name)
    :param parquet_dir: Directory to save the Parquet file
    :return: None
    """
    output_path = os.path.join(parquet_dir, f"{name}.parquet")
    logging.info(f"Saving {name} DataFrame to Parquet at {output_path}...")
    logging.info(f"Schema of {name} DataFrame:")
    # df.printSchema()
    df.write.mode("overwrite").parquet(output_path)  
    spark.catalog.clearCache()
    logging.info(f"{name} DataFrame saved successfully.")
    return None


def gather_statistics(df, df_name):
    """
    Gather and print statistics for a given DataFrame.
    
    Parameters:
    df (DataFrame): The DataFrame to gather statistics for
    df_name (str): The name of the DataFrame (for logging purposes)
    """
    # Total number of rows
    total_rows = df.count()
    logging.info(f"Total number of rows in {df_name}: {total_rows}")
    
    # Number of columns
    total_columns = len(df.columns)
    logging.info(f"Total number of columns in {df_name}: {total_columns}")
    
    # Schema information
    logging.info(f"Schema of {df_name}:")
    df.printSchema()
    
    primary_keys = [
    ["course_cd", "race_date", "race_number", "saddle_cloth_number", "sec_time_stamp"],
    ["course_cd", "race_date", "race_number", "saddle_cloth_number", "time_stamp"],
    ["course_cd", "race_date", "race_number", "saddle_cloth_number", "sec_time_stamp", "time_stamp"]
]

    for cols in primary_keys:
        # If the key set includes sec_time_stamp as the only timestamp field
        # and is intended to be unique only for non-null sec_time_stamp,
        # we need to filter out rows with null sec_time_stamp.
        if cols == ["course_cd", "race_date", "race_number", "saddle_cloth_number", "sec_time_stamp"]:
            duplicates_df = df.filter(col("sec_time_stamp").isNotNull()) \
                            .groupBy(*cols).agg(count("*").alias("count")) \
                            .filter(col("count") > 1)
        else:
            # For keys that include time_stamp or include both sec_time_stamp and time_stamp,
            # use the full DataFrame as is
            duplicates_df = df.groupBy(*cols).agg(count("*").alias("count")).filter(col("count") > 1)
        
        num_duplicates = duplicates_df.count()
        logging.info(f"Number of duplicate rows in {df_name} based on {cols}: {num_duplicates}")
    
    # Identify and print duplicates where sec_time_stamp maps to more than one gps time_stamp
    gps_time_stamp_duplicates = df.groupBy("sec_time_stamp").agg(count("time_stamp").alias("count")).filter(col("count") > 1)
    num_gps_time_stamp_duplicates = gps_time_stamp_duplicates.count()
    logging.info(f"Number of sec_time_stamp mapping to multiple gps time_stamp: {num_gps_time_stamp_duplicates}")
    
    if num_gps_time_stamp_duplicates > 0:
        logging.info("Sample of sec_time_stamp mapping to multiple gps time_stamp:")
        gps_time_stamp_duplicates.show(10, truncate=False)
    
    # Summary statistics
    logging.info(f"Summary statistics for {df_name}:")
    #df.describe().show()
    
def drop_duplicates_with_tolerance(df, tolerance=0.5):
    # Separate null and non-null sec_time_stamp rows
    df_null_sec = df.filter(col("sec_time_stamp").isNull())
    df_non_null = df.filter(col("sec_time_stamp").isNotNull())
    
    # Compute time_diff and filter by tolerance
    df_non_null = df_non_null.withColumn(
        "time_diff", abs(unix_timestamp("sec_time_stamp") - unix_timestamp("time_stamp"))
    )
    
    df_non_null = df_non_null.filter(col("time_diff") <= tolerance)
    
    # Window specification: pick the best match (lowest time_diff, then earliest time_stamp)
    window_spec = Window.partitionBy("course_cd", "race_date", "race_number", "saddle_cloth_number", "sec_time_stamp") \
                       .orderBy(col("time_diff").asc(), col("time_stamp").asc())
    
    df_non_null = df_non_null.withColumn("row_number", row_number().over(window_spec)) \
                             .filter(col("row_number") == 1) \
                             .drop("time_diff", "row_number")
    
    # Combine non-null processed rows with null sec_time_stamp rows
    df_result = df_non_null.unionByName(df_null_sec)
    
    # FINAL STEP: Ensure uniqueness at (course_cd, race_date, race_number, saddle_cloth_number, time_stamp)
    # Add a flag column: 1 if sec_time_stamp is not null, 0 if null
    df_result = df_result.withColumn("sec_ts_flag", (col("sec_time_stamp").isNotNull()).cast("integer"))

    final_window = Window.partitionBy("course_cd", "race_date", "race_number", "saddle_cloth_number", "time_stamp") \
                         .orderBy(col("sec_ts_flag").desc())

    df_result = df_result.withColumn("final_row_number", row_number().over(final_window)) \
                         .filter(col("final_row_number") == 1) \
                         .drop("sec_ts_flag", "final_row_number")
    
    return df_result

def initialize_environment():
    # Paths and configurations
    config_path = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/config.ini'
    log_file = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/SparkPy_load.log"
    jdbc_driver_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/jdbc/postgresql-42.7.4.jar"
    parquet_dir = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/"
    os.makedirs(parquet_dir, exist_ok=True)
    
    # Clear the log file by opening it in write mode
    with open(log_file, 'w'):
        pass  # This will truncate the file without writing anything
    
    # Load configuration
    config = load_config(config_path)

    # Database credentials from config
    db_host = config['database']['host']
    db_port = config['database']['port']
    db_name = config['database']['dbname']
    db_user = config['database']['user']
    # db_password = os.getenv("DB_PASSWORD", "sdfsdfhhjgfj!")  # Ensure DB_PASSWORD is set

    # Validate database password
    #if not db_password:
    #    raise ValueError("Database password is missing. Set it in the DB_PASSWORD environment variable.")

    # JDBC URL and properties
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    jdbc_properties = {
        "user": db_user,
        #"password": db_password,
        "driver": "org.postgresql.Driver"
    }
    
    # Initialize Spark session
    jdbc_driver_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/jdbc/postgresql-42.7.4.jar"
    sedona_jar_abs_path = "/home/exx/sedona/apache-sedona-1.7.0-bin/sedona-spark-shaded-3.4_2.12-1.7.0.jar"
    
    # # Paths to GeoTools JAR files
    # geotools_jar_paths = [
    #     "/home/exx/anaconda3/envs/mamba_env/envs/tf_310/lib/python3.10/site-packages/pyspark/jars/geotools-wrapper-1.1.0-25.2.jar",
    #     "/home/exx/anaconda3/envs/mamba_env/envs/tf_310/lib/python3.10/site-packages/pyspark/jars/sedona-python-adapter-3.0_2.12-1.2.0-incubating.jar",
    #     "/home/exx/anaconda3/envs/mamba_env/envs/tf_310/lib/python3.10/site-packages/pyspark/jars/sedona-viz-3.0_2.12-1.2.0-incubating.jar",
    # ]
    
    # Initialize logging
    initialize_logging(log_file)
    queries = sql_queries()
    # Initialize Spark session
    
    
    # input("Press Enter to continue... getting ready to initialize spark")
    
    spark = initialize_spark(jdbc_driver_path)
    return spark, jdbc_url, jdbc_properties, queries, parquet_dir, log_file

def load_config(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

def initialize_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logging.info("Environment setup initialized.")

# Initialize Spark session
from sedona.spark import SedonaContext

def initialize_spark(jdbc_driver_path):
    """
    Initializes Spark without Sedona and GeoTools.
    """
    try:
        spark = (
            SparkSession.builder
            .appName("Horse Racing Data Processing")
            .master("local[*]")
            .config("spark.driver.memory", "64g")
            .config("spark.executor.memory", "32g")
            .config("spark.executor.memoryOverhead", "8g")
            .config("spark.sql.debug.maxToStringFields", "1000")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
            .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
            .config("spark.jars", jdbc_driver_path)
            .getOrCreate()
        )

        # Optional: reduce verbosity
        spark.sparkContext.setLogLevel("ERROR")

        print("Spark session created successfully.")
        return spark

    except Exception as e:
        print(f"An error occurred during Spark initialization: {e}")
        logging.error(f"An error occurred during Spark initialization: {e}")
        return None


def identify_and_remove_outliers(df, column):
    """
    Identify and remove outliers in a DataFrame column using the IQR method.
    
    Parameters:
    df (DataFrame): The input DataFrame
    column (str): The column name to check for outliers
    
    Returns:
    DataFrame: DataFrame with outliers removed
    """
    # Calculate Q1 and Q3
    quantiles = df.approxQuantile(column, [0.25, 0.75], 0.05)
    Q1 = quantiles[0]
    Q3 = quantiles[1]
    
    # Calculate IQR
    IQR = Q3 - Q1
    
    # Determine outlier boundaries
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    # Filter outliers
    df_no_outliers = df.filter((col(column) >= lower_bound) & (col(column) <= upper_bound))
    
    return df_no_outliers

def identify_and_impute_outliers(df: DataFrame, column: str, tolerance=0.05) -> DataFrame:
    """
    Identify and impute missing and outlier values in a DataFrame column using the IQR method.
    For missing values, impute with the average stride_frequency for that race.
    For outliers, cap them at the lower or upper bound.
    """
    # Calculate Q1 and Q3 using approxQuantile
    quantiles = df.approxQuantile(column, [0.25, 0.75], tolerance)
    Q1, Q3 = quantiles
    IQR = Q3 - Q1

    # Determine outlier boundaries
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # Log calculated bounds
    logging.info(f"Column: {column}, Q1: {Q1}, Q3: {Q3}, IQR: {IQR}, Lower Bound: {lower_bound}, Upper Bound: {upper_bound}")

    # Create a window to compute race-level mean stride_frequency (exclude saddle_cloth_number for race-level mean)
    race_window = Window.partitionBy("course_cd", "race_date", "race_number")

    # Compute the mean stride_frequency per race
    df = df.withColumn("race_mean_stride_freq", mean(col(column)).over(race_window))

    # Step 1: Fallback to the global mean of non-outlier, non-missing values
    non_outlier_mean = df.filter((col(column).isNotNull()) & (col(column) >= lower_bound) & (col(column) <= upper_bound)) \
                         .agg(mean(col(column)).alias("non_outlier_mean")) \
                         .first()["non_outlier_mean"]
    
    if non_outlier_mean is None:
        non_outlier_mean = (Q1 + Q3) / 2  # Fallback to midrange if no valid global mean
        logging.warning(f"Global mean for non-outliers could not be calculated. Falling back to {non_outlier_mean}.")

    # If race_mean_stride_freq is null, use non_outlier_mean
    df = df.withColumn("imputed_value",
        when(col("race_mean_stride_freq").isNull(), non_outlier_mean).otherwise(col("race_mean_stride_freq"))
    )

    # Impute missing stride_frequency
    df = df.withColumn(column,
        when(col(column).isNull(), col("imputed_value")).otherwise(col(column))
    )
    print(f"Imputed values for {column}: ***************************************************************")
    df.select("course_cd", "race_date", "race_number", "imputed_value", "stride_frequency", column).show(20, truncate=False)
    
    # Step 2: Cap outliers at lower_bound or upper_bound
    df = df.withColumn(column,
        when(col(column) < lower_bound, lower_bound)
        .when(col(column) > upper_bound, upper_bound)
        .otherwise(col(column))
    )
    
    print(f"Imputed values for {column}: ##################################################################")
    
    df.select("course_cd", "race_date", "race_number", "imputed_value", "stride_frequency", column).show(20, truncate=False)
    
    # Log changes
    logging.info(f"Missing values and outliers in {column} have been imputed and capped.")

    # Clean up temporary columns
    df = df.drop("race_mean_stride_freq", "imputed_value")
    df.select("course_cd", "race_date", "race_number", column).show(20, truncate=False)
    return df


def identify_missing_and_outliers(spark, parquet_dir, df, cols):
    """
    Load matched_df and identify missing and outlier data.
    
    Parameters:
    cols (list): List of columns to check for outliers and missing data
    """

    gather_statistics(df, "your_df")
    
    # Identify missing and outlier data for specific columns
    for column in cols:
        missing_count = df.filter(col(column).isNull()).count()
        logging.info(f"Number of missing values in {column}: {missing_count}")
        
        outliers_df = identify_outliers(df, column)
        num_outliers = outliers_df.count()
        logging.info(f"Number of outliers in {column}: {num_outliers}")
        if num_outliers > 0:
            logging.info(f"Sample of outliers in {column}:")
            #outliers_df.select(cols).show(10, truncate=False)

def identify_outliers(df: DataFrame, column: str) -> DataFrame:
    """
    Identify outliers in a DataFrame column using the IQR method.
    
    Parameters:
    df (DataFrame): The input DataFrame
    column (str): The column name to check for outliers
    
    Returns:
    DataFrame: DataFrame containing the outliers
    """
    # Calculate Q1 and Q3
    quantiles = df.approxQuantile(column, [0.25, 0.75], 0.05)
    Q1 = quantiles[0]
    Q3 = quantiles[1]
    
    # Calculate IQR
    IQR = Q3 - Q1
    
    # Determine outlier boundaries
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    print(f"Q1: {Q1}, Q3: {Q3}, IQR: {IQR}")
    print(f"Lower Bound: {lower_bound}, Upper Bound: {upper_bound}")

    # Filter outliers
    outliers_df = df.filter((col(column) < lower_bound) | (col(column) > upper_bound))
    
    return outliers_df

def detect_cardinality_columns(df, threshold, cardinality_type):
    """
    Detects and prints columns in a DataFrame based on their cardinality.
    
    :param df: PySpark DataFrame to analyze.
    :param threshold: Cardinality threshold to classify columns.
    :param cardinality_type: Either "high", "low", or "both" types.
    :return: Dictionary containing high and low cardinality columns.
    """
    cardinality = None
    high_cardinality_columns = []
    low_cardinality_columns = []
    for col_name in df.columns:
        cardinality = df.select(countDistinct(col_name).alias("cardinality")).collect()[0]["cardinality"]
        # Categorize the column based on the threshold
        if cardinality > threshold:
            high_cardinality_columns.append((col_name, cardinality))
        else:
            low_cardinality_columns.append((col_name, cardinality))

    # Filter results based on the cardinality_type argument
    if cardinality_type == "high":
        print("\n--- High Cardinality Columns ---")
        for col_name, cardinality in high_cardinality_columns:
            print(f"Column: {col_name}, Cardinality: {cardinality}")
    elif cardinality_type == "low":
        print("\n--- Low Cardinality Columns ---")
        for col_name, cardinality in low_cardinality_columns:
            print(f"Column: {col_name}, Cardinality: {cardinality}")
    elif cardinality_type == "both":
        # Display both types if no specific type is requested
        print("\n--- High Cardinality Columns ---")
        for col_name, cardinality in high_cardinality_columns:
            print(f"Column: {col_name}, Cardinality: {cardinality}")
        if not high_cardinality_columns:
            print("No high cardinality columns found.")
        
        print("\n--- Low Cardinality Columns ---")
        for col_name, cardinality in low_cardinality_columns:
            print(f"Column: {col_name}, Cardinality: {cardinality}")
        if not low_cardinality_columns:
            print("No low cardinality columns found.")
    else:
        raise ValueError("Invalid cardinality_type. Use 'high', 'low', or 'both'.")

    # Return a dictionary for further processing
    return 

def drop_unnecessary_columns(df: DataFrame, additional_columns_to_keep: list = None) -> DataFrame:
    """
    Drops unnecessary columns from a DataFrame, retaining essential ones like OHE, features, labels, and sequences.

    Parameters:
    -----------
    df                       : DataFrame, input Spark DataFrame.
    additional_columns_to_keep: list, additional columns to retain beyond the essential ones (optional).

    Returns:
    --------
    DataFrame: A new DataFrame with only the specified columns retained.
    """
    # Define essential columns dynamically
    essential_columns = [
        col for col in df.columns 
        if col.endswith("_ohe") or col in ["features", "label", "scaled_features", "sequence"]
    ]

    # Include additional columns specified by the user
    if additional_columns_to_keep:
        essential_columns.extend(additional_columns_to_keep)

    # Ensure the list is unique
    essential_columns = list(set(essential_columns))

    # Identify columns to drop
    columns_to_drop = [col for col in df.columns if col not in essential_columns]

    # print(f"Dropping {len(columns_to_drop)} unnecessary columns.")
    # print("Columns being dropped:", columns_to_drop)

    # Drop columns
    df_cleaned = df.drop(*columns_to_drop)
    return df_cleaned

def split_train_val_data(df):
    # 1. Split Data by Date
    train_cutoff = "2023-12-31"
    val_cutoff = "2024-06-30"

    train_df = df.filter(F.col("race_date") <= F.lit(train_cutoff))
    val_df = df.filter((F.col("race_date") > F.lit(train_cutoff)) & (F.col("race_date") <= F.lit(val_cutoff)))
    test_df = df.filter(F.col("race_date") > F.lit(val_cutoff))

    print(f"Train: {train_df.count()}, Validation: {val_df.count()}, Test: {test_df.count()}")
    
    assert train_df.count() > 0, "Training set is empty."
    assert val_df.count() > 0, "Validation set is empty."
    assert test_df.count() > 0, "Test set is empty."

    return train_df, val_df, test_df

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, when, lit, collect_list, size
from pyspark.ml.feature import VectorAssembler, MinMaxScaler, StandardScaler

def prepare_lstm_data(train_df, val_df, test_df):
    """
    Prepares data for LSTM training, validation, and testing with embedded features.
    
    Args:
        spark (SparkSession): The Spark session.
        df (Sorted DataFrame): The input DataFrame.
        train_df: Train data split using split_train_val_data function.
        val_df: Validation data using split_train_val_data function.
        test_df: Test data set using split_train_val_data function.
        
    Returns:
        train_df, val_df, test_df: DataFrames with embedded features and sequences.
    """

    # Step 1: Define numeric columns and OHE columns
    numeric_cols = [
        "morn_odds", "gate_index", "age_at_race_day", "purse", "weight", 
        "start_position", "claimprice", "power", "avgspd", "class_rating", 
        "net_sentiment", "all_earnings", "cond_earnings", "avg_spd_sd", 
        "ave_cl_sd", "hi_spd_sd", "pstyerl", "all_starts", "all_win", 
        "all_place", "all_show", "all_fourth", "cond_starts", "cond_win", 
        "cond_place", "cond_show", "cond_fourth", "sectionals_length_to_finish",
        "sectionals_sectional_time", "sectionals_running_time", "sectionals_distance_back",
        "sectionals_distance_ran", "sectionals_number_of_strides", "cumulative_sectional_time",
        "gps_section_avg_speed", "gps_section_avg_stride_freq", "gps_first_progress", 
        "gps_last_progress", "distance_meters", "prev_speed", "time_diff_s", 
        "acceleration_m_s2", "max_speed_overall", "min_speed_overall", 
        "is_fastest_gate", "is_slowest_gate", "final_speed", "fatigue_factor", 
        "actual_distance_run_m", "acceleration_m_s2_was_missing",
        "gps_section_avg_stride_freq_was_missing", "prev_speed_was_missing",
        "sectionals_distance_back_was_missing", "sectionals_number_of_strides_was_missing",
        "jock_key_index", "train_key_index"
    ]

    # Step 3: Scale Features (Train Only)
    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="raw_features")
    train_assembled = assembler.transform(train_df)
    
    scaler = MinMaxScaler(inputCol="raw_features", outputCol="scaled_features")
    scaler_model = scaler.fit(train_assembled)
    
    # Apply scaling to train, val, test
    train_scaled = scaler_model.transform(train_assembled).drop("raw_features")
    val_scaled = scaler_model.transform(assembler.transform(val_df)).drop("raw_features")
    test_scaled = scaler_model.transform(assembler.transform(test_df)).drop("raw_features")

    return train_scaled, val_scaled, test_scaled

def create_sequences(df: DataFrame, seq_len: int) -> DataFrame:
    """
    Create sequences using a sliding window approach.
    
    :param df: Input DataFrame
    :param seq_len: Length of the sequence
    :return: DataFrame with sequences
    """
    w = Window.partitionBy("horse_id").orderBy("race_date", "gate_index").rowsBetween(-(seq_len - 1), 0)
    df = df.withColumn("sequence", collect_list("scaled_features").over(w))
    return df.filter(size(col("sequence")) == seq_len)

def flatten_sequence_column(
    df: DataFrame,
    sequence_col: str = "sequence",
    partition_cols: list = None,
    order_by_cols: list = None
) -> DataFrame:
    """
    Flattens (explodes) a Spark DataFrame that contains a 'sequence' column
    (an array of vectors) into one row per time step.
    
    :param df: Spark DataFrame that has a column with array<STRUCT> or array<VECTOR>.
    :param sequence_col: The name of the array column you want to flatten (default 'sequence').
    :param partition_cols: List of columns identifying the entity (e.g. race_id, horse_id).
                          This is optional, but helps clarify the ordering.
    :param order_by_cols:  Additional columns that define the order within each partition.
                          E.g. ["race_date", "gate_index"].
    :return: A new DataFrame with the exploded sequence so that each row
             corresponds to exactly one time step from 'sequence'.
             
             - The flattened time-step index will appear in the column 'time_step_index'
             - The single time-step array element will appear in the column 'time_step_features'
             - The original 'sequence' column is dropped.
    """
    if partition_cols is None:
        partition_cols = []
    if order_by_cols is None:
        order_by_cols = []
    
    # 1) Sort your DataFrame if you want a guaranteed ordering
    #    (Spark doesn't guarantee ordering unless you specify it).
    df_ordered = df.orderBy(*partition_cols, *order_by_cols)
    
    # 2) Use posexplode to get both index (time_step_index) and the item (time_step_features)
    flattened_df = df_ordered.select(
        # keep all columns except the 'sequence' one
        *[c for c in df_ordered.columns if c != sequence_col],
        F.posexplode(sequence_col).alias("time_step_index", "time_step_features")
    )
    
    # 3) (Optional) Drop the original sequence column if it still exists
    #    (After posexplode, it’s typically not needed.)
    return flattened_df

def process_merged_results_sectionals(spark, df, parquet_dir):
    """
    Process merged results and sectionals DataFrame to remove duplicates, impute missing values,
    convert data types, OHE, and gather statistics. Retains horse_id for embedding.
    
    Parameters:
    -----------
    spark      : SparkSession
    df         : DataFrame, the merged DataFrame to process
    parquet_dir: str, directory to save parquet data if needed
    
    Returns:
    --------
    DataFrame: The processed DataFrame with final 'features' and 'label' columns,
               plus 'horse_id' retained for embedding.
    """
    # 1) Check for duplicates ================================================================================
    primary_keys = ["course_cd", "race_date", "race_number", "horse_id", "gate_index"]

    duplicates = (
        df.groupBy(*primary_keys)
          .agg(count("*").alias("cnt"))
          .filter(col("cnt") > 1)
    )
    dup_count = duplicates.count()
    
    if dup_count > 0:
        print(f"Found {dup_count} duplicate primary key combinations.")
        duplicates.show()
        raise ValueError(f"Duplicates found: {dup_count}. Dedup function needed.")
    else:
        print("No duplicates found.")
    
    print("1. Duplicates checked.")
    
    # 2) Convert decimal columns to double ===================================================================
    decimal_cols = ["weight", "power", "morn_odds", "all_earnings", "cond_earnings"]
    for col_name in decimal_cols:
        df = df.withColumn(col_name, col(col_name).cast("double"))
    print("2. Decimal columns converted to double.")
    
    # 3) Impute missing values ================================================================================
    # 3a) Impute date_of_birth with the median date_of_birth
    df = df.withColumn("date_of_birth_ts", col("date_of_birth").cast("timestamp").cast("long"))
    
    median_window = Window.orderBy("date_of_birth_ts")
    row_count = df.filter(col("date_of_birth_ts").isNotNull()).count()

    if row_count > 0:
        if row_count % 2 == 0:  # Even
            median_row_1 = row_count // 2
            median_row_2 = median_row_1 + 1
            median_ts = (
                df.filter(col("date_of_birth_ts").isNotNull())
                  .select("date_of_birth_ts")
                  .withColumn("row_num", row_number().over(median_window))
                  .filter((col("row_num") == median_row_1) | (col("row_num") == median_row_2))
                  .groupBy()
                  .agg(expr("avg(date_of_birth_ts) as median_ts"))
                  .collect()[0]["median_ts"]
            )
        else:  # Odd
            median_row = (row_count + 1) // 2
            median_ts = (
                df.filter(col("date_of_birth_ts").isNotNull())
                  .select("date_of_birth_ts")
                  .withColumn("row_num", row_number().over(median_window))
                  .filter(col("row_num") == median_row)
                  .collect()[0]["date_of_birth_ts"]
            )
        median_date = lit(expr(f"CAST(FROM_UNIXTIME({median_ts}) AS DATE)"))
    else:
        # If no valid date_of_birth at all, fallback to some default
        median_date = lit(expr("CAST('2000-01-01' AS DATE)"))

    df = df.withColumn(
        "date_of_birth",
        when(col("date_of_birth").isNull(), median_date).otherwise(col("date_of_birth"))
    ).drop("date_of_birth_ts")
    print("3a. Missing date_of_birth values imputed with median date.")
    
    # 3b) Convert date_of_birth to age in years
    df = df.withColumn("date_of_birth", col("date_of_birth").cast("date"))
    df = df.withColumn("race_date", col("race_date").cast("date"))
    df = df.withColumn(
        "age_at_race_day",
        datediff(col("race_date"), col("date_of_birth")) / 365.25
    )
    print("3b. Created age_at_race_day from date_of_birth.")
    
    # 3c) Impute missing weather with 'UNKNOWN'
    df = df.fillna({"weather": "UNKNOWN"})
    print("3c. weather -> UNKNOWN where missing.")
    
    # 3d) Impute missing wps_pool with mean
    mean_value = df.select(mean(col("wps_pool")).alias("mean_wps_pool")).collect()[0]["mean_wps_pool"]
    df = df.withColumn("wps_pool", when(col("wps_pool").isNull(), mean_value).otherwise(col("wps_pool")))
    print("3d. wps_pool -> mean where missing.")

    # 3e) Impute equip with 'No_Equip'
    df = df.withColumn(
        "med",
        when(length(trim(col("med"))) == 0, lit("MISSING"))
        .otherwise(col("med"))
    )
    
    # 3f) Impute trk_cond/trk_cond_desc with 'MISSING'
    df = df.withColumn(
        "turf_mud_mark",
        when(length(trim(col("turf_mud_mark"))) == 0, lit("MISSING"))
        .otherwise(col("turf_mud_mark"))
    )

    # 3g) Numeric columns: add missing flags + fill with 0
    numeric_impute_cols = ["acceleration_m_s2", "gps_section_avg_stride_freq", "prev_speed", "sectionals_distance_back", "sectionals_number_of_strides"]
    for c in numeric_impute_cols:
        df = df.withColumn(f"{c}_was_missing", when(col(c).isNull(), lit(1)).otherwise(lit(0)))
    df = df.fillna({col_: 0.0 for col_ in numeric_impute_cols})
    print("Imputed numeric columns with 0 and added missing flags.")
    
    # 4) Create label column ================================================================================
    # Example: multi-class finish bracket (0=win,1=place,2=show,3=fourth,4=outside top4)
    df = df.withColumn(
        "label",
        when(col("official_fin") == 1, lit(0))      # Win
        .when(col("official_fin") == 2, lit(1))     # Place
        .when(col("official_fin") == 3, lit(2))     # Show
        .when(col("official_fin") == 4, lit(3))     # Fourth
        .otherwise(lit(4))                          # Outside top-4
    )
    print("4. Created label column (multi-class based on official_fin).")
    
    # 5) One-Hot Encode (and StringIndex) the relevant categorical columns ===================================
    categorical_cols = ["course_cd", "equip", "surface", "trk_cond", "weather", "med", "stk_clm_md", "turf_mud_mark", "race_type" ]
    # Build indexers and encoders for categorical columns
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid="keep")
        for c in categorical_cols
    ]
    encoders = [
        OneHotEncoder(inputCols=[f"{c}_index"], outputCols=[f"{c}_ohe"])
        for c in categorical_cols
    ]

    # Handle jock_key and train_key
    jock_indexer = StringIndexer(inputCol="jock_key", outputCol="jock_key_index", handleInvalid="keep")
    train_indexer = StringIndexer(inputCol="train_key", outputCol="train_key_index", handleInvalid="keep")

    # Combine all stages into a pipeline
    preprocessing_stages = [jock_indexer, train_indexer] + indexers + encoders
    pipeline = Pipeline(stages=preprocessing_stages)

    # Fit and transform the pipeline
    model = pipeline.fit(df)
    df_transformed = model.transform(df)
    print("6. Completed StringIndex + OneHotEncode for categorical columns.")

    # 6A - Drop original categorical columns
    df_transformed = df_transformed.drop(*categorical_cols)
    print("6A. Dropped original categorical columns.")
    
    #6B - Drop jock_key and train_key
    df_transformed = df_transformed.drop("jock_key", "train_key")
    print("6B. Dropped jock_key and train_key.")
    
    # 6C - Assign embedding column
    embedding_col = "horse_id"
    # Check if the embedding column already exists
    if embedding_col not in df_transformed.columns:
        df_transformed = df_transformed.select("*", embedding_col)
    print(f"6C. Assigned embedding column: {embedding_col}")
    
    
    # 7 - Setup Strict Time-based Train/Test Sorting and Split Data ===========================================
    # Sort the dataset for sequencing
    sorted_df = df_transformed.orderBy(["course_cd", "race_date", "race_number", "horse_id", "gate_index"])
    
    train_df, val_df, test_df = split_train_val_data(sorted_df)
    # input("Press Enter to continue...7")
    
    # 8. Scale Features on Training Set =======================================================================
    train_scaled, val_scaled, test_scaled = prepare_lstm_data(train_df, val_df, test_df)
    # input("Press Enter to continue...8")
    
    #9 - Sequence Creation: Use create_sequence function to create sequences of length 10 ======================
    sequence_length = 10

    # Create sequences
    train_sequences = create_sequences(train_scaled, sequence_length)
    print("Train Sequences: ", train_sequences.filter(size(col("sequence")) != 10).count())
    val_sequences = create_sequences(val_scaled, sequence_length)
    print("Validation Sequences: ", val_sequences.filter(size(col("sequence")) != 10).count())
    test_sequences = create_sequences(test_scaled, sequence_length)
    print("Test Sequences: ", test_sequences.filter(size(col("sequence")) != 10).count())

    print(f"Train Sequences: {train_sequences.count()}, Validation Sequences: {val_sequences.count()}, Test Sequences: {test_sequences.count()}")

    # Retain Important Columns
    columns_to_keep = [
        "race_date", "race_number", "horse_id", "gate_index", "label", 
        "scaled_features", "features", "sequence"
    ]

    train_sequences = drop_unnecessary_columns(train_sequences, columns_to_keep)
    val_sequences = drop_unnecessary_columns(val_sequences, columns_to_keep)
    test_sequences = drop_unnecessary_columns(test_sequences, columns_to_keep)

    # Flatten sequences
    train_flat = flatten_sequence_column(
        df=train_sequences,
        sequence_col="sequence", 
        partition_cols=["course_cd", "race_date", "race_number", "horse_id", "gate_index"],  # or whatever uniquely identifies an entity
        order_by_cols=["gate_index"]
    )

    val_flat = flatten_sequence_column(
        df=val_sequences,
        sequence_col="sequence", 
        partition_cols=["course_cd", "race_date", "race_number", "horse_id", "gate_index"],  # or whatever uniquely identifies an entity
        order_by_cols=["gate_index"]
    )

    test_flat = flatten_sequence_column(
        df=test_sequences,
        sequence_col="sequence", 
        partition_cols=["course_cd", "race_date", "race_number", "horse_id", "gate_index"],  # or whatever uniquely identifies an entity
        order_by_cols=["gate_index"]
    )

    # Write to parquet
    train_flat.write.parquet(f"{parquet_dir}/train_flat.parquet", mode="overwrite")
    val_flat.write.parquet(f"{parquet_dir}/val_flat.parquet", mode="overwrite")
    test_flat.write.parquet(f"{parquet_dir}/test_flat.parquet", mode="overwrite")

    sorted_df.printSchema()
    # input("Press Enter to continue...12")
    print("Data preparation complete. Train, Validation, and Test datasets saved.")
    final_df = drop_unnecessary_columns(sorted_df, columns_to_keep)
    return final_df