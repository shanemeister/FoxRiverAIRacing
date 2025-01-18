import os
import logging
from pyspark.sql.functions import (col, count, abs, unix_timestamp, when, lit, min as F_min, max as F_max , 
                                   mean, countDistinct, expr, datediff, when, trim, udf, array_repeat, array, size, 
                                   isnull, avg as F_avg, upper, length, collect_list, row_number, struct, radians, sin, cos, sqrt, atan2)
import configparser
import math
from pyspark.sql import SparkSession
from src.data_preprocessing.data_prep1.sql_queries import sql_queries
from pyspark.ml import Pipeline
from pyspark.sql.types import StringType, DoubleType, IntegerType, ArrayType
import pandas as pd
import numpy as np
from pyspark.ml.feature import StringIndexer, OneHotEncoder, MinMaxScaler, StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, Window
from pyspark.ml.linalg import VectorUDT
import pyspark.sql.functions as F
from pyspark.ml.functions import vector_to_array
from datetime import datetime

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

def convert_processed_spark_to_pandas(spark, parquet_dir):
    """
    Reads the final 'processed_data' from parquet,
    does any final Spark-based transformations or filtering,
    then collects to Pandas for use in classic ML frameworks.
    """
    processed_path = os.path.join(parquet_dir, "processed_data.parquet")
    if not os.path.exists(processed_path):
        print(f"Error: '{processed_path}' does not exist.")
        return None
    
    df_spark = spark.read.parquet(processed_path)
    
    # Example: we assume 'label' is an integer for multi-class classification.
    # Suppose features are all columns except label, or you have a known subset
    # that you want to use.

    # Convert spark -> pandas
    df_pandas = df_spark.toPandas()

    return df_pandas

def save_pandas_parquet(df, name, parquet_dir):
    """
    Save a pandas DataFrame as a Parquet file.
    
    :param df: pandas DataFrame to save
    :param name: Name of the DataFrame (used for the file name)
    :param parquet_dir: Directory to save the Parquet file
    :return: None
    """
    output_path = os.path.join(parquet_dir, f"{name}.parquet")
    logging.info(f"Saving {name} DataFrame to Parquet at {output_path}...")
    logging.info(f"Schema of {name} DataFrame:")
    logging.info(df.dtypes)
    
    # Write the DataFrame to Parquet with Snappy compression
    df.to_parquet(output_path, compression='snappy', index=False)
    
    logging.info(f"{name} DataFrame saved successfully.")
    return None


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
    df.printSchema()
    
    # Repartition the DataFrame to an optimal number of partitions
    df = df.repartition(8)  # Adjust the number based on your system's capabilities
    
    # Set Parquet block size
    spark.conf.set("parquet.block.size", 256 * 1024 * 1024)  # Set block size to 256 MB
    
    # Write the DataFrame to Parquet with Snappy compression
    df.write.mode("overwrite").option("compression", "snappy").parquet(output_path)
    
    # Clear Spark cache
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

def initialize_environment():
    # Paths and configurations
    config_path = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/config.ini'
    log_file = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/SparkPy_load.log"
    jdbc_driver_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/jdbc/postgresql-42.7.4.jar"
    parquet_dir = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/"
    os.makedirs(parquet_dir, exist_ok=True)
    
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
    
    # Initialize logging
    # queries = sql_queries()
    # Initialize Spark session
   
    spark = initialize_spark(jdbc_driver_path)
    return spark, jdbc_url, jdbc_properties, parquet_dir, log_file

def load_config(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logging.info("Environment setup initialized.")

def initialize_spark(jdbc_driver_path):
    """
    Initializes Spark without Sedona and GeoTools.
    Configures Spark to store temporary files in /home/exx/myCode/data/spark_tmp.
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
            .config("spark.local.dir", "/home/exx/myCode/data/spark_tmp")  # Set custom tmp directory
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

def save_predictions(pred_data, model_name):
    """
    Save model predictions to a directory based on today's date.

    :param pred_data: DataFrame containing the predictions
    :param model_name: Name of the model (e.g., 'xgb', 'lgb', 'cat')
    """
    # Define the base directory
    base_dir = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/predictions"

    # Get today's date in YYYYMMDD format
    today_date = datetime.today().strftime('%Y%m%d')

    # Create the directory path
    dir_path = os.path.join(base_dir, today_date)

    # Check if the directory exists, create it if it doesn't
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    # Define the file path
    file_path = os.path.join(dir_path, f"{model_name}_predictions.csv")

    # Save the DataFrame to CSV
    pred_data.to_csv(file_path, index=False)

    logging.info(f"Predictions saved to {file_path}")

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

def aggregate_gates_to_race_level(df: DataFrame) -> DataFrame:
    """
    For each (course_cd, race_date, race_number, horse_id),
    produce exactly one row that aggregates gate-level metrics.
    """

    # 1. Aggregation for gate-level metrics
    aggregations = [
        F.avg("gps_section_avg_speed").alias("avg_speed_agg"),
        F.max("gps_section_avg_speed").alias("max_speed_agg"),
        F.avg("gps_section_avg_stride_freq").alias("avg_stride_freq_agg"),
        F.first("final_speed").alias("final_speed_agg"),
        F.avg("acceleration_m_s2").alias("avg_accel_agg"),
        F.avg("fatigue_factor").alias("fatigue_agg"),
        F.avg("sec_sectional_time").alias("sectional_time_agg"),
        F.avg("sec_running_time").alias("running_time_agg"),
        F.avg("sec_distance_back").alias("distance_back_agg"),
        F.avg("sec_distance_ran").alias("distance_ran_agg"),
        F.avg("sec_number_of_strides").alias("strides_agg"),
        F.max("max_speed_overall").alias("max_speed_overall"),
        F.min("min_speed_overall").alias("min_speed_overall")
    ]

    # 2. Static race-level data
    static_cols = [
        F.first("label").alias("label"),
        F.first("distance_meters").alias("distance_meters"),
        F.first("trk_cond").alias("trk_cond"),
        F.first("jock_key").alias("jock_key"), 
        F.first("train_key").alias("train_key"),
        F.first("purse").alias("purse"),
        F.first("wps_pool").alias("wps_pool"),
        F.first("weight").alias("weight"),
        F.first("date_of_birth").alias("date_of_birth"),
        F.first("sex").alias("sex"),
        F.first("start_position").alias("start_position"),
        F.first("equip").alias("equip"),
        F.first("claimprice").alias("claimprice"),
        F.first("surface").alias("surface"),
        F.first("weather").alias("weather"),
        F.first("power").alias("power"),
        F.first("med").alias("med"),
        F.first("morn_odds").alias("morn_odds"),
        F.first("avgspd").alias("avgspd"),
        F.first("race_type").alias("race_type"),
        F.first("class_rating").alias("class_rating"),
        F.first("net_sentiment").alias("net_sentiment"),
        F.first("stk_clm_md").alias("stk_clm_md"),
        F.first("turf_mud_mark").alias("turf_mud_mark"),
        F.first("avg_spd_sd").alias("avg_spd_sd"),
        F.first("ave_cl_sd").alias("ave_cl_sd"),
        F.first("hi_spd_sd").alias("hi_spd_sd"),
        F.first("pstyerl").alias("pstyerl"),
        F.first("all_starts").alias("all_starts"),
        F.first("all_win").alias("all_win"),
        F.first("all_place").alias("all_place"),
        F.first("all_show").alias("all_show"),
        F.first("all_fourth").alias("all_fourth"),
        F.first("all_earnings").alias("all_earnings"),
        F.first("cond_starts").alias("cond_starts"),
        F.first("cond_win").alias("cond_win"),
        F.first("cond_place").alias("cond_place"),
        F.first("cond_show").alias("cond_show"),   
        F.first("cond_fourth").alias("cond_fourth"),
        F.first("cond_earnings").alias("cond_earnings"),
        F.first("age_at_race_day").alias("age_at_race_day"),
        F.last("gate_index").alias("gate_index")
    ]

    # 3. Perform Aggregation
    return (
        df.groupBy("course_cd", "race_date", "race_number", "horse_id")
          .agg(*aggregations, *static_cols)
    )

def process_columnar_data(spark, df, parquet_dir):
    """
    Process merged results, sectionals, and gps DataFrame columnar to:
    1. Check and handle duplicates
    2. Convert decimal columns to doubles
    3. Impute missing values
    4. Create age_at_race_day
    5. Add missing value flags for numeric columns
    6. Create labels for multi-class classification
    7. Aggregate gate-level metrics to race-level data
    
    Parameters:
    -----------
    spark : SparkSession
    df    : DataFrame, the merged DataFrame to process
    
    Returns:
    --------
    Processed DataFrame ready for feature engineering.
    """

    # 1. Check for Duplicates
    primary_keys = ["course_cd", "race_date", "race_number", "horse_id", "gate_index"]
    duplicates = (
        df.groupBy(*primary_keys)
          .agg(F.count("*").alias("cnt"))
          .filter(F.col("cnt") > 1)
    )

    dup_count = duplicates.count()
    if dup_count > 0:
        print(f"Found {dup_count} duplicate primary key combinations.")
        duplicates.show()
        raise ValueError(f"Duplicates found: {dup_count}. Deduplication required.")
    print("1. No duplicates found.")
       
    # 2. Convert Decimal Columns to Double
    decimal_cols = ["weight", "power", "morn_odds", "all_earnings", "cond_earnings", "wps_pool"]
    for col_name in decimal_cols:
        df = df.withColumn(col_name, F.col(col_name).cast("double"))
    print("2. Decimal columns converted to double.")
    
    # 3. Impute Missing Values
    # 3a. Impute date_of_birth with Median
    df = impute_date_of_birth_with_median(df)

    # 3b. Create age_at_race_day
    df = df.withColumn(
        "age_at_race_day",
        F.datediff(F.col("race_date"), F.col("date_of_birth")) / 365.25
    )
    print("3b. Created age_at_race_day.")

    # 3c. Impute categorical and numeric columns -- ensure no whitespace in categorical columns
    categorical_defaults = {"weather": "UNKNOWN", "med": "NONE", "turf_mud_mark": "MISSING"}
    numeric_impute_cols = [
        "acceleration_m_s2", "gps_section_avg_stride_freq",
        "prev_speed", "sec_distance_back", "sec_number_of_strides"
    ]

    # Remove whitespace in column names
    df = df.select([F.col(c).alias(c.strip()) for c in df.columns])

    # Impute missing categorical column "turf_mud_mark"
    df = df.withColumn(
        "turf_mud_mark",
        F.when(F.col("turf_mud_mark") == "", "MISSING").otherwise(F.col("turf_mud_mark"))
    )

    # Calculate the mean of the 'wps_pool' column, excluding nulls
    mean_value = df.select(F.mean(F.col("wps_pool")).alias("mean_wps_pool")).collect()[0]["mean_wps_pool"]

    # Replace null values in 'wps_pool' with the calculated mean
    df = df.withColumn(
        "wps_pool",
        F.when(F.col("wps_pool").isNull(), mean_value).otherwise(F.col("wps_pool"))
    )

    # Fill missing values for categorical defaults
    df = df.fillna(categorical_defaults)

    # Ensure "med" and "trk_cond" have no null or empty values
    df = df.withColumn(
        "med",
        F.when(F.col("med").isNull(), "UNK")
        .otherwise(F.when(F.col("med") == "", "UNK").otherwise(F.col("med")))
    )

    df = df.withColumn(
        "trk_cond",
        F.when(F.col("trk_cond").isNull(), "UNK")
        .otherwise(F.when(F.col("trk_cond") == "", "UNK").otherwise(F.col("trk_cond")))
    )

    # Create flags for numeric columns that were missing and fill them with 0.0
    for col_name in numeric_impute_cols:
        df = df.withColumn(f"{col_name}_was_missing", F.when(F.col(col_name).isNull(), 1).otherwise(0))

    df = df.fillna({col_name: 0.0 for col_name in numeric_impute_cols})

    print("3c. Imputed missing values.")
    # 4. Create Label Column
    # Set label to binary (1 for 1st place, 0 otherwise)
    #df = df.withColumn("label", when(col("official_fin") == 1, 1).otherwise(0))

    # Check label distribution
    #df.groupBy("label").count().show()
    # Set label as the exact finishing position
    # df = df.withColumn("label", col("official_fin").cast("int"))

    # # Check label distribution
    # df.groupBy("label").count().show()
    
    df = df.withColumn(
        "label",
        F.when(F.col("official_fin") == 1, 0)  # Win
         .when(F.col("official_fin") == 2, 1)  # Place
         .when(F.col("official_fin") == 3, 2)  # Show
         .when(F.col("official_fin") == 4, 3)  # Fourth
         .when(F.col("official_fin") == 5, 4)  # Fifth
         .when(F.col("official_fin") == 6, 5)  # Sixth
         .when(F.col("official_fin") == 7, 6)  # Seventh
         .otherwise(7)                         # Outside top-7
    )
    df.groupBy("label").count().show()
    print("4. Created label column.")
    input("Press Enter to continue...")

    # print("4. Creating label column...")
    # input("Press Enter to continue...")
    
    # 5. Aggregate Gate-Level Metrics to Race-Level
    df = aggregate_gates_to_race_level(df)
    print("5. Aggregated gate-level metrics to race-level.")
      
    # Step 1: One-Hot Encode and Index Categorical Columns
    categorical_cols = ["course_cd", "equip", "surface", "trk_cond", "weather", 
                        "med", "stk_clm_md", "turf_mud_mark", "race_type"]
    for col in categorical_cols:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' is missing in the input DataFrame.")

    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid="keep") for c in categorical_cols]
    encoders = [OneHotEncoder(inputCols=[f"{c}_index"], outputCols=[f"{c}_ohe"]) for c in categorical_cols]

    # Jockey and trainer keys
    jock_indexer = StringIndexer(inputCol="jock_key", outputCol="jock_key_index", handleInvalid="keep")
    train_indexer = StringIndexer(inputCol="train_key", outputCol="train_key_index", handleInvalid="keep")

    # Build and apply pipeline
    preprocessing_stages = [jock_indexer, train_indexer] + indexers + encoders
    
    for stage in preprocessing_stages:
        print(stage)
    
    pipeline = Pipeline(stages=preprocessing_stages)

    try:
        model = pipeline.fit(df)
        df_transformed = model.transform(df)
        print("1. Completed OHE and indexing for categorical columns.")
    except Exception as e:
        print(f"Pipeline transformation failed: {e}")
        raise

    # Sort Data for Splitting
    sorted_df = df_transformed.orderBy(["course_cd", "race_date", "race_number", "horse_id", "gate_index"])
    print("3. Sorted data for train/test splitting.")

    # Scale numerics columns
    sorted_df = scaled_features_for_pca_or_correlation_analysis(sorted_df)
    
    # Step 4: Time-Based Train/Validation/Test Split
    train_cutoff = '2023-06-30'
    val_cutoff = '2024-03-31'
    train_df, val_df, test_df = split_train_val_data(sorted_df, train_cutoff, val_cutoff)
    print(f"4. Data split: Train = {train_df.count()}, Validation = {val_df.count()}, Test = {test_df.count()}.")

    # Save Datasets
    train_df.write.parquet(f"{parquet_dir}/train_df.parquet", mode="overwrite")
    val_df.write.parquet(f"{parquet_dir}/val_df.parquet", mode="overwrite")
    test_df.write.parquet(f"{parquet_dir}/test_df.parquet", mode="overwrite")
    sorted_df.write.parquet(f"{parquet_dir}/sorted_data.parquet", mode="overwrite")
    print("8. Saved train, validation, test, and sorted data.")

    return df
    
def scaled_features_for_pca_or_correlation_analysis(df):
    # Numeric features to scale
    numeric_features = [
        "avg_speed_agg", "max_speed_agg", "avg_stride_freq_agg", "final_speed_agg",
        "avg_accel_agg", "fatigue_agg", "sectional_time_agg", "running_time_agg",
        "distance_back_agg", "distance_ran_agg", "strides_agg", "max_speed_overall",
        "min_speed_overall", "distance_meters", "purse", "wps_pool", "weight",
        "claimprice", "power", "morn_odds", "avgspd", "class_rating", "net_sentiment",
        "avg_spd_sd", "ave_cl_sd", "hi_spd_sd", "pstyerl", "all_starts", "all_win",
        "all_place", "all_show", "all_fourth", "all_earnings", "cond_starts",
        "cond_win", "cond_place", "cond_show", "cond_fourth", "cond_earnings",
        "age_at_race_day"
    ]

    # Assemble numeric features into a vector
    assembler = VectorAssembler(inputCols=numeric_features, outputCol="numeric_features")
    assembled_df = assembler.transform(df)

    # Scale the features
    scaler = StandardScaler(inputCol="numeric_features", outputCol="scaled_features", withStd=True, withMean=True)
    scaler_model = scaler.fit(assembled_df)
    scaled_df = scaler_model.transform(assembled_df)

    # Drop intermediate columns if no longer needed
    train_df = scaled_df.drop("numeric_features")
    
    return train_df
    
def process_merged_results_sectionals(spark, df, parquet_dir):
    """
    Process merged results and sectionals DataFrame to:
    1. Check and handle duplicates
    2. Convert decimal columns to doubles
    3. Impute missing values
    4. Create age_at_race_day
    5. Add missing value flags for numeric columns
    6. Create labels for multi-class classification
    7. Aggregate gate-level metrics to race-level data
    
    Parameters:
    -----------
    spark : SparkSession
    df    : DataFrame, the merged DataFrame to process
    
    Returns:
    --------
    Processed DataFrame ready for feature engineering.
    """

    # 1. Check for Duplicates
    primary_keys = ["course_cd", "race_date", "race_number", "horse_id", "gate_index"]
    duplicates = (
        df.groupBy(*primary_keys)
          .agg(F.count("*").alias("cnt"))
          .filter(F.col("cnt") > 1)
    )

    dup_count = duplicates.count()
    if dup_count > 0:
        print(f"Found {dup_count} duplicate primary key combinations.")
        duplicates.show()
        raise ValueError(f"Duplicates found: {dup_count}. Deduplication required.")
    print("1. No duplicates found.")
       
    # 2. Convert Decimal Columns to Double
    decimal_cols = ["weight", "power", "morn_odds", "all_earnings", "cond_earnings", "wps_pool"]
    for col_name in decimal_cols:
        df = df.withColumn(col_name, F.col(col_name).cast("double"))
    print("2. Decimal columns converted to double.")
    
    # 3. Impute Missing Values
    # 3a. Impute date_of_birth with Median
    df = impute_date_of_birth_with_median(df)

    # 3b. Create age_at_race_day
    df = df.withColumn(
        "age_at_race_day",
        F.datediff(F.col("race_date"), F.col("date_of_birth")) / 365.25
    )
    print("3b. Created age_at_race_day.")

    # 3c. Impute categorical and numeric columns -- ensure no whitespace in categorical columns
    
    categorical_defaults = {"weather": "UNKNOWN", "med": "NONE", "turf_mud_mark": "MISSING"}
    numeric_impute_cols = ["acceleration_m_s2", "gps_section_avg_stride_freq", 
                           "prev_speed", "sec_distance_back", "sec_number_of_strides"]

    df = df.select([F.col(c).alias(c.strip()) for c in df.columns])
    
    df = df.withColumn("turf_mud_mark", F.when(F.col("turf_mud_mark") == "", "MISSING").otherwise(F.col("turf_mud_mark")))
    
    # Calculate the mean of the 'wps_pool' column, excluding nulls
    mean_value = df.select(mean(col("wps_pool")).alias("mean_wps_pool")).collect()[0]["mean_wps_pool"]
    # Replace null values in 'wps_pool' with the calculated mean
    df = df.withColumn(
    "wps_pool",
    when(col("wps_pool").isNull(), mean_value).otherwise(col("wps_pool"))
    )

    df = df.fillna(categorical_defaults)
    
    df = df.withColumn("med", F.when(F.col("med").isNull(), "UNK").otherwise(F.when(F.col("med") == "", "UNK").otherwise(F.col("med"))))  
    
    df = df.withColumn("trk_cond", F.when(F.col("trk_cond").isNull(), "UNK").otherwise(F.when(F.col("trk_cond") == "", "UNK").otherwise(F.col("trk_cond")))) 
    
    for col_name in numeric_impute_cols:
        df = df.withColumn(f"{col_name}_was_missing", F.when(F.col(col_name).isNull(), 1).otherwise(0))
    df = df.fillna({col_name: 0.0 for col_name in numeric_impute_cols})
    print("3c. Imputed missing values.")
    
    # 4. Create Label Column
    # Set label to binary (1 for 1st place, 0 otherwise)
    #df = df.withColumn("label", when(col("official_fin") == 1, 1).otherwise(0))

    # Check label distribution
    #df.groupBy("label").count().show()
    # Set label as the exact finishing position
    # df = df.withColumn("label", col("official_fin").cast("int"))

    # # Check label distribution
    # df.groupBy("label").count().show()
    
    df = df.withColumn(
        "label",
        F.when(F.col("official_fin") == 1, 0)  # Win
         .when(F.col("official_fin") == 2, 1)  # Place
         .when(F.col("official_fin") == 3, 2)  # Show
         .when(F.col("official_fin") == 4, 3)  # Fourth
         .when(F.col("official_fin") == 5, 4)  # Fifth
         .when(F.col("official_fin") == 6, 5)  # Sixth
         .when(F.col("official_fin") == 7, 6)  # Seventh
         .otherwise(7)                         # Outside top-7
    )
    df.groupBy("label").count().show()
    print("4. Created label column.")
    input("Press Enter to continue...")

    # print("4. Creating label column...")
    # input("Press Enter to continue...")
    
    # 5. Aggregate Gate-Level Metrics to Race-Level
    df = aggregate_gates_to_race_level(df)
    print("5. Aggregated gate-level metrics to race-level.")
    
    # 6. OHE, preprocess_and_sequence_data 
    train_cutoff = '2023-06-30'
    val_cutoff = '2024-03-31'
    min_seq_len = 5
    max_seq_len = 10
    
    # troubleshoot_missing_values(df)
    df = preprocess_and_sequence_data(df, parquet_dir, train_cutoff, val_cutoff, min_seq_len, max_seq_len, pad=True)
    
    return df

def impute_date_of_birth_with_median(df):
    """
    Impute date_of_birth with the median value (or a default if no data exists).
    """
    df = df.withColumn("date_of_birth_ts", F.col("date_of_birth").cast("timestamp").cast("long"))
    median_window = Window.orderBy("date_of_birth_ts")

    median_ts = df.filter(F.col("date_of_birth_ts").isNotNull()).approxQuantile("date_of_birth_ts", [0.5], 0)[0]
    if median_ts is None:
        median_date = F.lit("2000-01-01").cast("date")
    else:
        median_date = F.from_unixtime(F.lit(median_ts)).cast("date")

    df = df.withColumn(
        "date_of_birth",
        F.when(F.col("date_of_birth").isNull(), median_date).otherwise(F.col("date_of_birth"))
    ).drop("date_of_birth_ts")
    print("3a. Missing date_of_birth values imputed with median date.")
    return df

def preprocess_and_sequence_data(df, parquet_dir, train_cutoff, val_cutoff, min_seq_len, max_seq_len, pad=True):
    """
    End-to-end pipeline for preprocessing and sequencing data for LSTM.

    Steps:
    1. One-Hot Encode and Index categorical columns
    2. Add embedding column for horse_id
    3. Sort data for train/test splitting and sequencing
    4. Split data into train, validation, and test sets based on time
    5. Scale features on the training set
    6. Create sequences with aggregated and scaled features
    7. Retain only essential columns
    8. Save train, validation, and test datasets

    Parameters:
    ----------
    df             : DataFrame, processed DataFrame after gate-level aggregation
    parquet_dir    : str, directory to save parquet files
    train_cutoff   : str, date cutoff for training set
    val_cutoff     : str, date cutoff for validation set
    min_seq_len    : int, minimum sequence length for sequence creation
    max_seq_len    : int, maximum sequence length for sequence creation
    pad            : bool, whether to pad sequences to max_seq_len
    
    Returns:
    --------
    sorted_df : DataFrame, fully processed and sorted DataFrame
    """    
    # Step 1: One-Hot Encode and Index Categorical Columns
    categorical_cols = ["course_cd", "equip", "surface", "trk_cond", "weather", 
                        "med", "stk_clm_md", "turf_mud_mark", "race_type"]
    for col in categorical_cols:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' is missing in the input DataFrame.")

    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid="keep") for c in categorical_cols]
    encoders = [OneHotEncoder(inputCols=[f"{c}_index"], outputCols=[f"{c}_ohe"]) for c in categorical_cols]

    # Jockey and trainer keys
    jock_indexer = StringIndexer(inputCol="jock_key", outputCol="jock_key_index", handleInvalid="keep")
    train_indexer = StringIndexer(inputCol="train_key", outputCol="train_key_index", handleInvalid="keep")

    # Build and apply pipeline
    preprocessing_stages = [jock_indexer, train_indexer] + indexers + encoders
    
    for stage in preprocessing_stages:
        print(stage)
    # print("Check preprocessing stages")
    # input("Press Enter to continue...")
    
    
    pipeline = Pipeline(stages=preprocessing_stages)

    try:
        model = pipeline.fit(df)
        df_transformed = model.transform(df)
        print("1. Completed OHE and indexing for categorical columns.")
    except Exception as e:
        print(f"Pipeline transformation failed: {e}")
        raise

    # Step 2: Add Embedding Column
    embedding_col = "horse_id"
    if embedding_col not in df_transformed.columns:
        df_transformed = df_transformed.withColumn(embedding_col, F.col(embedding_col))
    print(f"2. Added embedding column: {embedding_col}.")

    # Step 3: Sort Data for Sequencing and Splitting
    sorted_df = df_transformed.orderBy(["course_cd", "race_date", "race_number", "horse_id", "gate_index"])
    print("3. Sorted data for train/test splitting and sequencing.")

    # Step 4: Time-Based Train/Validation/Test Split
    train_df, val_df, test_df = split_train_val_data(sorted_df, train_cutoff, val_cutoff)
    print(f"4. Data split: Train = {train_df.count()}, Validation = {val_df.count()}, Test = {test_df.count()}.")

    # Step 5: Scale Features
    train_scaled, val_scaled, test_scaled, num_cols = prepare_lstm_data(sorted_df)
    print("5. Features scaled on the training set.")

    # Step 6: Create Sequences
    train_sequences = create_past_race_sequences_variable(train_scaled, min_seq_len, max_seq_len, pad)
    val_sequences = create_past_race_sequences_variable(val_scaled, min_seq_len, max_seq_len, pad)
    test_sequences = create_past_race_sequences_variable(test_scaled, min_seq_len, max_seq_len, pad)
    print("6. Created sequences with min/max length and padding.")

    # Step 7: Retain Only Essential Columns
    columns_to_keep = [
        "race_date", "race_number", "horse_id", "gate_index", "label", 
        "scaled_features", "features", "past_races_sequence", "aggregated_struct"
    ]
    train_sequences = drop_unnecessary_columns(train_sequences, columns_to_keep)
    val_sequences = drop_unnecessary_columns(val_sequences, columns_to_keep)
    test_sequences = drop_unnecessary_columns(test_sequences, columns_to_keep)
    print("7. Retained only essential columns.")

    # Step 8: Save Datasets
    train_sequences.write.parquet(f"{parquet_dir}/train_sequences.parquet", mode="overwrite")
    val_sequences.write.parquet(f"{parquet_dir}/val_sequences.parquet", mode="overwrite")
    test_sequences.write.parquet(f"{parquet_dir}/test_sequences.parquet", mode="overwrite")
    sorted_df.write.parquet(f"{parquet_dir}/sorted_data.parquet", mode="overwrite")
    print("8. Saved train, validation, test, and sorted data.")

    return sorted_df

def split_train_val_data(df, train_cutoff, val_cutoff):
    train_df = df.filter(F.col("race_date") <= F.lit(train_cutoff))
    val_df = df.filter((F.col("race_date") > F.lit(train_cutoff)) & (F.col("race_date") <= F.lit(val_cutoff)))
    test_df = df.filter(F.col("race_date") > F.lit(val_cutoff))
    return train_df, val_df, test_df

from pyspark.ml.feature import VectorAssembler, MinMaxScaler

def prepare_lstm_data(df):
    """
    Prepare data for LSTM model by scaling numerical features or using precomputed scaled features.
    If scaled_features already exist, remove individual numeric columns to avoid redundancy.
    
    Args:
        df (DataFrame): Spark DataFrame to be prepared.

    Returns:
        DataFrame: Transformed DataFrame ready for LSTM input.
    """
    # Check if "scaled_features" column exists
    if "scaled_features" in df.columns:
        print("Using precomputed 'scaled_features'. Dropping individual numerical columns...")
        # Identify numeric columns in the DataFrame
        numeric_cols = [
            "avg_speed_agg", "max_speed_agg", "avg_stride_freq_agg", "final_speed_agg",
            "avg_accel_agg", "fatigue_agg", "sectional_time_agg", "running_time_agg",
            "distance_back_agg", "distance_ran_agg", "strides_agg", "max_speed_overall",
            "min_speed_overall", "distance_meters", "purse", "wps_pool", "weight",
            "claimprice", "power", "morn_odds", "avgspd", "class_rating", "net_sentiment",
            "avg_spd_sd", "ave_cl_sd", "hi_spd_sd", "pstyerl", "all_starts", "all_win",
            "all_place", "all_show", "all_fourth", "all_earnings", "cond_starts",
            "cond_win", "cond_place", "cond_show", "cond_fourth", "cond_earnings",
            "age_at_race_day"
        ]
        
        # Drop numeric columns as "scaled_features" replaces them
        df_cleaned = df.drop(*[col for col in numeric_cols if col in df.columns])
        return df_cleaned
    
    else:
        print("No 'scaled_features' column found. Creating scaled features from numeric columns...")
        
        # Define numeric columns for scaling
        numeric_cols = [
            "avg_speed_agg", "max_speed_agg", "avg_stride_freq_agg", "final_speed_agg",
            "avg_accel_agg", "fatigue_agg", "sectional_time_agg", "running_time_agg",
            "distance_back_agg", "distance_ran_agg", "strides_agg", "max_speed_overall",
            "min_speed_overall", "distance_meters", "purse", "wps_pool", "weight",
            "claimprice", "power", "morn_odds", "avgspd", "class_rating", "net_sentiment",
            "avg_spd_sd", "ave_cl_sd", "hi_spd_sd", "pstyerl", "all_starts", "all_win",
            "all_place", "all_show", "all_fourth", "all_earnings", "cond_starts",
            "cond_win", "cond_place", "cond_show", "cond_fourth", "cond_earnings",
            "age_at_race_day"
        ]

        # Assemble numerical features
        assembler = VectorAssembler(inputCols=numeric_cols, outputCol="numerical_features")
        df_assembled = assembler.transform(df)

        # Scale features
        scaler = MinMaxScaler(inputCol="numerical_features", outputCol="scaled_features")
        scaler_model = scaler.fit(df_assembled)
        df_scaled = scaler_model.transform(df_assembled)

        # Drop intermediate columns if no longer needed
        df_scaled = df_scaled.drop("numerical_features")
        # Drop numeric columns as "scaled_features" replaces them
        df_cleaned = df_scaled.drop(*[col for col in numeric_cols if col in df.columns])
        return df_cleaned
        
def create_past_race_sequences_variable(
    df: DataFrame,
    min_seq_len: int,
    max_seq_len: int,
    pad: bool = True,
) -> DataFrame:
    """
    Collects between min_seq_len and max_seq_len past races into 'past_races_sequence'.
    Includes both `ohe_flat` and `aggregator doubles`.
    """
    # Aggregator double columns
    aggregator_cols = [
        "avg_speed_agg", "max_speed_agg", "final_speed_agg", "avg_accel_agg", 
        "fatigue_agg", "sectional_time_agg", "running_time_agg", "distance_back_agg", 
        "distance_ran_agg", "strides_agg", "max_speed_overall", "min_speed_overall"
    ]

    # OHE columns (VECTOR type)
    ohe_cols = [
        "course_cd_ohe", "equip_ohe", "surface_ohe", "trk_cond_ohe", "weather_ohe",
        "med_ohe", "stk_clm_md_ohe", "turf_mud_mark_ohe", "race_type_ohe"
    ]

    # Convert OHE columns to ARRAY<DOUBLE>
    for col_name in ohe_cols:
        df = df.withColumn(col_name, vector_to_array(col(col_name)))

    # Combine OHE columns into a single flat array
    df = df.withColumn(
        "ohe_flat",
        array(*[col(c) for c in ohe_cols])
    )

    # Define window for past races
    windowSpec = (
        Window.partitionBy("horse_id")
              .orderBy("race_date", "race_number")
    )

    # Create aggregated_struct with `ohe_flat` and `aggregator_cols`
    df = df.withColumn(
        "aggregated_struct",
        F.struct(
            F.flatten(F.col("ohe_flat")).alias("ohe_flat"),  # Flatten combined OHE arrays
            *[F.col(c).alias(c) for c in aggregator_cols]
        )
    )

    # Collect up to `max_seq_len` past races
    df = df.withColumn(
        "past_races_sequence",
        F.collect_list("aggregated_struct").over(windowSpec.rowsBetween(-max_seq_len, -1))
    )

    # Filter rows with fewer than `min_seq_len`
    df = df.filter(F.size(F.col("past_races_sequence")) >= min_seq_len)

    # Add padding if required
    if pad:
        # Define padding struct
        padding_struct = F.struct(
            F.array([F.lit(0.0)] * sum(len(c) for c in ohe_cols)).alias("ohe_flat"),
            *[F.lit(-999.0).alias(c) for c in aggregator_cols]
        )
        df = df.withColumn("sequence_len", F.size(F.col("past_races_sequence")))
        df = df.withColumn(
            "past_races_sequence",
            F.when(
                F.col("sequence_len") < max_seq_len,
                F.concat(
                    F.col("past_races_sequence"),
                    F.array_repeat(padding_struct, F.lit(max_seq_len) - F.col("sequence_len"))
                )
            ).otherwise(F.col("past_races_sequence"))
        )

        # Drop helper columns
        df = df.drop("sequence_len")

    return df

def drop_unnecessary_columns(df: DataFrame, additional_columns_to_keep: list = None) -> DataFrame:
    """
    Drops unnecessary columns from the DataFrame, retaining essential ones for training.

    Parameters:
    ----------
    df                       : DataFrame, input Spark DataFrame.
    additional_columns_to_keep: list, additional columns to retain beyond essential ones.

    Returns:
    --------
    DataFrame with only specified columns retained.
    """
    # Essential columns based on suffixes and explicit column names
    essential_columns = [
        col for col in df.columns 
        if col.endswith("_ohe") or col in ["features", "label", "scaled_features", "sequence", "past_races_sequence"]
    ]

    # Include user-specified columns
    if additional_columns_to_keep:
        essential_columns.extend(additional_columns_to_keep)

    # Ensure unique list
    essential_columns = list(set(essential_columns))

    # Drop columns not in essential_columns
    columns_to_drop = [col for col in df.columns if col not in essential_columns]
    return df.drop(*columns_to_drop)

def troubleshoot_missing_values(df):
    
    # Step 1: One-Hot Encode and Index Categorical Columns
    categorical_cols = ["course_cd", "equip", "surface", "trk_cond", "weather", 
                        "med", "stk_clm_md", "turf_mud_mark", "race_type"]
    
    print("Columns in DataFrame:", df.columns)
    assert all(col.strip() != "" for col in df.columns), "DataFrame contains columns with empty names."
    
    for col in ["course_cd", "equip", "surface", "trk_cond", "weather", 
            "med", "stk_clm_md", "turf_mud_mark", "race_type", "jock_key", "train_key"]:
            null_count = df.filter((F.col(col).isNull()) | (F.col(col) == "")).count()
            print(f"Column '{col}': {null_count} null or empty values.")
            
    
    for col in df.columns:
        if col.strip() == "":
            print(f"Empty or whitespace-only column name detected: '{col}'")
            
    for col in ["turf_mud_mark", "med", "equip", "surface"]:
        empty_count = df.filter(F.col(col) == "").count()
        print(f"Column '{col}': {empty_count} empty string values.")
        