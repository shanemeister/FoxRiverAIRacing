import os
import logging
from pyspark.sql.functions import col, count, row_number, abs, unix_timestamp, mean, when
import configparser
from pyspark.sql import SparkSession
from src.data_preprocessing.sql_queries import sql_queries
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, mean
from pyspark.sql import DataFrame, Window

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
        gps_time_stamp_duplicates.show(truncate=False)
    
    # Summary statistics
    logging.info(f"Summary statistics for {df_name}:")
    df.describe().show()
    
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
    # db_password = os.getenv("DB_PASSWORD", "SparkPy24!")  # Ensure DB_PASSWORD is set

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
    # Initialize logging
    initialize_logging(log_file)
    queries = sql_queries()
    # Initialize Spark session
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

def initialize_spark(jdbc_driver_path):
    spark = SparkSession.builder \
        .appName("Horse Racing Data Processing") \
        .config("spark.driver.extraClassPath", jdbc_driver_path) \
        .config("spark.executor.extraClassPath", jdbc_driver_path) \
        .config("spark.driver.memory", "64g") \
        .config("spark.executor.memory", "32g") \
        .config("spark.executor.memoryOverhead", "8g") \
        .config("spark.sql.debug.maxToStringFields", "1000") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logging.info("Spark session created successfully.")
    return spark

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

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, abs, unix_timestamp, mean, when

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

    gather_statistics(df, "master_df")
    
    # Identify missing and outlier data for specific columns
    for column in cols:
        missing_count = df.filter(col(column).isNull()).count()
        logging.info(f"Number of missing values in {column}: {missing_count}")
        
        outliers_df = identify_outliers(df, column)
        num_outliers = outliers_df.count()
        logging.info(f"Number of outliers in {column}: {num_outliers}")
        if num_outliers > 0:
            logging.info(f"Sample of outliers in {column}:")
            outliers_df.show(truncate=False)

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