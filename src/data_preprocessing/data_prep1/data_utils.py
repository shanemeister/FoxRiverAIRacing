import os
import logging
from pyspark.sql.functions import col, count, row_number, abs, unix_timestamp, mean, when, lit, min as spark_min, max as spark_max , row_number, mean, countDistinct, expr, datediff, when, trim
import configparser
from pyspark.sql import SparkSession
from src.data_preprocessing.data_prep1.sql_queries import sql_queries
from pyspark.sql.window import Window
from pyspark.sql import DataFrame, Window
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
import datetime

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
        .master("local[*]") \
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

def process_merged_results_sectionals(spark, df, parquet_dir):
    """
    Process merged results and sectionals DataFrame to remove duplicates, impute missing values, convert data types, OHE, and gather statistics.
    
    Parameters:
    df (DataFrame): The merged DataFrame to process
    
    Returns:
    DataFrame: The processed DataFrame
    """
# 1 Check for duplicates ========================================================================================
    # Drop duplicates with a tolerance of 0.5 seconds
    primary_keys = ["course_cd", "race_date", "race_number", "saddle_cloth_number", "sectionals_gate_name"]

    duplicates = df.groupBy(*primary_keys) \
                   .agg(count("*").alias("cnt")) \
                   .filter(col("cnt") > 1)

    dup_count = duplicates.count()

    try:
        if dup_count > 0:
            print(f"Found {dup_count} duplicate primary key combinations.")
            duplicates.show()
            raise ValueError(f"Duplicates found -- write function to dedup -- duplicates found: {dup_count}")
        else:
            print("No duplicates found.")
    except ValueError as e:
        print(f"Duplicates error: {e}")
        exit(1)
    
    print("1. Duplicates checked.")

    
# 2 furlong = 201.168 meters -- Convert distance to meters ========================================================================================
    conversion_factor = 201.168
    df = df \
        .withColumn("distance_meters", col("distance") * lit(conversion_factor)) \
        .drop("dist_unit")
    print("2. Distance column converted to meters.")

# 3 Convert decimal columns to double ========================================================================================
    decimal_cols = ["weight", "distance", "power", "morn_odds", "all_earnings", "cond_earnings"]
    for col_name in decimal_cols:
        df = df.withColumn(col_name, col(col_name).cast("double")) 
    print("3. Decimal columns converted to double.")    
# 4 Impute missing values ========================================================================================
# 4a Impute date_of_birth with the median date_of_birth ========================================================================================
    # Convert date_of_birth to a numeric timestamp for median calculation
    df = df.withColumn("date_of_birth_ts", col("date_of_birth").cast("timestamp").cast("long"))

    # Calculate the median of date_of_birth
    median_window = Window.orderBy("date_of_birth_ts")
    row_count = df.filter(col("date_of_birth_ts").isNotNull()).count()

    if row_count % 2 == 0:  # Even number of rows
        median_row_1 = row_count // 2
        median_row_2 = median_row_1 + 1
        median_ts = df.filter(col("date_of_birth_ts").isNotNull()) \
            .select("date_of_birth_ts") \
            .withColumn("row_num", expr("row_number() over (ORDER BY date_of_birth_ts)")) \
            .filter((col("row_num") == median_row_1) | (col("row_num") == median_row_2)) \
            .groupBy().agg(expr("avg(date_of_birth_ts)").alias("median_ts")) \
            .collect()[0]["median_ts"]
    else:  # Odd number of rows
        median_row = (row_count + 1) // 2
        median_ts = df.filter(col("date_of_birth_ts").isNotNull()) \
            .select("date_of_birth_ts") \
            .withColumn("row_num", expr("row_number() over (ORDER BY date_of_birth_ts)")) \
            .filter(col("row_num") == median_row) \
            .collect()[0]["date_of_birth_ts"]

    # Convert median timestamp back to date
    median_date = lit(expr(f"CAST(FROM_UNIXTIME({median_ts}) AS DATE)"))

    # Fill missing values with the global median date
    df = df.withColumn(
        "date_of_birth",
        when(col("date_of_birth").isNull(), median_date).otherwise(col("date_of_birth"))
    ).drop("date_of_birth_ts")
    print("4a. Missing date_of_birth values imputed with the median date_of_birth.")
#4b Convert date_of_birth to age ========================================================================================
    # Ensure both date_of_birth and race_date are in date format
    df = df.withColumn("date_of_birth", col("date_of_birth").cast("date"))
    df = df.withColumn("race_date", col("race_date").cast("date"))

    # Calculate age in days, then convert to years
    df = df.withColumn(
        "age_at_race_day",
        datediff(col("race_date"), col("date_of_birth")) / 365.25  # Convert days to years
    )
    print("4b. Converted date_of_birth to Age")
#4c Impute missing weather values ========================================================================================
    df = df.fillna({"weather": "Clear"})
    print("4c. Missing weather values imputed with 'Clear'.")
    
#4d Impute missing wps_pool values ========================================================================================
    # Calculate the mean of the 'wps_pool' column, excluding nulls
    mean_value = df.select(mean(col("wps_pool")).alias("mean_wps_pool")).collect()[0]["mean_wps_pool"]

    # Replace null values in 'wps_pool' with the calculated mean
    df = df.withColumn(
        "wps_pool",
        when(col("wps_pool").isNull(), mean_value).otherwise(col("wps_pool"))
    )

    # Show the updated DataFrame
    df.filter(col("wps_pool").isNull()).count()
    print("4d. Missing wps_pool values imputed with the mean wps_pool. wps_pool null count: ", df.filter(col("wps_pool").isNull()).count())
    
#4e Impute missing equip values ========================================================================================    
    df = df.fillna({"equip": "No_Equip"})
    print("4e. Missing equip values imputed with 'No_Equip'.")
    
#4f Impute missing trk_cond values ========================================================================================
    cols = ["trk_cond", "trk_cond_desc"] 
    df.select(cols).distinct().count()
    distinct_value_counts = df.groupBy(cols).count()
    # Fill missing values with "MISSING" for the specified columns
    df = df.fillna({col: "MISSING" for col in cols})
    print("4f. Missing trk_cond values imputed with 'MISSING'.")
    
# 5 Create the label column ========================================================================================
    # 1. Binary Classification (Top-4 Finish):
    #    Label = 1 if official_fin <= 4 else 0.
    df = df.withColumn("label", when(col("official_fin") <= 4, 1).otherwise(0))
    # 2.	Winning Probability (Single-Class):
    #     Label = 1 if official_fin == 1 (win), else 0.
    # df = df.withColumn("label", when(col("official_fin") == 1, lit(1)).otherwise(lit(0)))
    # 3. Ordinal Outcome (Exact Finish Position):
    # The label is just the finishing position itself. If official_fin is already a numeric column, you can assign it directly:
    # df = df.withColumn("label", col("official_fin"))
    # 4. Regression (Winning Time Prediction):
    # The label is the winning time in seconds. If winning_time is already a numeric column, you can assign it directly:
    # df = df.withColumn("label", col("winning_time"))
    # 5. Multi-Class (Exact Finish Bracket):
    # Define classes as:
    # •	Class 0: Win (finish == 1)
	# •	Class 1: Top-3 but not win (2 ≤ finish ≤ 3)
	# •	Class 2: Top-4 but not top-3 (finish == 4)
	# •	Class 3: Outside top-4 (finish > 4)
    # df = df.withColumn("label",
    #     when(col("official_fin") == 1, lit(0))                          # Win
    #     .when((col("official_fin") >= 2) & (col("official_fin") <= 3), lit(1))  # Top-3 but not win
    #     .when(col("official_fin") == 4, lit(2))                         # Top-4 but not top-3
    #     .otherwise(lit(3))                                              # Outside top-4
    # )
    print("5. Created the label column: <=4 = 1, >4 = 0.")
    
#6 OHE and Prep Spark Pipeline ========================================================================================
    # Took out course_cd to see if it would help identify other predictive features.
    # Categorical columns equip, surface, trk_cond, weather, dist_unit, race_type 
    categorical_cols = ["course_cd", "equip", "surface", "trk_cond", "weather", "race_type", "sex" , "med", "stk_clm_md", "turf_mud_mark"]
    indexers = [StringIndexer(inputCol=c, outputCol=c+"_index", handleInvalid="keep") for c in categorical_cols]
    encoders = [OneHotEncoder(inputCols=[c+"_index"], outputCols=[c+"_ohe"]) for c in categorical_cols]
    print("6. OneHotEncoded categorical columns.")
    
    for c in categorical_cols:
        empty_count = df.filter((col(c) == "") | (col(c).isNull())).count()
        if empty_count > 0:
            print(f"Warning: {c} has {empty_count} rows with empty or null values.")
            
    # for c in categorical_cols:
    #     distinct_values = df.select(c).distinct().collect()
    #     print(c, [row[c] for row in distinct_values])
    df = df.withColumn("turf_mud_mark", when(col("turf_mud_mark") == "", "MISSING").otherwise(col("turf_mud_mark")))     
    df = df.withColumn("med", when(col("med") == "", "MISSING").otherwise(col("med")))
#7 Assemble Features ========================================================================================
    jock_indexer = StringIndexer(inputCol="jock_key", outputCol="jock_key_index", handleInvalid="keep")
    train_indexer = StringIndexer(inputCol="train_key", outputCol="train_key_index", handleInvalid="keep")
    # Numeric columns
    # Removing "race_number" 
    numeric_cols = ["morn_odds", "age_at_race_day",  "purse", "weight", "start_position", 
                    "claimprice", "power", "avgspd", "class_rating", "net_sentiment","weight", 
                    "distance", "power", "all_earnings", "cond_earnings", "avg_spd_sd", 
                    "ave_cl_sd", "hi_spd_sd", "pstyerl", "all_starts", 
                "all_win", "all_place", "all_show", "all_fourth", "cond_starts", 
                    "cond_win", "cond_place", "cond_show", "cond_fourth"]
    # Add later to numeric cols after normalization: "jock_key_index", "train_key_index", 
    
    #Spark Pipeline
        # Create a pipeline to transform data
    preprocessing_stages = [jock_indexer, train_indexer] + indexers + encoders
    pipeline = Pipeline(stages=preprocessing_stages)
    model = pipeline.fit(df)
    df_transformed = model.transform(df)
    
    ohe_cols = [c+"_ohe" for c in categorical_cols]
    
    assembler = VectorAssembler(inputCols=numeric_cols + ohe_cols, outputCol="raw_features")
    df_assembled = assembler.transform(df_transformed)
    
    # 1.	Assemble Numeric Features Into a Vector:
    # First, use a VectorAssembler to combine all numeric columns into a single feature vector:
    # Normalize Numeric Values
    # numeric_cols defined as above
    numeric_assembler = VectorAssembler(
        inputCols=numeric_cols,
        outputCol="numeric_vector"
    )
    df_with_numeric_vector = numeric_assembler.transform(df_assembled)  # df_assembled is your DataFrame with numeric_cols
    # 2.	Apply StandardScaler:
    # Using StandardScaler with withMean=True and withStd=True ensures zero mean and unit variance scaling.
    scaler = StandardScaler(
    inputCol="numeric_vector",
    outputCol="numeric_scaled",
    withMean=True,  # center the data with mean
    withStd=True    # scale to unit variance
    )
    scaler_model = scaler.fit(df_with_numeric_vector)
    df_scaled = scaler_model.transform(df_with_numeric_vector)

    # 3. Replace Original Numeric Features with Scaled Vector:
    # Now df_scaled has a new column numeric_scaled that contains the scaled versions of your numeric features. 
    # You can drop the original numeric columns if you no longer need them, or keep them for reference. When building 
    # your final features vector for the model, include numeric_scaled vector instead of individual numeric columns.
    # Suppose you have categorical OHE columns in ohe_cols
    # Combine numeric_scaled with ohe_cols
    final_assembler = VectorAssembler(
        inputCols=["numeric_scaled"] + ohe_cols,
        outputCol="features"
    )
    df_final = final_assembler.transform(df_scaled)

    # Now df_final contains 'features' that has normalized numeric features plus OHE columns.
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
    scaler_model = scaler.fit(df_assembled)
    df_final = scaler_model.transform(df_assembled)
    
    print("7. Assembled features.")
    
#8 Drop unneeded columns that are now part of features or OHE ========================================================================================
    drop_cols = ["wps_pool","distance","course_cd","equip","surface","trk_cond","weather","dist_unit","race_type","sex","med","stk_clm_md","turf_mud_mark",
                 "course_cd_index","equip_index","surface_index","trk_cond_index","weather_index","race_type_index","sex_index","med_index","stk_clm_md_index",
                 "turf_mud_mark_index","jock_key","train_key","date_of_birth","raw_features","age_at_race_day","race_number","purse","weight","start_position","claimprice",
                 "power","morn_odds","avgspd","jock_key_index","train_key_index","class_rating","net_sentiment","avg_spd_sd","ave_cl_sd","hi_spd_sd","pstyerl","all_starts",
                 "all_win","all_place","all_show","all_fourth","all_earnings","cond_starts","cond_win","cond_place","cond_show","cond_fourth","cond_earnings"]
    
    df_final = df_final.drop(*drop_cols)
    
    df_final.printSchema()
    
    return df_final