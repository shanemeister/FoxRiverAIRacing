from pyspark.sql.functions import (
    col,
    unix_timestamp,
    expr, abs,
    min as spark_min,
    sum as spark_sum,
    date_format, count, when, udf
)
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from datetime import timedelta
from pyspark.sql import SparkSession
from src.data_preprocessing.data_prep1.data_utils import save_parquet
from pyspark.sql import DataFrame

# Define the UDF to add seconds (including fractional seconds) to a timestamp
def add_seconds(ts, seconds):
    if ts is None or seconds is None:
        return None
    return ts + timedelta(seconds=seconds)

def join_matched_df(gpspoint, sectionals):
    # Step 9: Convert 'time_stamp' and 'sec_time_stamp' to milliseconds since epoch to preserve sub-second precision
    gpspoint_with_ms = gpspoint.withColumn(
        "time_stamp_ms",
        (col("time_stamp").cast("double") * 1000).cast("long")
    )

    sectionals_with_ms = sectionals.withColumn(
        "sec_time_stamp_ms",
        (col("sec_time_stamp").cast("double") * 1000).cast("long")
    )

    # Step 10: Define the join condition with time window (±1000 milliseconds)
    join_condition = (
        (gpspoint_with_ms.course_cd == sectionals_with_ms.course_cd) &
        (gpspoint_with_ms.race_date == sectionals_with_ms.race_date) &
        (gpspoint_with_ms.race_number == sectionals_with_ms.race_number) &
        (gpspoint_with_ms.saddle_cloth_number == sectionals_with_ms.saddle_cloth_number) &
        (abs(gpspoint_with_ms.time_stamp_ms - sectionals_with_ms.sec_time_stamp_ms) <= 250)
    )

    # Step 11: Perform the left join based on the join condition
    matched_df = gpspoint_with_ms.join(
        sectionals_with_ms,
        on=join_condition,
        how="left"
    ).select(
        gpspoint_with_ms["*"],
        sectionals_with_ms["sec_time_stamp"],
        sectionals_with_ms["gate_numeric"],
        sectionals_with_ms["gate_name"],
        sectionals_with_ms["sectional_time"],
        sectionals_with_ms["length_to_finish"],
        sectionals_with_ms["running_time"],
        sectionals_with_ms["distance_back"],
        sectionals_with_ms["number_of_strides"],
        sectionals_with_ms["horse_id"], 
        sectionals_with_ms["official_fin"],
        sectionals_with_ms["post_pos"],
        sectionals_with_ms["speed_rating"],
        sectionals_with_ms["turf_mud_mark"],
        sectionals_with_ms["weight"],
        sectionals_with_ms["morn_odds"],
        sectionals_with_ms["avgspd"],
        sectionals_with_ms["surface"],
        sectionals_with_ms["trk_cond"],
        sectionals_with_ms["class_rating"],
        sectionals_with_ms["weather"],
        sectionals_with_ms["wps_pool"],
        sectionals_with_ms["stk_clm_md"],
        sectionals_with_ms["todays_cls"],
        sectionals_with_ms["net_sentiment"]   
    )
    
    print("matched_df schema after joining gps_with_ms and sectionals_with_ms [RVS]:")
    matched_df.printSchema()
    
    return matched_df

def merge_sectionals(sectional_results, race_id_cols, first_time_df, gpspoint):
    """
    # Step 2: Join 'first_time_df' with 'sectionals_df' to associate each sectional with the race's start time

    # Step 3: Sort 'sectionals_df' by 'gate_numeric' to ensure correct order of gates
    sectionals_df = sectionals_df.orderBy(*race_id_cols, "gate_numeric")

    # Step 4: Define the window specification for cumulative sum
    window_spec = Window.partitionBy(*race_id_cols).orderBy("gate_numeric").rowsBetween(Window.unboundedPreceding, 0)

    # Step 5: Compute cumulative sum of 'sectional_time' for each race
    # Step 6: Define the UDF to add seconds (including fractional seconds) to a timestamp
    """
    # Register the UDF
    add_seconds_udf = udf(add_seconds, TimestampType())

    # Step 2: Join 'first_time_df' with 'sectional_results' to associate each sectional with the race's start time
    sectional_results = sectional_results.join(
        first_time_df,
        on=race_id_cols,
        how="left"
    )

    # Step 3: Sort 'sectional_results' by 'gate_numeric' to ensure correct order of gates
    sectional_results = sectional_results.orderBy(*race_id_cols, "gate_numeric")

    # Step 4: Define the window specification for cumulative sum
    window_spec = Window.partitionBy(*race_id_cols).orderBy("gate_numeric").rowsBetween(Window.unboundedPreceding, 0)

    # Step 5: Compute cumulative sum of 'sectional_time' for each race
    sectional_results = sectional_results.withColumn(
        "cumulative_sectional_time",
        spark_sum("sectional_time").over(window_spec)
    )
    # Step 6: Define the UDF to add seconds (including fractional seconds) to a timestamp

    # Register the UDF
    add_seconds_udf = udf(add_seconds, TimestampType())
    
    # Step 7: Create 'sec_time_stamp' by adding 'cumulative_sectional_time' to 'earliest_time_stamp' using the UDF
    sectional_results = sectional_results.withColumn(
        "sec_time_stamp",
        add_seconds_udf(col("earliest_time_stamp"), col("cumulative_sectional_time"))
    )
    
    # Step 8: Drop intermediate columns if no longer needed
    sectional_results = sectional_results.drop("earliest_time_stamp", "cumulative_sectional_time")
    print("sectional_results schema after merging with first_time_df [RVS]:")
    sectional_results.printSchema()
    return sectional_results

def dup_check(df: DataFrame, cols: list) -> DataFrame:
    """
    Check for duplicate rows based on the specified columns and provide information on missing values.
    
    Parameters:
    df (DataFrame): Input DataFrame
    cols (list): List of columns to check for duplicates
    
    Returns:
    DataFrame: DataFrame with duplicates removed
    """
    # Check for duplicates based on the primary key
    duplicates_df = df.groupBy(cols) \
        .agg(count("*").alias("count")) \
        .filter(col("count") > 1)

    num_duplicates = duplicates_df.count()
    print(f"Number of duplicate rows based on primary key: {num_duplicates}")

    # Check for duplicates based on sec_time_stamp
    sec_time_stamp_duplicates_df = df.groupBy("sec_time_stamp") \
        .agg(count("*").alias("count")) \
        .filter(col("count") > 1)

    num_sec_time_stamp_duplicates = sec_time_stamp_duplicates_df.count()
    print(f"Number of duplicate rows based on sec_time_stamp: {num_sec_time_stamp_duplicates}")

    # Check for missing data in matched_df
    missing_data_df = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])

    print("Missing data in matched_df:")
    missing_data_df.show(n=10, truncate=False)  # Display only the first 10 rows without truncating long strings

    # Display a sample of the DataFrame
    print("Sample of matched_df:")
    df.show(n=10, truncate=False)  # Display only the first 10 rows without truncating long strings

    # Remove duplicates based on the specified columns
    df = df.dropDuplicates(cols)

    return df

def merge_gps_sectionals(spark, sectional_results, gpspoint, parquet_dir):
    """
    Merge GPS and sectional results data into one -- master_df.
    
    Parameters:
    spark (SparkSession): Spark session object
    sectionals_results (DataFrame): Sectional Results data
    gpspoint (DataFrame): GPS data
    
     # Step 1: Calculate the earliest 'time_stamp' for each race
    
    Returns:
    DataFrame: Merged DataFrame
    """

    # Step 1: Calculate the earliest 'time_stamp' for each race
    race_id_cols = ["course_cd", "race_date", "race_number", "saddle_cloth_number"]

    first_time_df = gpspoint.groupBy(*race_id_cols).agg(
        spark_min("time_stamp").alias("earliest_time_stamp")
    )
    sectionals = merge_sectionals(sectional_results, race_id_cols, first_time_df, gpspoint)
    
    matched_df = join_matched_df(gpspoint, sectionals)
    
    # Check for duplicates based on the primary key
    primary_key_columns = ["course_cd", "race_date", "race_number", "saddle_cloth_number", "time_stamp"]
    matched_df_de_dup = dup_check(matched_df, primary_key_columns)
    save_parquet(spark, matched_df_de_dup, "matched_df", parquet_dir)
    
    return matched_df_de_dup
