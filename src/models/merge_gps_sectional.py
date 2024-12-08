from pyspark.sql.functions import (
    col,
    unix_timestamp,
    expr, abs,
    min as spark_min,
    sum as spark_sum,
    date_format
)
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf
from datetime import timedelta

# Define the UDF to add seconds (including fractional seconds) to a timestamp
def add_seconds(ts, seconds):
    if ts is None or seconds is None:
        return None
    return ts + timedelta(seconds=seconds)

def join_matched_df(gps_df, sectionals_df):
    # Step 9: Convert 'time_stamp' and 'sec_time_stamp' to milliseconds since epoch to preserve sub-second precision
    gps_with_ms = gps_df.withColumn(
        "time_stamp_ms",
        (col("time_stamp").cast("double") * 1000).cast("long")
    )

    sectionals_with_ms = sectionals_df.withColumn(
        "sec_time_stamp_ms",
        (col("sec_time_stamp").cast("double") * 1000).cast("long")
    )

    # Step 10: Define the join condition with time window (±1000 milliseconds)
    join_condition = (
        (gps_with_ms.course_cd == sectionals_with_ms.course_cd) &
        (gps_with_ms.race_date == sectionals_with_ms.race_date) &
        (gps_with_ms.race_number == sectionals_with_ms.race_number) &
        (gps_with_ms.saddle_cloth_number == sectionals_with_ms.saddle_cloth_number) &
        (abs(gps_with_ms.time_stamp_ms - sectionals_with_ms.sec_time_stamp_ms) <= 500)
    )

    # Step 11: Perform the left join based on the join condition
    matched_df = gps_with_ms.join(
        sectionals_with_ms,
        on=join_condition,
        how="left"
    ).select(
        gps_with_ms["*"],
        sectionals_with_ms["sec_time_stamp"],
        sectionals_with_ms["gate_numeric"],
        sectionals_with_ms["gate_name"],
        sectionals_with_ms["sectional_time"]
    )

    return matched_df

def merge_sectionals(sectionals_df, race_id_cols, first_time_df, gps_df):
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

    # Step 2: Join 'first_time_df' with 'sectionals_df' to associate each sectional with the race's start time
    sectionals_df = sectionals_df.join(
        first_time_df,
        on=race_id_cols,
        how="left"
    )

    # Step 3: Sort 'sectionals_df' by 'gate_numeric' to ensure correct order of gates
    sectionals_df = sectionals_df.orderBy(*race_id_cols, "gate_numeric")

    # Step 4: Define the window specification for cumulative sum
    window_spec = Window.partitionBy(*race_id_cols).orderBy("gate_numeric").rowsBetween(Window.unboundedPreceding, 0)

    # Step 5: Compute cumulative sum of 'sectional_time' for each race
    sectionals_df = sectionals_df.withColumn(
        "cumulative_sectional_time",
        spark_sum("sectional_time").over(window_spec)
    )
    # Step 6: Define the UDF to add seconds (including fractional seconds) to a timestamp

    # Register the UDF
    add_seconds_udf = udf(add_seconds, TimestampType())
    
    # Step 7: Create 'sec_time_stamp' by adding 'cumulative_sectional_time' to 'earliest_time_stamp' using the UDF
    sectionals_df = sectionals_df.withColumn(
        "sec_time_stamp",
        add_seconds_udf(col("earliest_time_stamp"), col("cumulative_sectional_time"))
    )
    
    # Step 8: Drop intermediate columns if no longer needed
    sectionals_df = sectionals_df.drop("earliest_time_stamp", "cumulative_sectional_time")

    return sectionals_df

def merge_gps_sectionals(spark, results_df, sectionals_df, gps_df):
    """
    Merge GPS and sectional data with results data.
    
    Parameters:
    spark (SparkSession): Spark session object
    results_df (DataFrame): Results data
    sectionals_df (DataFrame): Sectional data
    gps_df (DataFrame): GPS data
    
     # Step 1: Calculate the earliest 'time_stamp' for each race
    
    Returns:
    DataFrame: Merged DataFrame
    """
   
    race_id_cols = ["course_cd", "race_date", "race_number", "saddle_cloth_number"]

    # Step 1: Calculate the earliest 'time_stamp' for each race
    race_id_cols = ["course_cd", "race_date", "race_number", "saddle_cloth_number"]

    first_time_df = gps_df.groupBy(*race_id_cols).agg(
        spark_min("time_stamp").alias("earliest_time_stamp")
    )
    sectionals = merge_sectionals(sectionals_df, race_id_cols, first_time_df, gps_df)
    
    matched_df = join_matched_df(gps_df, sectionals)
    
    return matched_df
