from pyspark.sql.functions import (
            col, udf, sum as spark_sum, mean as spark_mean,
            min as spark_min, avg as spark_avg, lag,
            count as spark_count, when, abs, udf, expr)
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

def merge_sectionals(sectional_results, race_id_cols, first_time_df, gpspoint):
    """
    Merge sectional results with the earliest time stamp for each race.
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
    sectional_results = sectional_results.orderBy(*race_id_cols, "sectionals_gate_numeric")

    # Step 4: Define the window specification for cumulative sum
    window_spec = Window.partitionBy(*race_id_cols).orderBy("sectionals_gate_numeric").rowsBetween(Window.unboundedPreceding, 0)

    # Step 5: Compute cumulative sum of 'sectional_time' for each race
    sectional_results = sectional_results.withColumn(
        "cumulative_sectional_time",
        spark_sum("sectionals_sectional_time").over(window_spec)
    )
    
    # Step 6: Create 'sec_time_stamp' by adding 'cumulative_sectional_time' to 'earliest_time_stamp' using the UDF
    sectional_results = sectional_results.withColumn(
        "sec_time_stamp",
        add_seconds_udf(col("earliest_time_stamp"), col("cumulative_sectional_time"))
    )
    
    # Step 7: Drop intermediate columns if no longer needed
    sectional_results = sectional_results.drop("cumulative_sectional_time")
    print("sectional_results schema after merging with first_time_df [RVS]:")
    sectional_results.printSchema()
    return sectional_results

# Assuming add_seconds is already defined and registered as add_seconds_udf
# and that `sectional_results` already contains EQB features and now sec_time_stamp
# from merge_sectionals

def aggregate_gps_by_section(spark, sectional_results, gpspoint):
    race_id_cols = ["course_cd", "race_date", "race_number", "saddle_cloth_number"]
    window_spec = Window.partitionBy(*race_id_cols).orderBy("sec_time_stamp")

    # Compute start_time for each section as either earliest_time_stamp (for first section)
    # or previous section's sec_time_stamp
    sectional_results = sectional_results.withColumn(
        "prev_sec_time_stamp", lag("sec_time_stamp").over(window_spec)
    )

    # The earliest_time_stamp might have been dropped previously. If so, rejoin it from first_time_df if needed.
    # Let's assume earliest_time_stamp is still there or we can re-join it:
    # If not available, re-join from the initial earliest_time_df.
    # For simplicity, assume we still have earliest_time_stamp or store it before dropping.

    sectional_results = sectional_results.withColumn(
        "start_time",
        when(col("prev_sec_time_stamp").isNull(), col("earliest_time_stamp"))
        .otherwise(col("prev_sec_time_stamp"))
    ).withColumn(
        "end_time", col("sec_time_stamp")
    ).drop("prev_sec_time_stamp")  # Keep earliest_time_stamp if needed

    # Now each section row has start_time and end_time, defining the interval for that section.

    # Join gpspoint with sectional_results intervals on race_id_cols
    # Then filter by time intervals:
    interval_df = sectional_results.select(*race_id_cols, "sectionals_gate_name", "start_time", "end_time")

    gps_with_intervals = gpspoint.join(interval_df, on=race_id_cols, how="inner")

    gps_filtered = gps_with_intervals.filter(
        (col("time_stamp") >= col("start_time")) & (col("time_stamp") <= col("end_time"))
    )

    # Aggregate GPS metrics per section:
    # Example: avg_speed, avg_stride_freq
    # If you want more metrics like distance traveled, you might need a segment_distance calculation precomputed.
    gps_agg = gps_filtered.groupBy(*race_id_cols, "sectionals_gate_name").agg(
        spark_mean("speed").alias("section_avg_speed"),
        spark_mean("stride_frequency").alias("section_avg_stride_freq")
        # Add more aggregation as needed, e.g. sum of speeds/time to compute acceleration approximations
    )

    # Join aggregated metrics back to sectional_results
    final_df = sectional_results.join(
        gps_agg,
        on=[*race_id_cols, "sectionals_gate_name"],
        how="left"
    )

    # final_df now contains static EQB features (if already in sectional_results),
    # sectional data, and aggregated GPS features at each gate.

    return final_df

def merge_gps_sectionals(spark, sectional_results, gpspoint, parquet_dir):
    race_id_cols = ["course_cd", "race_date", "race_number", "saddle_cloth_number"]

    # Step 1: Compute earliest_time_stamp per race
    first_time_df = gpspoint.groupBy(*race_id_cols).agg(spark_min("time_stamp").alias("earliest_time_stamp"))

    # Merge sectionals with earliest_time_stamp and compute sec_time_stamp
    sectional_results = merge_sectionals(sectional_results, race_id_cols, first_time_df, gpspoint)

    # Now aggregate GPS data by section intervals
    final_df = aggregate_gps_by_section(spark, sectional_results, gpspoint)

    # Optional: Check duplicates, drop columns, etc., as needed

    save_parquet(spark, final_df, "final_aggregated_df", parquet_dir)
    
    return final_df

