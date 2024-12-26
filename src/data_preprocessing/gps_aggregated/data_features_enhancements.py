import os
import logging
from datetime import timedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, expr, lit, avg as F_avg, min as F_min, max as F_max, sum as F_sum,
    first, last, count, row_number, lag, udf
)
from pyspark.sql.types import TimestampType, DoubleType
from pyspark.sql.window import Window

from src.data_preprocessing.data_prep1.data_utils import (
    save_parquet, 
    # other utils...
)
from src.data_preprocessing.data_prep1.data_utils import haversine_udf

#######################################
# 1) HELPER UDF / UTILITY FUNCTIONS
#######################################
def add_seconds(ts, seconds):
    """UDF to add fractional seconds to a timestamp."""
    if ts is None or seconds is None:
        return None
    return ts + timedelta(seconds=seconds)

add_seconds_udf = udf(add_seconds, TimestampType())


#######################################
# 2) MAIN DATA ENHANCEMENTS MODULE
#######################################
def data_enhancements(spark: SparkSession, parquet_dir: str) -> DataFrame:
    """
    Main entry point to load the 'merge_gps_sectionals_agg' DataFrame and apply
    advanced GPS-based metrics (fastest/slowest gates, fatigue factor, ground loss, etc.),
    excluding route-based geometry computations.

    NOTE: We preserve ALL existing columns in `merge_gps_sectionals_agg` and only append new ones.
    """
    # 1. LOAD BASE DATA
    merged_df = spark.read.parquet(os.path.join(parquet_dir, "merge_gps_sectionals_agg.parquet"))
    logging.info(f"Loaded 'merge_gps_sectionals_agg' with {merged_df.count()} rows")

    # 2. CALCULATE INSTANTANEOUS ACCELERATION
    merged_df = calculate_instantaneous_acceleration(merged_df)

    # 3. IDENTIFY FASTEST/SLOWEST GATES + FATIGUE FACTOR
    merged_df = identify_fastest_slowest_gates(merged_df)
    merged_df = calculate_fatigue_factor(merged_df)

    # 4. CALCULATE GROUND LOSS (ACTUAL DISTANCE VS. OFFICIAL)
    ground_loss_df = calculate_ground_loss_from_gps(spark, parquet_dir)
    #   ground_loss_df has columns: [course_cd, race_date, race_number, saddle_cloth_number, total_distance_run_m]

    # 5. JOIN ground_loss_df to merged_df, preserving all merged_df columns + new distance column
    #    We'll rename 'total_distance_run_m' to something like 'actual_distance_run_m' if we like.
    join_keys = ["course_cd", "race_date", "race_number", "saddle_cloth_number"]
    final_df = merged_df.alias("m").join(
        ground_loss_df.alias("gl"),
        on=join_keys,
        how="inner"
    ).select(
        # All original columns from merged_df
        col("m.*"),
        # The ground-loss column
        col("gl.total_distance_run_m").alias("actual_distance_run_m")
    )

    # 6. SAVE AND RETURN
    save_parquet(spark, final_df, "enriched_data", parquet_dir)
    logging.info("Enrichment job succeeded. Final DataFrame includes all original plus appended columns.")

    return final_df


#######################################
# 3) INSTANTANEOUS ACCELERATION
#######################################
def calculate_instantaneous_acceleration(df: DataFrame) -> DataFrame:
    """
    Calculates instantaneous acceleration from the 'gps_section_avg_speed' columns
    for each gate. Speed in m/s, time in seconds => acceleration in m/s^2.
    """
    race_cols = ["course_cd", "race_date", "race_number", "saddle_cloth_number"]
    window_spec = Window.partitionBy(*race_cols).orderBy("gate_index")

    # 1. Lag the "gps_section_avg_speed" to get previous speed
    df = df.withColumn(
        "prev_speed",
        lag("gps_section_avg_speed").over(window_spec)
    )

    # 2. Calculate time difference between gates
    #    Assume sectionals_sectional_time is the time from previous gate -> this gate
    df = df.withColumn(
        "time_diff_s",
        col("sectionals_sectional_time")  
    )

    # 3. Instantaneous acceleration = (speed_i - speed_{i-1}) / time_diff
    df = df.withColumn(
        "acceleration_m_s2",
        when(col("time_diff_s") > 0,
             (col("gps_section_avg_speed") - col("prev_speed")) / col("time_diff_s"))
        .otherwise(lit(0.0))
    )

    return df


#######################################
# 4) FASTEST/SLOWEST GATES + FATIGUE
#######################################
def identify_fastest_slowest_gates(df: DataFrame) -> DataFrame:
    """
    Identifies which gate had the max/min speed for each (course_cd, race_date, race_number, saddle_cloth_number).
    We'll define 'gate_speed' from e.g. gps_section_avg_speed.
    """
    race_cols = ["course_cd", "race_date", "race_number", "saddle_cloth_number"]
    speed_window = Window.partitionBy(*race_cols)

    # Find max/min speed
    df = df.withColumn(
        "max_speed_overall",
        F_max("gps_section_avg_speed").over(speed_window)
    ).withColumn(
        "min_speed_overall",
        F_min("gps_section_avg_speed").over(speed_window)
    )

    # Mark fastest/slowest
    df = df.withColumn(
        "is_fastest_gate",
        when(col("gps_section_avg_speed") == col("max_speed_overall"), lit(1)).otherwise(lit(0))
    ).withColumn(
        "is_slowest_gate",
        when(col("gps_section_avg_speed") == col("min_speed_overall"), lit(1)).otherwise(lit(0))
    )

    return df

def calculate_fatigue_factor(df: DataFrame) -> DataFrame:
    """
    Fatigue factor = (max_speed - final_speed) / max_speed
    We'll define 'final_speed' as the gps_section_avg_speed at the final gate (largest gate_index).
    """
    race_cols = ["course_cd", "race_date", "race_number", "saddle_cloth_number"]
    w_desc = Window.partitionBy(*race_cols).orderBy(col("gate_index").desc())

    # final_speed = speed at last gate
    df = df.withColumn(
        "final_speed",
        first("gps_section_avg_speed").over(w_desc)
    )

    df = df.withColumn(
        "fatigue_factor",
        when(col("max_speed_overall") > 0,
             (col("max_speed_overall") - col("final_speed")) / col("max_speed_overall"))
        .otherwise(lit(None))
    )
    return df


#######################################
# 5) CALCULATE GROUND LOSS
#######################################
def calculate_ground_loss_from_gps(spark: SparkSession, parquet_dir: str) -> DataFrame:
    """
    Loads raw gpspoint data, uses latitude and longitude to compute actual distance traveled,
    and returns total_distance_run_m for each (course_cd, race_date, race_number, saddle_cloth_number).
    """
    gps_df = spark.read.parquet(os.path.join(parquet_dir, "gpspoint.parquet"))
    
    # Extract latitude and longitude from 'location' (assuming 'location' is in WKB format)
    gps_df = gps_df.withColumn("latitude", expr("CAST(unhex(location) AS STRING)").cast("double")) \
                   .withColumn("longitude", expr("CAST(unhex(location) AS STRING)").cast("double"))
    
    race_cols = ["course_cd", "race_date", "race_number", "saddle_cloth_number"]
    w_time = Window.partitionBy(*race_cols).orderBy("time_stamp")
    
    # LAG latitude and longitude
    gps_df = gps_df.withColumn("prev_latitude", lag("latitude").over(w_time)) \
                   .withColumn("prev_longitude", lag("longitude").over(w_time))
    
    # Distance between consecutive points using Haversine function
    gps_df = gps_df.withColumn(
        "segment_distance",
        when(
            (col("prev_latitude").isNotNull()) & (col("prev_longitude").isNotNull()),
            haversine_udf(col("latitude"), col("longitude"), col("prev_latitude"), col("prev_longitude"))
        ).otherwise(lit(0.0))
    )
    
    # Cumulative sum of distances
    gps_df = gps_df.withColumn(
        "cumulative_distance",
        F_sum("segment_distance").over(w_time)
    )
    
    # Final distance per horse
    distance_df = gps_df.groupBy(*race_cols).agg(
        F_max("cumulative_distance").alias("total_distance_run_m")
    )
    
    # Return the keys + distance
    return distance_df