import os
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when

def results_sectionals_gps_merged_agg_features(
    spark,
    results_sectionals_df: DataFrame,
    gpspoint_df: DataFrame,
    parquet_dir: str
) -> DataFrame:
    """
    Aggregates the gpspoint data by (course_cd, race_date, race_number, saddle_cloth_number)
    to compute various summary statistics (e.g., average/min/max speed, stride frequency, etc.),
    then left-joins those aggregated columns onto results_sectionals_df.

    Returns the merged DataFrame, and also saves it to parquet.
    """

    #######################################
    # 1) DEFINE KEYS + AGGREGATE GPS DATA
    #######################################
    # We'll group the gpspoint data by the 4 race ID columns and compute 
    # aggregator columns for speed and stride_frequency. 
    # Adjust or expand as needed.

    race_id_cols = ["course_cd", "race_date", "race_number", "saddle_cloth_number"]

    gps_agg_df = (
        gpspoint_df.groupBy(*race_id_cols)
        .agg(
            F.mean("speed").alias("gps_avg_speed"),
            F.min("speed").alias("gps_min_speed"),
            F.max("speed").alias("gps_max_speed"),
            F.mean("stride_frequency").alias("gps_avg_stride_freq"),
            F.max("stride_frequency").alias("gps_max_stride_freq"),
            F.min("stride_frequency").alias("gps_min_stride_freq"),
            F.stddev("speed").alias("gps_std_speed"),
        )
    )

    # For logs:
    logging.info(f"gps_agg_df has {gps_agg_df.count()} aggregated rows from gpspoint.")
    
    #######################################
    # 2) LEFT-JOIN THE GPS AGGREGATIONS ONTO RESULTS + SECTIONALS
    #######################################
    # We'll do a left join so that every row in results_sectionals_df is retained,
    # even if there's no GPS aggregator row.
    join_condition = (
        (results_sectionals_df["course_cd"] == gps_agg_df["course_cd"]) &
        (results_sectionals_df["race_date"] == gps_agg_df["race_date"]) &
        (results_sectionals_df["race_number"] == gps_agg_df["race_number"]) &
        (results_sectionals_df["saddle_cloth_number"] == gps_agg_df["saddle_cloth_number"])
    )
    
    merged_df = (
        results_sectionals_df.alias("rs")
        .join(gps_agg_df.alias("g"), on=join_condition, how="left")
        # Keep all columns from the base table (rs), plus the aggregator columns
        .select(
            col("rs.*"),
            col("g.gps_avg_speed"),
            col("g.gps_min_speed"),
            col("g.gps_max_speed"),
            col("g.gps_avg_stride_freq"),
            col("g.gps_max_stride_freq")
        )
    )
    
    #######################################
    # 3) (Optional) IMPUTE MISSING COLUMNS
    #######################################
    # If we want to fill NA with a sentinel like 0.0 or "MISSING", we can do so here.
    # For numeric aggregator columns, you might do:
    merged_df = merged_df.fillna({"gps_avg_speed": 0.0,
                                  "gps_min_speed": 0.0,
                                  "gps_max_speed": 0.0,
                                  "gps_avg_stride_freq": 0.0,
                                  "gps_max_stride_freq": 0.0})
    

    merged_df = add_stride_synergy_features(merged_df)
    final_df = add_fatigue_factor(merged_df)
    
    return final_df

def add_stride_synergy_features(df):
    """
    Adds stride-synergy features that combine the 'avg_stride_length' from sectionals
    with the 'gps_avg_stride_freq' from GPS aggregator.

    df must have:
      - df['avg_stride_length']  (from your aggregated sectionals)
      - df['gps_avg_stride_freq'] (from your aggregated gpspoint)

    We create:
      - 'stride_power' = avg_stride_length * gps_avg_stride_freq
      - 'stride_efficiency_ratio' = stride_power / (some measure of speed) (optional)
      - Alternatively or additionally: 'stride_freq_ratio' = gps_avg_stride_freq / avg_stride_length
    """

    # 1) We'll produce a simple product:
    df = df.withColumn(
        "stride_power",
        col("avg_stride_length") * col("gps_avg_stride_freq")
    )

    # 2) If you want a ratio, you can do:
    df = df.withColumn(
        "stride_freq_ratio",
        when(
            (col("avg_stride_length").isNotNull()) & (col("avg_stride_length") != 0),
            col("gps_avg_stride_freq") / col("avg_stride_length")
        ).otherwise(lit(None))
    )

    # 3) If you want a synergy-based speed vs. actual speed:
    #    Suppose you have "gps_avg_speed" as well—then "stride_power" ~ actual speed
    #    and you can measure difference or ratio. For example:
    df = df.withColumn(
        "stride_power_vs_avg_speed_ratio",
        when(
            (col("gps_avg_speed").isNotNull()) & (col("gps_avg_speed") != 0),
            (col("stride_power") / col("gps_avg_speed"))
        ).otherwise(lit(None))
    )

    return df

def add_fatigue_factor(df):
    """
    Adds a 'fatigue_factor' column assuming you have:
      - df['gps_max_speed']  (peak speed in m/s)
      - df['gps_avg_speed'] or some 'final_speed' column (the speed near the end).
    """
    # If you truly have a final_speed aggregator (like from the last gate or the last X seconds), use that.
    # Otherwise, use gps_avg_speed as a proxy.
    # We'll call it "final_speed" for clarity:
    df = df.withColumn(
        "final_speed",
        when(col("final_running_time").isNotNull(),  # example: from sectionals
             col("total_distance_ran") / col("total_running_time")  
        ).otherwise(col("gps_avg_speed"))  # fallback
    )

    # Now define fatigue factor
    df = df.withColumn(
        "fatigue_factor",
        when(
            (col("gps_max_speed").isNotNull()) & (col("gps_max_speed") > 0),
            (col("gps_max_speed") - col("final_speed")) / col("gps_max_speed")
        ).otherwise(lit(None))
    )
    return df