from pyspark.sql.functions import (
    col, udf, sum as spark_sum, 
    min as spark_min, avg as spark_avg, 
    count as spark_count, when, lag
)
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from datetime import timedelta
from pyspark.sql import DataFrame
from src.data_preprocessing.data_prep1.data_utils import save_parquet

def add_seconds(ts, seconds):
    if ts is None or seconds is None:
        return None
    return ts + timedelta(seconds=seconds)

add_seconds_udf = udf(add_seconds, TimestampType())

def dup_check(df: DataFrame, cols: list) -> DataFrame:
    duplicates_df = df.groupBy(cols).agg(count("*").alias("count")).filter(col("count") > 1)
    num_duplicates = duplicates_df.count()
    print(f"Number of duplicate rows based on primary key: {num_duplicates}")

    sec_time_stamp_duplicates_df = df.groupBy("sec_time_stamp") \
        .agg(count("*").alias("count")) \
        .filter(col("count") > 1)

    num_sec_time_stamp_duplicates = sec_time_stamp_duplicates_df.count()
    print(f"Number of duplicate rows based on sec_time_stamp: {num_sec_time_stamp_duplicates}")

    missing_data_df = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    print("Missing data in matched_df:")
    missing_data_df.show(n=10, truncate=False)

    print("Sample of matched_df:")
    df.show(n=10, truncate=False)

    df = df.dropDuplicates(cols)
    return df

def merge_sectionals(sectional_results, race_id_cols, first_time_df):
    """
    Merge sectional results with earliest_time_stamp for each race and compute intervals.
    """
    # Join earliest_time_stamp
    sectional_results = sectional_results.join(first_time_df, on=race_id_cols, how="left")

    # Sort by sectionals_gate_numeric
    sectional_results = sectional_results.orderBy(*race_id_cols, "sectionals_gate_numeric")

    # Compute cumulative_sectional_time
    window_spec = Window.partitionBy(*race_id_cols).orderBy("sectionals_gate_numeric").rowsBetween(Window.unboundedPreceding, 0)
    sectional_results = sectional_results.withColumn(
        "cumulative_sectional_time",
        spark_sum("sectionals_sectional_time").over(window_spec)
    )

    # Create sec_time_stamp using earliest_time_stamp
    sectional_results = sectional_results.withColumn(
        "sec_time_stamp",
        add_seconds_udf(col("earliest_time_stamp"), col("cumulative_sectional_time"))
    )

    # Compute start_time and end_time for each section
    # The first section: start_time = earliest_time_stamp
    # Subsequent sections: start_time = previous sec_time_stamp
    section_window = Window.partitionBy(*race_id_cols).orderBy("sec_time_stamp")
    sectional_results = sectional_results.withColumn(
        "prev_sec_time_stamp", lag("sec_time_stamp").over(section_window)
    ).withColumn(
        "start_time", when(col("prev_sec_time_stamp").isNull(), col("earliest_time_stamp")).otherwise(col("prev_sec_time_stamp"))
    ).withColumn(
        "end_time", col("sec_time_stamp")
    )

    # Drop intermediate columns if no longer needed
    # Keep earliest_time_stamp for now until you are sure no step needs it further down.
    sectional_results = sectional_results.drop("cumulative_sectional_time", "prev_sec_time_stamp")

    # If you are absolutely sure earliest_time_stamp is no longer needed later, you can drop it here:
    # sectional_results = sectional_results.drop("earliest_time_stamp")

    print("sectional_results schema after merging:")
    sectional_results.printSchema()
    return sectional_results

def aggregate_gps_data(gpspoint, sectionals, race_id_cols):
    """
    Aggregate GPS data within each sectional interval using start_time and end_time from sectionals.
    sec_time_stamp, start_time, and end_time are already in 'sectionals'.
    No need to reference earliest_time_stamp here.
    """
    # Join gpspoint with sectionals on race_id_cols to get sec_time_stamp, start_time, end_time
    gps_with_sec = gpspoint.join(
        sectionals.select(*race_id_cols, "sec_time_stamp", "start_time", "end_time"),
        on=race_id_cols,
        how="inner"
    )

    # Filter gps by time interval of each section
    gps_filtered = gps_with_sec.filter(
        (col("time_stamp") >= col("start_time")) & (col("time_stamp") <= col("end_time"))
    )

    # Aggregate gps data per section
    gps_aggregated = gps_filtered.groupBy(*race_id_cols, "sec_time_stamp").agg(
        spark_avg("speed").alias("avg_speed"),
        spark_avg("stride_frequency").alias("avg_stride_frequency"),
        spark_sum("distance_traveled").alias("total_distance_traveled"),
        spark_count("time_stamp").alias("gps_point_count")
    )
    return gps_aggregated

def merge_gps_sectionals(spark, sectional_results, gpspoint, parquet_dir):
    """
    Merge GPS and sectional results data into one final DataFrame.
    """
    race_id_cols = ["course_cd", "race_date", "race_number", "saddle_cloth_number"]

    # Compute earliest_time_stamp
    first_time_df = gpspoint.groupBy(*race_id_cols).agg(
        spark_min("time_stamp").alias("earliest_time_stamp")
    )

    # Merge sectionals to get sec_time_stamp and intervals
    sectionals = merge_sectionals(sectional_results, race_id_cols, first_time_df)

    # Aggregate GPS data by section
    gps_aggregated = aggregate_gps_data(gpspoint, sectionals, race_id_cols)

    # Join aggregated GPS data with sectionals
    merged_df = sectionals.join(gps_aggregated, on=race_id_cols + ["sec_time_stamp"], how="left")

    # Remove duplicates if any
    primary_key_columns = ["course_cd", "race_date", "race_number", "saddle_cloth_number", "sec_time_stamp"]
    merged_df_de_dup = dup_check(merged_df, primary_key_columns)
    save_parquet(spark, merged_df_de_dup, "merged_df", parquet_dir)

    return merged_df_de_dup