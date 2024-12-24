from pyspark.sql.functions import (
    col, abs, min as spark_min, sum as spark_sum, mean as spark_mean, max as spark_max, count, when, upper, trim,
    udf, lit, row_number, lag, first, last, expr
)
import os
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from datetime import timedelta
from pyspark.sql import DataFrame
from src.data_preprocessing.data_prep1.data_utils import save_parquet

# UDF to add fractional seconds to a timestamp
def add_seconds(ts, seconds):
    if ts is None or seconds is None:
        return None
    return ts + timedelta(seconds=seconds)

add_seconds_udf = udf(add_seconds, TimestampType())

def merge_gps_sectionals(spark, parquet_dir):
    """
    Merge GPS and sectional results data.
    """
    gpspoint = spark.read.parquet(os.path.join(parquet_dir, "gpspoint.parquet"))
    sectionals = spark.read.parquet(os.path.join(parquet_dir, "merge_results_sectionals.parquet"))  
        
    race_id_cols = ["course_cd", "race_date", "race_number"]

    add_seconds_udf = udf(add_seconds, TimestampType())
        
    ############################################################################
    # 1) Compute earliest GPS time per race and horse
    ############################################################################
    
    gps_earliest_df = gpspoint.groupBy(*race_id_cols).agg(
        spark_min("time_stamp").alias("earliest_time_stamp_gps")
    )

    ############################################################################
    # 2) Sort the sectional data by gate_index
    ############################################################################
    # Just ensure sectionals are ordered by gate_index. 
    # We'll do cumulative sums after we join with earliest_time.
    sectionals_sorted = sectionals.orderBy(*race_id_cols, "saddle_cloth_number", "gate_index")
 
    ######################################
    # 3) Join the earliest GPS time with sectionals
    ######################################
    sectionals_with_earliest = sectionals_sorted.join(
        gps_earliest_df,
        on=race_id_cols,
        how="inner"
    )
    
    ######################################
    # 4) Compute sec_time_stamp in sectionals
    #
    # First, compute cumulative_sectional_time per race/horse ordered by gate_index.
    ######################################
    window_spec = Window.partitionBy(*race_id_cols, "saddle_cloth_number").orderBy("gate_index").rowsBetween(Window.unboundedPreceding, 0)
    sectionals_with_cum = sectionals_with_earliest.withColumn(
        "cumulative_sectional_time",
        spark_sum("sectionals_sectional_time").over(window_spec)
    )

    # Now add earliest_time_stamp_gps to cumulative_sectional_time to get sec_time_stamp
    sectionals_with_sec_time = sectionals_with_cum.withColumn(
        "sec_time_stamp",
        add_seconds_udf(col("earliest_time_stamp_gps"), col("cumulative_sectional_time"))
    )

    print("Computed sec_time_stamp by adding cumulative_sectional_time to earliest GPS timestamp.")
    sectionals_with_sec_time.printSchema()

    ######################################
    # 5) Create a temporary view to inspect the data
    #
    # We'll select a subset of columns that are essential for inspection:
    # course_cd, race_date, race_number, saddle_cloth_number, gate_name, gate_index, sec_time_stamp
    ######################################
    view_df = sectionals_with_sec_time.select(
    "course_cd", "race_date", "race_number", "saddle_cloth_number", "sectionals_gate_name", "sectionals_sectional_time","gate_index", "sec_time_stamp"
    ).orderBy(*race_id_cols, "gate_index")

    view_name = "sectionals_with_sec_time_view"
    view_df.createOrReplaceTempView(view_name)
    
    ################################################
    #  6) To aggregate GPS data for the interval leading up to each gate, you need a 
    # start_time and an end_time for that interval. A common approach:
    #  1. start_time for gate i: sec_time_stamp from the previous gate 
    #    (or the earliest GPS timestamp if it’s the first gate).
    #  2. end_time for gate i: sec_time_stamp of gate i itself.
    #
    #  1A. Sort Each Horse’s Sectionals by gate_index
    #  In 5) above I already sorted with:
    # window_spec = Window.partitionBy(*race_id_cols).orderBy("gate_index").rowsBetween(Window.unboundedPreceding, 0)
    # 
    # 1B. Use a lag Window Function to Get start_time
    #
    #  For each row (gate i), define the “start” as the sec_time_stamp of the previous gate i-1:

    # Window for each horse, ordered by gate_index
    w = Window.partitionBy("course_cd", "race_date", "race_number", "saddle_cloth_number").orderBy("gate_index")

    sectionals_intervals = sectionals_with_sec_time \
        .withColumn("start_time",
            lag("sec_time_stamp").over(w)
        )

    # If start_time is null (i.e., this is the first gate), default to earliest_time_stamp_gps 
    # or the same sec_time_stamp

    sectionals_intervals = sectionals_intervals.withColumn(
        "start_time",
        when(col("start_time").isNull(), col("earliest_time_stamp_gps"))
        .otherwise(col("start_time"))
    )

    # The "end_time" is simply this row's sec_time_stamp
    sectionals_intervals = sectionals_intervals.withColumn("end_time", col("sec_time_stamp"))

    sectionals_intervals.select(
        *race_id_cols, "saddle_cloth_number", "gate_index", "start_time", "end_time"
    ).show(10, truncate=False)

    ##############################################
    # 7) Join GPS data with sectionals_intervals
    # 	1.	Join on (course_cd, race_date, race_number, saddle_cloth_number) 
    #       so we only consider the correct horse in the correct race.
    # 	2.	Filter where gpspoint.time_stamp is between start_time and end_time.
    ##############################################

    interval_join = gpspoint.join(
        sectionals_intervals,
        on=["course_cd", "race_date", "race_number", "saddle_cloth_number"],
        how="inner"  # or 'left', if you want all intervals even if no GPS data
    ).filter(
        (col("time_stamp") >= col("start_time")) &
        (col("time_stamp") <= col("end_time"))
    )
     
    ############################################################################################
    # 8) Aggregate GPS data for each interval
    # After the filter, you have all rows that satisfy:
    # 	•	(course_cd, race_date, race_number, saddle_cloth_number) is the same
    # 	•	time_stamp is in [start_time, end_time]
    #
    # Now you can groupBy the unique ID of each gate interval. Typically, that’s:
    # 	•	(course_cd, race_date, race_number, saddle_cloth_number, gate_index)
    #
    # Then compute your aggregates:
    # 	1.	avg_speed: average of gpspoint.speed
    # 	2.	avg_stride_freq: average of gpspoint.stride_frequency
    # 	3.	gps_first_progress: the first progress in that interval
    # 	4.	gps_last_progress: the last progress in that interval
    # 	5.	gps_first_longitude, gps_first_latitude if desired
    # 	6.	gps_last_longitude, gps_last_latitude
    # 	7.	Possibly, max_speed, min_speed, etc.
    ############################################################################################
    
    aggregated = interval_join.groupBy(
            "course_cd", "race_date", "race_number", "saddle_cloth_number", "gate_index"
    ).agg(
        spark_mean("speed").alias("gps_section_avg_speed"),
        spark_mean("stride_frequency").alias("gps_section_avg_stride_freq"),
        first("progress").alias("gps_first_progress"),
        last("progress").alias("gps_last_progress"),
        first("location").alias("gps_first_location"),
        last("location").alias("gps_last_location"),
        count("*").alias("gps_num_points")  # how many points fell in the interval
    )
    
    ############################################################################################
    # 9) Rejoin Aggregates Back to Sectionals
    ############################################################################################
    final_df = sectionals_intervals.join(
        aggregated,
        on=[*race_id_cols, "saddle_cloth_number", "gate_index"],  # same grouping key
        how="inner"  # or 'inner' if you only want intervals that had GPS data
    )
    
    # Since 'gps_first_location' and 'gps_last_location' are not WKT but rather hex-encoded WKB (EWKB)
    final_df = final_df \
    .withColumn("gps_first_location_geom", expr("ST_GeomFromWKB(unhex(gps_first_location))")) \
    .withColumn("gps_last_location_geom", expr("ST_GeomFromWKB(unhex(gps_last_location))"))

    ############################################################################################
    # 10) How Many Intervals Have No GPS Data?
    #
    ############################################################################################

    missing_count = final_df.filter("gps_num_points IS NULL").count()
    print("Number of intervals with no GPS data:", missing_count)
    
    # If your gps_num_points is NULL or 0, you want to mark gps_coverage=False, otherwise True:
    final_df = final_df.withColumn(
        "gps_coverage",
        when((col("gps_num_points").isNotNull()) & (col("gps_num_points") > 0), lit(True))
        .otherwise(lit(False))
    )
    
    print("Marked intervals with no GPS data as 'gps_coverage=False'.")
    input("Press Enter to continue...")
    ############################################################################################
    # 11a.) Convert distance from Furlongs (F) to meters if dist_unit is F
    #    1 Furlong ≈ 201.168 meters.
    #    Then change dist_unit to 'm'.

    final_df = final_df.withColumn(
        "distance_meters",
        when(upper(trim(col("dist_unit"))) == "F", ((col("distance") / 100)) * lit(201.168))
        .otherwise(lit(None)))
    # Now final_df has "distance_meters" instead of "distance" / "dist_unit"

    final_df.printSchema()
    final_df.select("distance", "dist_unit", "distance_meters").show(10, truncate=False)
 
    # Step 2: (Optional) drop the old columns
    final_df = final_df.drop("distance", "dist_unit")


    # 11) Save the final_df to parquet
    ############################################################################################
    
    # Now you can save
    save_parquet(spark, final_df, "merge_gps_sectionals_agg", parquet_dir)
    
    return final_df
    
    