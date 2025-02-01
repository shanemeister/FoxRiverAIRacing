import os
import logging
import time
import pprint
from src.data_preprocessing.data_prep2.data_healthcheck import time_series_data_healthcheck
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from pyspark.sql.functions import (col, count, row_number, abs, unix_timestamp,
                                   when, lit, min as F_min, max as F_max , upper, trim,
                                   row_number, mean as F_mean, countDistinct, last, first, when)
from src.train_and_predict.inference_sql_queries import full_query

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

def manage_sentinel_values(df):
    tpd_cols = [
    "avg_dist_bk_gate2_5", "avg_dist_bk_gate3_5",
    "avg_dist_bk_gate4_5", "avg_speed_fullrace_5",
    "avg_strfreq_q1_5", "avg_strfreq_q2_5",
    "avg_strfreq_q3_5", "avg_strfreq_q4_5",
    "avg_stride_length_5", "avg_dist_bk_gate1_5",
    "speed_improvement"
]
    # 1) Define a "gps_present" flag
    df = df.withColumn(
        "gps_present",
        when(
            col("avg_speed_fullrace_5").isNotNull() &
            col("avg_strfreq_q2_5").isNotNull() &
            col("avg_strfreq_q3_5").isNotNull(), 
            1
        ).otherwise(0))

    # 2) Sentinel fill for each TPD column
    for c in tpd_cols:
        df = df.withColumn(
            c, 
            F.when(F.col(c).isNull(), F.lit(0)).otherwise(F.col(c))
        )

    # Now each TPD column is numeric, either real data or -999.
    # The model also has gps_present to learn "missing vs. present."
    
    return df

def fix_outliers(df):
    """
    1) Hard-code certain known columns with suspicious extremes.
    2) Use percentile-based approach for columns we want to clamp in a data-driven manner.
    """
    # Step A: Hard-coded outlier caps
    outlier_bounds = {
        "avg_beaten_len_5": (0, 50.0),
        "days_off": (0, 365.0),
        "avgspd": (0, 120.0),
        "avg_workout_rank_3": (0, 60.0),
        "time_behind": (0, 60),
        "speed_improvement": (-20, 50),
        "sire_itm_percentage": (0, 1),
        "sire_roi": (-100, 10000),
        "dam_itm_percentage": (0, 1),
        "dam_roi": (-100, 5000),
        "avg_dist_bk_gate4_5": (0,50),
        "avg_dist_bk_gate3_5":(0,50),
        "avg_dist_bk_gate2_5":(0,50),
        "avg_dist_bk_gate1_5":(0,50),
        "age_at_race_day":(1.5, 10),
    }

    for col_name, (min_val, max_val) in outlier_bounds.items():
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) < min_val, min_val)
             .when(F.col(col_name) > max_val, max_val)
             .otherwise(F.col(col_name))
        )

    # Step B: Data-driven approach for certain columns
    cols_for_stat_clamp = ["claimprice", "purse", "avg_speed_5"]  # example
    for c in cols_for_stat_clamp:
        # 1% and 99% quantiles
        bounds = df.approxQuantile(c, [0.01, 0.99], 0.001)
        lower, upper = bounds[0], bounds[1]

        df = df.withColumn(
            c,
            F.when(F.col(c) < lower, lower)
             .when(F.col(c) > upper, upper)
             .otherwise(F.col(c))
        )

    return df

def load_merged_race_data(spark, jdbc_url, jdbc_properties, parquet_dir):
    """
    Load both future_races and historical_data from PostgreSQL,
    then create two DataFrames:
        1) future_df (only the future races)
        2) filtered_historical_data (only historical rows for horses in future races)

    Finally, we'll join them on horse_id to produce merged_df, 
    log the schema/row count, and return merged_df.
    """
    queries = full_query()
    future_df = None
    hist_df = None

    # 1) Load both DataFrames from PostgreSQL
    for name, query in queries.items():
        if name in ["future_races", "historical_data"]:
            logging.info(f"Loading '{name}' from PostgreSQL...")
            start_time = time.time()
            temp_df = spark.read.jdbc(
                url=jdbc_url,
                table=f"({query}) as subquery",
                properties=jdbc_properties
            )
            load_time = time.time() - start_time
            count_temp = temp_df.count()
            logging.info(f"'{name}' loaded in {load_time:.2f} seconds. Count: {count_temp}")

            if name == "future_races":
                future_df = temp_df
            else:
                hist_df = temp_df
    
    healthcheck_report = time_series_data_healthcheck(future_df)
    pprint.pprint(healthcheck_report)
            
    # Ensure both are loaded
    if future_df is None or hist_df is None:
        raise ValueError("Failed to load either 'future_races' or 'hist_df' from queries.")

    hist_df = hist_df.withColumn("data_flag", lit("historical"))
    future_df = future_df.withColumn("data_flag", lit("future"))
    
    # 2) Filter hist_df to only the horses in the future_races
    future_race_horse_ids = future_df.select('horse_id').distinct()

    filtered_hist_df = hist_df.join(
        future_race_horse_ids,
        on="horse_id",
        how="inner"  # keeps only historical rows that match these horse_ids
    )

    healthcheck_report = time_series_data_healthcheck(filtered_hist_df)
    pprint.pprint(healthcheck_report)
    
    # Ensure they have the same set of columns
    combined_df = filtered_hist_df.unionByName(future_df)

    # 4) Log schema and row count properly
    logging.info("Schema of merged_df:")
    combined_df.printSchema()  # This will print to stdout/logs, not return a string

    row_count = combined_df.count()
    logging.info(f"combined_df row count: {row_count}")

    return combined_df

def load_base_inference_data(spark, jdbc_url, jdbc_properties, parquet_dir):
    """
    Load Parquet file used to train
    """
    future_races = load_merged_race_data(spark, jdbc_url, jdbc_properties, parquet_dir)

    # Check for Dups:
    logging.info("Checking for duplicates on primary keys...")
    primary_keys = ["course_cd", "race_date", "race_number", "horse_id"]
    duplicates = (
        future_races.groupBy(*primary_keys)
        .agg(F.count("*").alias("cnt"))
        .filter(F.col("cnt") > 1)
    )

    # Log the number of duplicates found
    num_duplicates = duplicates.count()
    logging.info(f"Number of duplicate records found: {num_duplicates}")

    # Show duplicates if any
    if num_duplicates > 0:
        logging.info("Duplicate records found:")
        duplicates.show()
    else:
        logging.info("No duplicate records found.")
    
    logging.info("Convert Decimal Columns to Double.")
    # 2. Convert Decimal Columns to Double
    decimal_cols = ["weight", "power", "distance_meters", "morn_odds", "total_races_5", "avg_fin_5",
                    "class_rating", "all_earnings", "cond_earnings","purse", "best_speed",
                "jock_win_percent", "jock_itm_percent", "trainer_itm_percent", 
                    "trainer_win_percent", "jt_win_percent", "jt_itm_percent",
                    "jock_win_track", "jock_itm_track", "trainer_win_track", "trainer_itm_track",
                    "jt_win_track", "jt_itm_track", 'previous_distance', 'horse_itm_percentage' ]
    for col_name in decimal_cols:
        future_races = future_races.withColumn(col_name, F.col(col_name).cast("double"))
    logging.info("Decimal columns converted to double.")
    logging.info("2. Decimal columns converted to double.")
    logging.info("Imputing date_of_birth with median date.")
    # 3b. Create age_at_race_day
    future_races = future_races.withColumn(
        "age_at_race_day",
        F.datediff(F.col("race_date"), F.col("date_of_birth")) / 365.25
    )
    logging.info("Created age_at_race_day.")
    print("3b. Created age_at_race_day.")

    logging.info("Imputing categorical and numeric columns.")
    
    # 3c. Impute categorical and numeric columns -- ensure no whitespace in categorical columns
    categorical_defaults = { "turf_mud_mark": "MISSING", "layoff_cat": "MISSING", "trk_cond": "MISSING", "med": "NONE" , 
                            "surface": "MISSING", "previous_surface": "MISSING"}    
    # Fill missing values for categorical defaults
    future_races = future_races.fillna(categorical_defaults)
    # Impute med with NONE
    future_races = future_races.withColumn("med", when(col("med") == "", "NONE").otherwise(col("med")))
    # Impute turf_mud_mark with MISSING
    future_races = future_races.withColumn("turf_mud_mark",when(col("turf_mud_mark") == "", "MISSING").otherwise(col("turf_mud_mark")))

    # Impute horse_itm_percentage with 0 when it is null
    future_races = future_races.withColumn("horse_itm_percentage", when(col("horse_itm_percentage").isNull(), 0).otherwise(col("horse_itm_percentage")))
    
    # Set empty values to 0 for prev_speed and count_workouts_3
    future_races = future_races.withColumn("prev_speed", when(col("prev_speed").isNull(), 0).otherwise(col("prev_speed")))
    future_races = future_races.withColumn("count_workouts_3", when(col("count_workouts_3").isNull(), 0).otherwise(col("count_workouts_3")))

    future_races = manage_sentinel_values(future_races)

    columns_to_fill = [
        'all_earnings', 'all_fourth', 'all_place', 'all_show', 'all_starts', 'all_win', 
        'cond_earnings', 'cond_fourth', 'cond_place', 'cond_show', 'cond_starts', 'cond_win', 'days_off', 
        'jock_itm_percent', 'jock_itm_track', 'jock_win_percent', 'jock_win_track', 'jt_itm_percent', 
        'jt_itm_track', 'jt_win_percent', 'jt_win_track', 'trainer_itm_percent', 'trainer_itm_track', 
        'trainer_win_percent', 'trainer_win_track', 'net_sentiment','prev_race_date', 'first_race_date_5', 'most_recent_race_5', 
        'avg_fin_5', 'avg_speed_5', 'avg_workout_rank_3', 'sire_roi', 'dam_roi', 'sire_itm_percentage', 'dam_itm_percentage']
    logging.info("Filling missing values for columns.")
    for column in columns_to_fill:
        if column == 'prev_race_date':
            # If null, fill with '1970-01-01' as a date literal
            future_races = future_races.withColumn(
                column,
                when(col(column).isNull(), F.to_date(F.lit("1970-01-01"), "yyyy-MM-dd"))
                .otherwise(col(column))
            )
        elif column == 'first_race_date_5':
            # If null, fill with '1970-01-01' as a date literal
            future_races = future_races.withColumn(
                column,
                when(col(column).isNull(), F.to_date(F.lit("1970-01-01"), "yyyy-MM-dd"))
                .otherwise(col(column))
            )
        elif column == 'most_recent_race_5':
            # If null, fill with '1970-01-01' as a date literal
            future_races = future_races.withColumn(
                column,
                when(col(column).isNull(), F.to_date(F.lit("1970-01-01"), "yyyy-MM-dd"))
                .otherwise(col(column))
            )
        else:
            # If null, fill with 0 (for numeric columns)
            future_races = future_races.withColumn(
                column,
                when(col(column).isNull(), lit(0)).otherwise(col(column))
            )
        
    logging.info("Numeric columns cast to double.")
    numeric_cols = ["race_number","horse_id","purse","weight","claimprice","distance_meters","time_behind","pace_delta_time",
                    "class_rating","prev_speed_rating","previous_class","previous_distance",
                    "off_finish_last_race","power","horse_itm_percentage","avgspd","net_sentiment","avg_spd_sd",
                    "ave_cl_sd","hi_spd_sd","pstyerl","all_starts","all_win","all_place","all_show","all_fourth",
                    "all_earnings","cond_starts","cond_win","cond_place","cond_show","cond_fourth","cond_earnings",
                    "total_races_5","avg_fin_5","avg_speed_5","best_speed","avg_beaten_len_5",
                    "avg_dist_bk_gate1_5","avg_dist_bk_gate2_5","avg_dist_bk_gate3_5",
                    "avg_dist_bk_gate4_5","avg_speed_fullrace_5","avg_stride_length_5","avg_strfreq_q1_5",
                    "avg_strfreq_q2_5","avg_strfreq_q3_5","avg_strfreq_q4_5","prev_speed","speed_improvement",
                    "days_off","avg_workout_rank_3","count_workouts_3","race_count",
                    "jock_win_percent","jock_itm_percent","trainer_win_percent","trainer_itm_percent","jt_win_percent",
                    "jt_itm_percent","jock_win_track","jock_itm_track","trainer_win_track","trainer_itm_track","jt_win_track",
                    "jt_itm_track", "sire_itm_percentage", "sire_roi", "dam_itm_percentage", "dam_roi"]
    
    for col_name in numeric_cols:
        future_races = future_races.withColumn(col_name, F.col(col_name).cast("double"))
      
    # Example usage:
    future_races = fix_outliers(future_races)
    
    logging.info("Starting the write to parquet.")
    start_time = time.time()
    future_races.write.mode("overwrite").parquet(f"{parquet_dir}/future_races")
    logging.info(f"Data written to Parquet in {time.time() - start_time:.2f} seconds")
    logging.info("Data cleansing complete. future_races being returned.")
    
    return future_races
        