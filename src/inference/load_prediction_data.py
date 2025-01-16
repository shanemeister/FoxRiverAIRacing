import os
import logging
import time
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from pyspark.sql.functions import (col, count, row_number, abs, unix_timestamp,  
                                   when, lit, min as F_min, max as F_max , upper, trim,
                                   row_number, mean as F_mean, countDistinct, last, first, when)
from src.inference.inference_sql_queries import sql_queries
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql

def impute_date_of_birth_with_median(df):
    """  
    Impute date_of_birth with the median value (or a default if no data exists).   
    """
    race_df = race_df.withColumn("date_of_birth_ts", F.col("date_of_birth").cast("timestamp").cast("long"))
    median_window = Window.orderBy("date_of_birth_ts")

    median_ts = race_df.filter(F.col("date_of_birth_ts").isNotNull()).approxQuantile("date_of_birth_ts", [0.5], 0)[0]
    if median_ts is None:
        median_date = F.lit("2000-01-01").cast("date")
    else:
        median_date = F.from_unixtime(F.lit(median_ts)).cast("date")

    race_df = race_df.withColumn(
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

    # Now each TPD column is numeric, either real data or 0.
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

def load_base_inference_data(spark, jdbc_url, jdbc_properties, parquet_dir):
    """
    Load Parquet file used to train
    """
    race_df = None
    queries = sql_queries()
    
    for name, query in queries.items():
        if name == "infer_results":
            race_df = spark.read.jdbc(
                url=jdbc_url,
                table=f"({query}) AS subquery",
                properties=jdbc_properties
            )
            race_df.printSchema()
    
    # output_path = os.path.join(parquet_dir, f"{name}.parquet")
    # logging.info(f"Saving {name} DataFrame to Parquet at {output_path}...")
    # race_df.write.mode("overwrite").parquet(output_path)
    # logging.info(f"{name} data loaded and saved successfully.")
    
    # Check for Dups:
    logging.info("Checking for duplicates on primary keys...")
    primary_keys = ["course_cd", "race_date", "race_number", "horse_id"]
    duplicates = (
        race_df.groupBy(*primary_keys)
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
                    "jt_win_track", "jt_itm_track", 'previous_distance' ]
    for col_name in decimal_cols:
        race_df = race_df.withColumn(col_name, F.col(col_name).cast("double"))
    logging.info("Decimal columns converted to double.")
    print("2. Decimal columns converted to double.")
    logging.info("Imputing date_of_birth with median date.")
    # 3b. Create age_at_race_day
    race_df = race_df.withColumn(
        "age_at_race_day",
        F.datediff(F.col("race_date"), F.col("date_of_birth")) / 365.25
    )
    logging.info("Created age_at_race_day.")
    print("3b. Created age_at_race_day.")
    
    logging.info("Imputing categorical and numeric columns.")
    # 3c. Impute categorical and numeric columns -- ensure no whitespace in categorical columns
    categorical_defaults = { "turf_mud_mark": "MISSING", "layoff_cat": "MISSING", "med": "NONE" }
    # Fill missing values for categorical defaults
    race_df = race_df.fillna(categorical_defaults)
    # Impute med with NONE
    race_df = race_df.withColumn("med", when(col("med") == "", "NONE").otherwise(col("med")))
    # Impute turf_mud_mark with MISSING
    race_df = race_df.withColumn("turf_mud_mark",when(col("turf_mud_mark") == "", "MISSING").otherwise(col("turf_mud_mark")))

    race_df = manage_sentinel_values(race_df)
    
    columns_to_fill = [
        'all_earnings', 'all_fourth', 'all_place', 'all_show', 'all_starts', 'all_win', 
        'cond_earnings', 'cond_fourth', 'cond_place', 'cond_show', 'cond_starts', 'cond_win', 'days_off', 
        'jock_itm_percent', 'jock_itm_track', 'jock_win_percent', 'jock_win_track', 'jt_itm_percent', 
        'jt_itm_track', 'jt_win_percent', 'jt_win_track', 'trainer_itm_percent', 'trainer_itm_track', 
        'trainer_win_percent', 'trainer_win_track', 'net_sentiment','prev_race_date', 'first_race_date_5', 'most_recent_race_5', 
        'avg_fin_5', 'avg_speed_5', 'avg_workout_rank_3', 
        'best_speed', 'count_workouts_3', 'prev_speed', 'avg_beaten_len_5', 'total_races_5']
    logging.info("Filling missing values for columns.")
    for column in columns_to_fill:
        if column == 'prev_race_date':
            # If null, fill with '1900-01-01' as a date literal
            race_df = race_df.withColumn(
                column,
                when(col(column).isNull(), F.to_date(F.lit("1900-01-01"), "yyyy-MM-dd"))
                .otherwise(col(column))
            )
        elif column == 'first_race_date_5':
            # If null, fill with '1900-01-01' as a date literal
            race_df = race_df.withColumn(
                column,
                when(col(column).isNull(), F.to_date(F.lit("1900-01-01"), "yyyy-MM-dd"))
                .otherwise(col(column))
            )
        elif column == 'most_recent_race_5':
            # If null, fill with '1900-01-01' as a date literal
            race_df = race_df.withColumn(
                column,
                when(col(column).isNull(), F.to_date(F.lit("1900-01-01"), "yyyy-MM-dd"))
                .otherwise(col(column))
            )
        else:
            # If null, fill with 0 (for numeric columns)
            race_df = race_df.withColumn(
                column,
                when(col(column).isNull(), lit(0)).otherwise(col(column))
            )
        
    logging.info("Numeric columns cast to double.")
    numeric_cols = ['race_number','horse_id','purse','weight','claimprice','power','morn_odds','avgspd','class_rating',
                    'net_sentiment','avg_spd_sd','ave_cl_sd','hi_spd_sd','pstyerl','all_starts','all_win','all_place',
                    'all_show','all_fourth','all_earnings','cond_starts','cond_win','cond_place','cond_show','cond_fourth',
                    'cond_earnings','avg_speed_5','best_speed','avg_beaten_len_5','avg_dist_bk_gate1_5','avg_dist_bk_gate2_5',
                    'avg_dist_bk_gate3_5','avg_dist_bk_gate4_5','avg_speed_fullrace_5','avg_stride_length_5','avg_strfreq_q1_5',
                    'avg_strfreq_q2_5','avg_strfreq_q3_5','avg_strfreq_q4_5','prev_speed','speed_improvement','days_off',
                    'avg_workout_rank_3','jock_win_percent','jock_itm_percent','trainer_win_percent','trainer_itm_percent',
                    'jt_win_percent','jt_itm_percent','jock_win_track','jock_itm_track','trainer_win_track','trainer_itm_track',
                    'jt_win_track','jt_itm_track','age_at_race_day','distance_meters', 'previous_distance', 'count_workouts_3',
                    'off_finish_last_race', 'previous_class' , 'race_count' ]
    
    for col_name in numeric_cols:
        race_df = race_df.withColumn(col_name, F.col(col_name).cast("double"))
    
          
    # Example usage:
    race_df = fix_outliers(race_df)
        
    logging.info("Starting the write to parquet.")
    start_time = time.time()
    race_df.write.mode("overwrite").parquet(f"{parquet_dir}/predict")
    #save_parquet(spark, training_data, "training_data", parquet_dir)
    logging.info(f"Data written to Parquet in {time.time() - start_time:.2f} seconds")
    logging.info("Data cleansing complete. race_df being returned.")
    
    return race_df
        
        