import os
import logging
import time
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import (col, count, row_number, abs, unix_timestamp,  
                                   when, lit, min as F_min, max as F_max , upper, trim,
                                   row_number, mean as F_mean, countDistinct, last, first, when)
from src.train_and_predict.training_sql_queries import sql_queries
from pyspark.sql.functions import current_date

def impute_date_of_birth_with_median(df):
    """  
    Impute date_of_birth with the median value (or a default if no data exists).   
    """
    df = df.withColumn("date_of_birth_ts", F.col("date_of_birth").cast("timestamp").cast("long"))
    
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
    
    # Log the counts
    future_count = df.filter(F.col("data_flag") == "future").count()
    historical_count = df.filter(F.col("data_flag") == "historical").count()
    logging.info(f"3. After processing impute_data_of_birth_with_median function: Number of rows with data_flag='future': {future_count}")
    logging.info(f"3. After processing impute_data_of_birth_with_median function:  Number of rows with data_flag='historical': {historical_count}")
    
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
    
    # Log the counts
    future_count = df.filter(F.col("data_flag") == "future").count()
    historical_count = df.filter(F.col("data_flag") == "historical").count()
    logging.info(f"4. After processing manage_sentinel_values function: Number of rows with data_flag='future': {future_count}")
    logging.info(f"4. After processing manage_sentinel_values function:  Number of rows with data_flag='historical': {historical_count}")
   
    
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

from pyspark.sql import Window
import pyspark.sql.functions as F

def update_missing_tpd(df):
    """
    For each race (grouped by course_cd, race_date, race_number), update the key fields as follows:
      - For "dist_bk_gate4": if official_fin == 1, force the value to 0.
      - For any key field (dist_bk_gate4, running_time, total_distance_ran) that is null,
        fill it with the race-level mean; if that is null, fill with the global mean.
    In addition, the function adds two flag columns:
      - missing_gps_flag: set to 1 if any key field was originally missing, otherwise 0.
      - gps_present: 0 if missing, 1 if complete.
    Rows are never dropped.
    """
    # Define key columns and race grouping keys.
    key_cols = ["dist_bk_gate4", "running_time", "total_distance_ran"]
    race_keys = ["course_cd", "race_date", "race_number"]

    # STEP 1: Compute a flag from the original data (before imputation)
    # Create a combined condition: if any key column is null then flag = 1, else 0.
    condition = F.lit(False)
    for col_name in key_cols:
        condition = condition | F.col(col_name).isNull()
    df = df.withColumn("original_missing_flag", F.when(condition, F.lit(1)).otherwise(F.lit(0)))
    df = df.withColumn("original_gps_present", F.when(condition, F.lit(0)).otherwise(F.lit(1)))

    # STEP 2: Compute race-level means for each key column.
    race_window = Window.partitionBy(*race_keys)
    for col_name in key_cols:
        df = df.withColumn(f"race_mean_{col_name}", F.avg(F.col(col_name)).over(race_window))
    
    # STEP 3: Compute global means for each key column.
    global_means = {}
    for col_name in key_cols:
        global_mean = df.select(F.mean(F.col(col_name)).alias("global_mean")).collect()[0]["global_mean"]
        global_means[col_name] = global_mean

    # STEP 4: Impute each key column.
    # For "dist_bk_gate4": if official_fin == 1, force to 0;
    df = df.withColumn(
        "dist_bk_gate4",
        F.when(F.col("official_fin") == 1, F.lit(0.0))
         .otherwise(
             F.when(F.col("dist_bk_gate4").isNull(),
                    F.when(F.col("race_mean_dist_bk_gate4").isNotNull(), F.col("race_mean_dist_bk_gate4"))
                     .otherwise(F.lit(global_means["dist_bk_gate4"])))
             .otherwise(F.col("dist_bk_gate4"))
         )
    )
    # For running_time and total_distance_ran: if null, use race mean if available; otherwise use global mean.
    for col_name in ["running_time", "total_distance_ran"]:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name).isNull(),
                   F.when(F.col(f"race_mean_{col_name}").isNotNull(), F.col(f"race_mean_{col_name}"))
                    .otherwise(F.lit(global_means[col_name])))
            .otherwise(F.col(col_name))
        )

    # STEP 5: Restore the original missing flag into new columns "missing_gps_flag" and "gps_present"
    df = df.withColumn("missing_gps_flag", F.col("original_missing_flag"))
    df = df.withColumn("gps_present", F.col("original_gps_present"))
    
    # STEP 6: Drop the temporary columns.
    for col_name in key_cols:
        df = df.drop(f"race_mean_{col_name}")
    df = df.drop("original_missing_flag", "original_gps_present")
    
    # Log the counts
    future_count = df.filter(F.col("data_flag") == "future").count()
    historical_count = df.filter(F.col("data_flag") == "historical").count()
    logging.info(f"2. After processing update_missing_tpd function: Number of rows with data_flag='future': {future_count}")
    logging.info(f"2. After processing update_missing_tpd function:  Number of rows with data_flag='historical': {historical_count}")
    
    logging.info("Rows in data_flag: train_df['data_flag'] = 'future'")
    
    return df

def drop_historical_missing_official_fin(df):
    """
    Remove rows from the DataFrame where:
      - data_flag is 'historical' AND
      - official_fin is null.
    Future races (data_flag != 'historical') are kept even if official_fin is null.
    """
    return df.filter(~((F.col("data_flag") == "historical") & (F.col("official_fin").isNull())))

def impute_performance_columns(df):
    """
    For the following columns:
      - time_behind:
            • If official_fin == 1 then set to 0.
            • Otherwise, if null, fill with the race-level mean;
              if race-level mean is null, fill with the global mean.
      - pace_delta_time, speed_rating, prev_speed_rating:
            • For missing values, fill with the race-level mean;
              if that is null, then fill with the global mean.
    Assumes the race grouping keys are course_cd, race_date, race_number.
    Returns the updated DataFrame.
    """
    # List of performance columns to impute.
    performance_cols = ["time_behind", "pace_delta_time", "speed_rating", "prev_speed_rating"]
    race_keys = ["course_cd", "race_date", "race_number"]
    
    # Define a window partitioned by the race grouping keys.
    race_window = Window.partitionBy(*race_keys)
    
    for col_name in performance_cols:
        # Compute the race-level mean for the column.
        df = df.withColumn(f"race_mean_{col_name}", F.avg(F.col(col_name)).over(race_window))
        # Compute the global mean (as a Python float).
        global_mean = df.select(F.mean(F.col(col_name)).alias("global_mean")).collect()[0]["global_mean"]
        
        if col_name == "time_behind":
            # For time_behind, if official_fin==1 then force to 0.
            df = df.withColumn(
                col_name,
                F.when(F.col("official_fin") == 1, F.lit(0.0))
                 .otherwise(
                     F.when(F.col(col_name).isNull(),
                            F.when(F.col(f"race_mean_{col_name}").isNotNull(), F.col(f"race_mean_{col_name}"))
                             .otherwise(F.lit(global_mean)))
                     .otherwise(F.col(col_name))
                 )
            )
        else:
            # For the remaining three columns: pace_delta_time, speed_rating, prev_speed_rating,
            # if missing, fill with the race-level mean; if that is null, use the global mean.
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(),
                       F.when(F.col(f"race_mean_{col_name}").isNotNull(), F.col(f"race_mean_{col_name}"))
                        .otherwise(F.lit(global_mean)))
                .otherwise(F.col(col_name))
            )
        
        # Drop the temporary race-level mean column.
        df = df.drop(f"race_mean_{col_name}")
    
    return df

from pyspark.sql import Window
import pyspark.sql.functions as F

def impute_performance_features(df):
    """
    Impute missing values for the performance features according to the following rules:
    
    Group 1 (fill with race mean if available; if not, use global mean):
      - best_speed
      - distance_meters
      - off_finish_last_race
      - previous_class
      - previous_distance

    Group 2 (fill with 0):
      - race_count
      - starts
      - total_races_5

    The race grouping keys are assumed to be: course_cd, race_date, race_number.
    """
    # Define race grouping keys
    race_keys = ["course_cd", "race_date", "race_number"]
    
    # Group 1 columns: fill with race-level mean; if missing, use global mean.
    group1 = ["best_speed", "distance_meters", "off_finish_last_race", "previous_class", "previous_distance", "avg_beaten_len_5"]
    
    # Create a window partitioned by the race keys.
    race_window = Window.partitionBy(*race_keys)
    
    for col_name in group1:
        # Compute race-level mean for the column.
        df = df.withColumn(f"race_mean_{col_name}", F.avg(F.col(col_name)).over(race_window))
        # Compute global mean (collect as a Python float)
        global_mean = df.select(F.mean(F.col(col_name)).alias("global_mean")).collect()[0]["global_mean"]
        # Impute: if the column is null, then use the race-level mean if not null; otherwise use the global mean.
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name).isNull(),
                   F.when(F.col(f"race_mean_{col_name}").isNotNull(), F.col(f"race_mean_{col_name}"))
                    .otherwise(F.lit(global_mean))
                  ).otherwise(F.col(col_name))
        )
        # Drop the temporary race mean column.
        df = df.drop(f"race_mean_{col_name}")
    
    # Group 2 columns: fill with 0 when null.
    group2 = ["race_count", "starts", "total_races_5"]
    for col_name in group2:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name).isNull(), F.lit(0)).otherwise(F.col(col_name))
        )
    
    return df

def filter_course_cd(train_df):
    # List of course_cd identifiers to keep for "future" data_flag
    course_cd_list = [
        'CNL', 'SAR', 'PIM', 'TSA', 'BEL', 'MVR', 'TWO', 'CLS', 'KEE', 'TAM', 'TTP', 'TKD', 
        'ELP', 'PEN', 'HOU', 'DMR', 'TLS', 'AQU', 'MTH', 'TGP', 'TGG', 'CBY', 'LRL', 
        'TED', 'IND', 'CTD', 'ASD', 'TCD', 'LAD', 'TOP'
    ]
    
    # Filter the DataFrame
    filtered_df = train_df.filter(
        (F.col("data_flag") == "historical") | 
        ((F.col("data_flag") == "future") & (F.col("course_cd").isin(course_cd_list)))
    )
    
    return filtered_df

def load_base_training_data(spark, jdbc_url, jdbc_properties, parquet_dir):
    """
    Load Parquet file used to train
    """
    train_df = None
    queries = sql_queries()
    for name, query in queries.items():
        if name == "training_data":
            logging.info("Query training_data located and loading from PostgreSQL...")
            start_time = time.time()
            train_df = spark.read.jdbc(
                url=jdbc_url,
                table=f"({query}) AS subquery",
                properties=jdbc_properties
            )
            logging.info(f"Data loaded from PostgreSQL in {time.time() - start_time:.2f} seconds.")

    if train_df is None:
        logging.error("No training_data query found; train_df is not defined.")
        # Handle the error or exit
    else:
        train_df.cache()
        rows_train_if = train_df.count()
        logging.info(f"Data loaded from PostgreSQL. Count: {rows_train_if}")

    future_count = train_df.filter(F.col("data_flag") == "future").count()
    historical_count = train_df.filter(F.col("data_flag") == "historical").count()
    logging.info(f"Before filtering: Number of rows with data_flag='future': {future_count}")
    logging.info(f"Before filtering: Number of rows with data_flag='historical': {historical_count}")
    
    # Apply the filter to the train_df DataFrame
    train_df = filter_course_cd(train_df)        
    
    # Log the counts after filtering
    future_count = train_df.filter(F.col("data_flag") == "future").count()
    historical_count = train_df.filter(F.col("data_flag") == "historical").count()
    logging.info(f"After filtering: Number of rows with data_flag='future': {future_count}")
    logging.info(f"After filtering: Number of rows with data_flag='historical': {historical_count}")

    train_df.printSchema()
    row_count = train_df.count()
    logging.info(f"Row count: {row_count}")
    logging.info(f"Count operation completed in {time.time() - start_time:.2f} seconds.")

    # Count rows with data_flag = "future"
    
    future_count = train_df.filter(F.col("data_flag") == "future").count()

    # Count rows with data_flag = "historical"
    historical_count = train_df.filter(F.col("data_flag") == "historical").count()

    # Log the counts
    logging.info(f"Before processing: Number of rows with data_flag='future': {future_count}")
    logging.info(f"Before processing: Number of rows with data_flag='historical': {historical_count}")
    
    logging.info("Rows in data_flag: train_df['data_flag'] = 'future'")
    
    # Impute dist_bk_gate4, running_time, total_distance_ran
    train_df = update_missing_tpd(train_df)
    
    # Check for Dups:
    logging.info("Checking for duplicates on primary keys...")
    primary_keys = ["course_cd", "race_date", "race_number", "horse_id"]
    duplicates = (
        train_df.groupBy(*primary_keys)
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
        train_df = train_df.withColumn(col_name, F.col(col_name).cast("double"))
    logging.info("Decimal columns converted to double.")
    print("2. Decimal columns converted to double.")
    
    train_df=impute_date_of_birth_with_median(train_df)
    
    logging.info("Imputing date_of_birth with median date.")
    # 3b. Create age_at_race_day
    train_df = train_df.withColumn(
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
    train_df = train_df.fillna(categorical_defaults)
    # Impute med with NONE
    train_df = train_df.withColumn("med", when(col("med") == "", "NONE").otherwise(col("med")))
    # Impute turf_mud_mark with MISSING
    train_df = train_df.withColumn("turf_mud_mark",when(col("turf_mud_mark") == "", "MISSING").otherwise(col("turf_mud_mark")))

    # Impute horse_itm_percentage with 0 when it is null
    train_df = train_df.withColumn("horse_itm_percentage", when(col("horse_itm_percentage").isNull(), 0).otherwise(col("horse_itm_percentage")))
    
    # Set empty values to 0 for prev_speed and count_workouts_3
    train_df = train_df.withColumn("prev_speed", when(col("prev_speed").isNull(), 0).otherwise(col("prev_speed")))
    train_df = train_df.withColumn("count_workouts_3", when(col("count_workouts_3").isNull(), 0).otherwise(col("count_workouts_3")))

    train_df = manage_sentinel_values(train_df)

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
            train_df = train_df.withColumn(
                column,
                when(col(column).isNull(), F.to_date(F.lit("1970-01-01"), "yyyy-MM-dd"))
                .otherwise(col(column))
            )
        elif column == 'first_race_date_5':
            # If null, fill with '1970-01-01' as a date literal
            train_df = train_df.withColumn(
                column,
                when(col(column).isNull(), F.to_date(F.lit("1970-01-01"), "yyyy-MM-dd"))
                .otherwise(col(column))
            )
        elif column == 'most_recent_race_5':
            # If null, fill with '1970-01-01' as a date literal
            train_df = train_df.withColumn(
                column,
                when(col(column).isNull(), F.to_date(F.lit("1970-01-01"), "yyyy-MM-dd"))
                .otherwise(col(column))
            )
        else:
            # If null, fill with 0 (for numeric columns)
            train_df = train_df.withColumn(
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
        train_df = train_df.withColumn(col_name, F.col(col_name).cast("double"))
      
    # Example usage:
    train_df = fix_outliers(train_df)
    
    # train_df = train_df.withColumn(
    #     "data_flag",
    #     F.when(F.col("race_date") < current_date(), lit("historical"))
    #      .otherwise(lit("future"))
    # )
    
    # # critical_cols = ["speed_rating"]  # columns you consider critical
    # # train_df = train_df.na.drop(subset=critical_cols)
    cols_to_fill = ['distance_meters',
                    'off_finish_last_race',
                    'pace_delta_time',
                    'prev_speed_rating',
                    'previous_class',
                    'previous_distance',
                    'race_count',
                    'speed_rating',
                    'starts',
                    'time_behind']

    # Window for each horse, ordered by race_date ascending
    # The `.rowsBetween(Window.unboundedPreceding, 0)` ensures 
    # "last()" sees all preceding rows in that partition, up to the current row.
    win = Window.partitionBy("horse_id").orderBy("race_date").rowsBetween(Window.unboundedPreceding, 0)

    for c in cols_to_fill:
        train_df = train_df.withColumn(
            c,
            F.last(train_df[c], ignorenulls=True).over(win)
        )  


    # Log the counts
    future_count = train_df.filter(F.col("data_flag") == "future").count()
    historical_count = train_df.filter(F.col("data_flag") == "historical").count()
    logging.info(f"5. Just before impute_performance_columns: Number of rows with data_flag='future': {future_count}")
    logging.info(f"5. Just before impute_performance_columns:  Number of rows with data_flag='historical': {historical_count}")
    
    # After loading and performing your earlier data cleansing...
    train_df = impute_performance_columns(train_df)
    train_df = drop_historical_missing_official_fin(train_df)
    train_df = impute_performance_features(train_df)
  
    future_count = train_df.filter(F.col("data_flag") == "future").count()

    # Count rows with data_flag = "historical"
    historical_count = train_df.filter(F.col("data_flag") == "historical").count()

    # Log the counts
    logging.info(f"6. Just AFTER deleting columns with NaN/null: Number of rows with data_flag='future': {future_count}")
    logging.info(f"6. Just AFTER deleting columns with NaN/null: Number of rows with data_flag='historical': {historical_count}")
    
    logging.info("Rows in data_flag: train_df['data_flag'] = 'future'")
    logging.info("Starting the write to parquet.")
    start_time = time.time()
    train_df.write.mode("overwrite").parquet(f"{parquet_dir}/train_df")
    logging.info(f"Data written to Parquet in {time.time() - start_time:.2f} seconds")
    logging.info("Data cleansing complete. train_df being returned.")
    
    return train_df
        