import os
import logging
import time
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import (col, count, abs, unix_timestamp, lag, avg as F_avg, first, coalesce, 
                                   when, lit, min as F_min, max as F_max , upper, trim,
                                   row_number, mean as F_mean, countDistinct, last, first)
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

def manage_tpd_cols_by_flag(df):
    """
    1) For rows where data_flag='historical': drop any row that has a null in the TPD columns.
    2) For rows where data_flag='future': fill null TPD columns with 0.

    Logs row counts before/after for each subset.
    """
    import pyspark.sql.functions as F
    
    tpd_cols = [
        "avg_dist_bk_gate2_5", "avg_dist_bk_gate3_5",
        "avg_dist_bk_gate4_5", "avg_speed_fullrace_5",
        "avg_strfreq_q1_5", "avg_strfreq_q2_5",
        "avg_strfreq_q3_5", "avg_strfreq_q4_5",
        "avg_stride_length_5", "avg_dist_bk_gate1_5",
        "speed_improvement"
    ]

    # 1) Subset historical
    df_hist = df.filter(F.col("data_flag") == "historical")
    #  - Log count before
    hist_count_before = df_hist.count()
    #  - Drop rows with null in any TPD col
    df_hist = df_hist.na.drop(subset=tpd_cols)
    #  - Log count after
    hist_count_after = df_hist.count()
    logging.info(f"manage_tpd_cols_by_flag [historical]: before={hist_count_before}, after={hist_count_after}")

    # 2) Subset future
    df_future = df.filter(F.col("data_flag") == "future")
    #  - Log count before
    fut_count_before = df_future.count()
    #  - Impute with 0 for null TPD cols
    for c in tpd_cols:
        df_future = df_future.withColumn(c, F.when(F.col(c).isNull(), F.lit(0)).otherwise(F.col(c)))
    #  - Log count after
    fut_count_after = df_future.count()
    logging.info(f"manage_tpd_cols_by_flag [future]: before={fut_count_before}, after={fut_count_after}")

    # 3) Union them back
    #    The unionByName ensures columns line up by name
    #    (Spark 2.3+ recommended).
    df_final = df_hist.unionByName(df_future)

    return df_final

def fix_outliers(df):
    """
    1) Hard-code certain known columns with suspicious extremes.
    2) Use percentile-based approach for columns we want to clamp in a data-driven manner.
    """
    # Step A: Hard-coded outlier caps
    outlier_bounds = {
        "avg_beaten_len_5": (0, 50.0),
        "avgtime_gate4": (0, 16.0),
        "days_off": (0, 365.0),
        "avgspd": (0, 120.0),
        "weight": (100, 125.0),
        "avg_workout_rank_3": (0, 60.0),
        "speed_improvement": (-20, 50),
        "sire_itm_percentage": (0, 1),
        "sire_roi": (-100, 10000),
        "avg_speed_fullrace_5": (0, 20),
        "dam_itm_percentage": (0, 1),
        "dam_roi": (-100, 5000),
        "avg_stride_length_5": (0, 10),
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

def drop_historical_missing_official_fin(df):
    """
    Remove rows from the DataFrame where:
      - data_flag is 'historical' AND
      - official_fin is null.
    Future races (data_flag != 'historical') are kept even if official_fin is null.
    """
    return df.filter(~((F.col("data_flag") == "historical") & (F.col("official_fin").isNull())))

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
    'CNL','SAR','PIM','TSA','BEL','MVR','TWO','CLS','KEE','TAM',
    'TTP','TKD','ELP','PEN','HOU','DMR','TLS','AQU','MTH','TGP',
    'TGG','CBY','LRL','TED','IND','CTD','ASD','TCD','LAD','TOP'
    ]

    filtered_df = train_df.filter(
        # Condition A: keep if has_gps=1 (GPS data) OR 
        # condition B: keep if course_cd is in the TPD list
        (F.col("has_gps") == 1) 
        | (F.col("course_cd").isin(course_cd_list))
    )
            
    return filtered_df

import pyspark.sql.functions as F
from pyspark.sql.window import Window

def forward_fill_par_time(
    df,
    partition_cols = ["course_cd", "distance_meters"],
    date_col = "race_date",
    par_time_col = "par_time"
):
    """
    Forward fill par_time in ascending date order for each partition.
    """
    w_asc = (Window.partitionBy(*partition_cols)
                    .orderBy(F.col(date_col).asc())
                    .rowsBetween(Window.unboundedPreceding, 0))

    expr_ffill = F.last(F.col(par_time_col), ignorenulls=True).over(w_asc)
    df_forward = df.withColumn(
        par_time_col,
        F.coalesce(F.col(par_time_col), expr_ffill)
    )
    return df_forward

def backward_fill_par_time(
    df,
    partition_cols = ["course_cd", "distance_meters"],
    date_col = "race_date",
    par_time_col = "par_time"
):
    """
    Backward fill par_time in descending date order for each partition.
    This fills from future races if forward fill found nothing.
    """
    w_desc = (Window.partitionBy(*partition_cols)
                    .orderBy(F.col(date_col).desc())
                    .rowsBetween(Window.unboundedPreceding, 0))

    expr_bfill = F.last(F.col(par_time_col), ignorenulls=True).over(w_desc)
    df_backward = df.withColumn(
        par_time_col,
        F.coalesce(F.col(par_time_col), expr_bfill)
    )
    return df_backward

def fill_par_time_global_mean(
    df,
    par_time_col = "par_time"
):
    """
    Fill null par_time with global mean.
    """
    global_mean = df.agg(F.avg(par_time_col).alias("gm")).collect()[0]["gm"]
    df_gmean = df.withColumn(
        par_time_col,
        F.coalesce(F.col(par_time_col), F.lit(global_mean))
    )
    return df_gmean

def impute_par_time_all_steps(df):
    """
    1) Forward fill by (course_cd, distance_meters).
    2) Backward fill by same partition (desc).
    3) Fill remaining null with global mean.
    """
    # Forward fill
    df_ff = forward_fill_par_time(df)
    # Backward fill
    df_bf = backward_fill_par_time(df_ff)
    # Global mean fallback
    df_final = fill_par_time_global_mean(df_bf)
    return df_final

def impute_sectional_features(
    df,
    impute_cols = [
        "avgtime_gate1","avgtime_gate2","avgtime_gate3","avgtime_gate4",
        "dist_bk_gate1","dist_bk_gate2","dist_bk_gate3","dist_bk_gate4",
        "running_time","total_distance_ran"
    ],
    race_key = ["course_cd", "race_date", "race_number"],  # grouping columns for Race-level means
    horse_id_col = "horse_id",         # column to partition for LOCF
    date_col = "race_date"             # ordering column for LOCF
):
    """
    Imputes the given `impute_cols` with 3 steps:
      1) LOCF within each horse partition, ordered by `date_col`.
      2) Race-level mean (grouping by `race_key`).
      3) Global mean if still null.

    The final DataFrame columns (e.g. 'avgtime_gate1') will be overwritten with
    imputed values. The leftover '_race_mean' columns are intermediate helpers
    and can be dropped after if desired.
    """

    # 1) LOCF Imputation
    w_locf = Window.partitionBy(horse_id_col).orderBy(date_col).rowsBetween(Window.unboundedPreceding, 0)

    df_locf = df
    # For each column c, fill with the last known non-null from prior rows in this partition
    for c in impute_cols:
        # Build expression for last non-null in ascending order
        c_last = last(col(c), ignorenulls=True).over(w_locf)
        # Overwrite c with COALESCE(c, c_last)
        df_locf = df_locf.withColumn(c, coalesce(col(c), c_last))

    # 2) Race-level mean
    # Build an aggregator for each col, i.e. avg(c) => c_race_mean
    race_means_exprs = [F_avg(c).alias(f"{c}_race_mean") for c in impute_cols]
    # Group by your race_key to compute per-race means
    df_race_means = df_locf.groupBy(*race_key).agg(*race_means_exprs)

    # Join these means back; left join so we retain all rows
    df_joined = df_locf.join(df_race_means, on=race_key, how="left")

    # Now fill each c with coalesce(c, c_race_mean)
    for c in impute_cols:
        c_racemean = f"{c}_race_mean"
        df_joined = df_joined.withColumn(
            c,
            coalesce(col(c), col(c_racemean))  # fill with race-level mean
        )

    # 3) Global mean
    # Compute a single row of global means for each column
    global_means_exprs = [F_avg(c).alias(f"{c}_global_mean") for c in impute_cols]
    global_means_row = df_joined.agg(*global_means_exprs).collect()[0].asDict()

    # Overwrite each column with coalesce(col(c), global_mean)
    df_imputed = df_joined
    for c in impute_cols:
        global_mean_key = f"{c}_global_mean"
        global_mean_val = global_means_row[global_mean_key]
        df_imputed = df_imputed.withColumn(
            c,
            coalesce(col(c), lit(global_mean_val))
        )

    # (Optional) Drop leftover "_race_mean" columns if desired:
    # race_mean_cols = [f"{c}_race_mean" for c in impute_cols]
    # df_imputed = df_imputed.drop(*race_mean_cols)
    
    race_mean_cols = [f"{c}_race_mean" for c in impute_cols]
    df_imputed = df_imputed.drop(*race_mean_cols)

    return df_imputed

def remove_future_races_with_unmatched_horses(df):
    """
    Deletes all future races (data_flag='future') that contain unmatched horses 
    (i.e. horses that do not appear in the historical subset).
    
    Steps:
    1) Split df into historical vs. future.
    2) Identify horses in historical -> distinct horse_ids.
    3) Left-anti join future on those horse_ids to find "unmatched_future_df".
    4) Distinctly gather the (course_cd, race_date, race_number) from unmatched_future_df.
    5) Filter out those race keys from future_df.
    6) Recombine historical + the 'cleaned' future subset.
    7) Return the final DataFrame.
    """

    import pyspark.sql.functions as F

    # Split
    df_hist = df.filter(F.col("data_flag") == "historical")
    df_future = df.filter(F.col("data_flag") == "future")

    # 1) Distinct horse_ids in historical
    historical_horses_df = df_hist.select("horse_id").distinct()

    # 2) Find unmatched future rows via left_anti on horse_id
    unmatched_future_df = df_future.join(historical_horses_df, on="horse_id", how="left_anti")

    # 3) Distinct race keys for those unmatched rows
    unmatched_races = (
        unmatched_future_df
        .select("course_cd", "race_date", "race_number")
        .distinct()
    )

    # 4) Filter out those race keys from df_future
    #    i.e. keep future rows that do NOT appear in unmatched_races
    joined_for_filter = df_future.join(
        unmatched_races,
        on=["course_cd","race_date","race_number"],
        how="left_anti"
    )

    # 5) Recombine historical + the "cleaned" future
    df_final = df_hist.unionByName(joined_for_filter)

    # 6) Log final row counts
    fut_count_final = df_final.filter(F.col("data_flag") == "future").count()
    hist_count_final = df_final.filter(F.col("data_flag") == "historical").count()
    logging.info(f"[remove_future_races_with_unmatched_horses] final future={fut_count_final}, historical={hist_count_final}")

    return df_final

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
    
    
    impute_cols = ["avgtime_gate1","avgtime_gate2","avgtime_gate3","avgtime_gate4","dist_bk_gate1","dist_bk_gate2","dist_bk_gate3","dist_bk_gate4","running_time","total_distance_ran"]
    race_key = ["course_cd", "race_date", "race_number"] # columns defining a "race"
    horse_id_col = "horse_id"
    date_col = "race_date" 
    
    train_df = impute_sectional_features(train_df, impute_cols, race_key, horse_id_col, date_col) 

    train_df = impute_par_time_all_steps(train_df)
    
    # Log the counts after filtering
    future_count = train_df.filter(F.col("data_flag") == "future").count()
    historical_count = train_df.filter(F.col("data_flag") == "historical").count()
    logging.info(f"Before filtering on has_gps and course_cd list: data_flag='future': {future_count}")
    logging.info(f"Before filtering on has_gps and course_cd list: data_flag='historical': {historical_count}")
    
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

    train_df = manage_tpd_cols_by_flag(train_df)

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
    numeric_cols = ["race_number","horse_id","purse","weight","claimprice","distance_meters",
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

    # train_df = train_df.na.drop(subset=critical_cols)
    cols_to_fill = ['distance_meters',
                    'off_finish_last_race',
                    'prev_speed_rating',
                    'previous_class',
                    'previous_distance',
                    'race_count',
                    'starts']

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

    train_df = remove_future_races_with_unmatched_horses(train_df)
    
    # train_df = remove_performance_columns(train_df) # dist_bk_gate4, running_time, total_distance_ran
    # Log the counts
    future_count = train_df.filter(F.col("data_flag") == "future").count()
    historical_count = train_df.filter(F.col("data_flag") == "historical").count()
    logging.info(f"5. Just after impute_performance_columns: Number of rows with data_flag='future': {future_count}")
    logging.info(f"5. Just after impute_performance_columns:  Number of rows with data_flag='historical': {historical_count}")

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
        