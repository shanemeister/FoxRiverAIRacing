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
from pyspark.sql.functions import col, when

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

def drop_historical_missing_official_fin(df):
    """
    Remove rows from the DataFrame where:
      - data_flag is 'historical' AND
      - official_fin is null.
    Future races (data_flag != 'historical') are kept even if official_fin is null.
    """
    return df.filter(~((F.col("data_flag") == "historical") & (F.col("official_fin").isNull())))

def fill_forward_locf(
    df: DataFrame,
    columns_to_update: list,
    horse_id_col: str = "horse_id",
    date_col: str = "race_date",
    race_num_col: str = "race_number"
) -> DataFrame:
    """
    Perform a forward-fill (LOCF: Last Observation Carried Forward) on 'df' 
    for each horse, ordered by (date_col, race_num_col).
    
    For each column in 'columns_to_update', any null value in a later row 
    will be replaced by the most recent non-null value from a previous row 
    (within the same horse_id partition).
    
    :param df:            The Spark DataFrame that contains horse racing data.
    :param columns_to_update: List of column names to forward-fill if null.
    :param horse_id_col:  Column name identifying the horse (e.g. "horse_id").
    :param date_col:      Column representing the date (or date-time) of the race.
    :param race_num_col:  Column representing the race number (to break ties or refine ordering).
    
    :return: A new DataFrame with forward-filled values for the specified columns.
    """

    # Define a window partitioned by horse_id, ordered by race_date asc, race_number asc
    # rowsBetween(Window.unboundedPreceding, Window.currentRow) 
    # means: for the current row, consider all rows from the start of the partition up to current.
    w = (
        W.Window
        .partitionBy(horse_id_col)
        .orderBy(F.col(date_col).asc(), F.col(race_num_col).asc())
        .rowsBetween(W.Window.unboundedPreceding, W.Window.currentRow)
    )

    # For each column, apply last_value(..., ignorenulls=True) 
    # to fill forward the most recent non-null occurrence.
    for col_name in columns_to_update:
        df = df.withColumn(
            col_name,
            F.last(F.col(col_name), ignorenulls=True).over(w)
        )

    return df

import logging
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import DataFrame

def fill_forward_locf(
    df: DataFrame,
    columns_to_update: list,
    horse_id_col: str = "horse_id",
    date_col: str = "race_date",
    race_num_col: str = "race_number"
) -> DataFrame:
    """
    Provided function: fill forward (LOCF) for the specified columns,
    presumably ordering by date_col + race_num_col for each horse_id.
    Implementation omitted here, just assume it does the fill in a chain:
      - window sort by horse_id, race_date, race_number
      - fill null from previous row
    """
    # We'll assume you have it implemented.
    return df  # placeholder

def impute_performance_columns(df: DataFrame) -> DataFrame:
    """
    For columns: [time_behind, pace_delta_time, speed_rating, prev_speed_rating]
    
    - If data_flag='historical':
        * time_behind => if official_fin=1 => 0; else fill with race-level mean => else global mean
        * pace_delta_time, speed_rating, prev_speed_rating => fill missing with race-level mean => else global mean
          (No more dropping rows for missing data.)
    
    - If data_flag='future':
        1) Apply fill_forward_locf() for all 4 columns.
        2) Then do the same race-level => global mean logic for any that remain missing:
           - time_behind => if official_fin=1 => 0
           - else if still null => race-level mean => global mean
           - same for the other 3 columns (minus the official_fin=1 => 0 part).
    """

    performance_cols = ["time_behind", "pace_delta_time", "speed_rating", "prev_speed_rating", "dist_bk_gate4", "running_time", "total_distance_ran"]
    race_keys = ["course_cd", "race_date", "race_number"]

    # ----------------------------------------------------------------------------
    # STEP 1) HISTORICAL
    # ----------------------------------------------------------------------------
    # For historical rows => fill with race-level => global mean
    # time_behind => if official_fin=1 => 0.0 else do the same approach
    # We do NOT drop rows anymore. Instead, we use mean imputation.

    # First compute the global means for the entire df_hist
    global_means_hist = {}
    for col_name in performance_cols:
        gm = df.select(F.mean(F.col(col_name)).alias("gm")).collect()[0]["gm"]
        global_means_hist[col_name] = gm

    # We'll define a race-level mean window
    race_window_hist = Window.partitionBy(*race_keys)

    for col_name in performance_cols:
        # 1) compute race-level mean
        df = df.withColumn(
            f"race_mean_{col_name}",
            F.avg(F.col(col_name)).over(race_window_hist)
        )
        # 2) define fill expression
        if col_name == "time_behind":
            # if official_fin=1 => 0.0
            # else if col is null => use race_mean if not null => else global
            df = df.withColumn(
                col_name,
                F.when(F.col("official_fin") == 1, 0.0)
                 .otherwise(
                     F.when(F.col(col_name).isNull(),
                            F.when(F.col(f"race_mean_{col_name}").isNotNull(),
                                   F.col(f"race_mean_{col_name}"))
                             .otherwise(F.lit(global_means_hist[col_name]))
                     ).otherwise(F.col(col_name))
                 )
            )
        else:
            # if col is null => race_mean => else global
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(),
                       F.when(F.col(f"race_mean_{col_name}").isNotNull(),
                              F.col(f"race_mean_{col_name}"))
                        .otherwise(F.lit(global_means_hist[col_name]))
                ).otherwise(F.col(col_name))
            )
        # Drop temp col
        df = df.drop(f"race_mean_{col_name}")

    hist_count_final = df.count()
    logging.info(f"[impute_performance_columns - HIST] final count={hist_count_final}")

    # ----------------------------------------------------------------------------
    # STEP 2) FUTURE
    # ----------------------------------------------------------------------------
    # 2a) Apply LOCF to the future data on these 4 columns
    df = fill_forward_locf(
        df,
        columns_to_update=performance_cols,
        horse_id_col="horse_id",
        date_col="race_date",
        race_num_col="race_number"
    )

    # 2b) Then fill any that remain missing with the same logic as above
    # but for future data. We'll do time_behind => if official_fin=1 => 0.0 => else race => global
    # for other 3 => race => global

    # compute global means for df
    global_means_future = {}
    for col_name in performance_cols:
        gm = df.select(F.mean(F.col(col_name)).alias("gm")).collect()[0]["gm"]
        global_means_future[col_name] = gm

    race_window_future = Window.partitionBy(*race_keys)

    for col_name in performance_cols:
        df = df.withColumn(
            f"race_mean_{col_name}",
            F.avg(F.col(col_name)).over(race_window_future)
        )
        if col_name == "time_behind":
            df = df.withColumn(
                col_name,
                F.when(F.col("official_fin") == 1, 0.0)
                 .otherwise(
                     F.when(F.col(col_name).isNull(),
                            F.when(F.col(f"race_mean_{col_name}").isNotNull(),
                                   F.col(f"race_mean_{col_name}"))
                             .otherwise(F.lit(global_means_future[col_name]))
                     ).otherwise(F.col(col_name))
                 )
            )
        else:
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(),
                       F.when(F.col(f"race_mean_{col_name}").isNotNull(),
                              F.col(f"race_mean_{col_name}"))
                        .otherwise(F.lit(global_means_future[col_name]))
                ).otherwise(F.col(col_name))
            )
        df = df.drop(f"race_mean_{col_name}")

    fut_count_final = df.count()
    logging.info(f"[impute_performance_columns - FUTURE] final count={fut_count_final}")

    # ----------------------------------------------------------------------------
    # STEP 3) UNION
    # ----------------------------------------------------------------------------
    # df_final = df_hist.unionByName(df)

    df_hist = df.filter(F.col("data_flag") == "historical")
    df_future = df.filter(F.col("data_flag") == "future")

    # Logging
    fut_count_final = df_future.count()
    hist_count_final = df_hist.count()
    logging.info(f"[impute_performance_columns] final future={fut_count_final}, historical={hist_count_final}")

    return df

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

# def remove_performance_columns(df):
#     """
#     Removes the three specified columns (dist_bk_gate4, running_time, total_distance_ran)
#     from the DataFrame and logs the final row counts for historical vs. future.
#     """

#     cols_to_drop = ["dist_bk_gate4", "running_time", "total_distance_ran"]

#     # 1) Drop the columns
#     df = df.drop(*cols_to_drop)

#     # 2) Log final row counts
#     future_count = df.filter(F.col("data_flag") == "future").count()
#     historical_count = df.filter(F.col("data_flag") == "historical").count()
#     logging.info(f"[remove_performance_columns] final future={future_count}, historical={historical_count}")

#     return df

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, when, isnull

def update_historical_and_future_with_imputation(
    future_df: DataFrame,
    historical_df: DataFrame,
    numeric_features: list,
    fill_method: str = "mean",  # or "median"
    rel_error: float = 0.0001
):
    """
    1) Identify any horse_id in future_df not present in historical_df ("missing horses").
    2) Compute global aggregator (mean or approx median) for each numeric feature in historical_df.
    3) Create one synthetic row per missing horse in historical_df, fill numeric cols => aggregator, first_time_runner=1.
    4) Also mark future_df rows for these missing horses => first_time_runner=1, else 0.
    5) Return (updated_hist, updated_future).

    :param future_df: Spark DF with 'horse_id' rows for upcoming races.
    :param historical_df: Spark DF with multiple rows per horse_id (or single). We'll add new rows if missing horse_id.
    :param numeric_features: List of columns in historical_df to fill. Must be numeric type.
    :param fill_method: "mean" => use F.mean(...). "median" => use approxQuantile(...).
    :param rel_error: Relative error for Spark approxQuantile if fill_method="median".
    :return: (updated_hist, updated_future)
             updated_hist => with new rows for missing horses,
             updated_future => same shape as future_df, but with first_time_runner=0/1.
    """

    # -----------------------------------------------------------------
    # STEP A: Identify missing horses in future_df that do not appear in historical_df
    # -----------------------------------------------------------------
    missing_horses = (
        future_df.select("horse_id").distinct()
        .join(
            historical_df.select("horse_id").distinct(),
            on="horse_id",
            how="leftanti"  # keep only rows in future_df that are NOT matched in historical_df
        )
    )
    # If no missing horses, we can still set first_time_runner=0 for all future horses
    if missing_horses.rdd.isEmpty():
        print("No missing horses found => no new rows added to historical_df.")
        # but we still define future_df's first_time_runner=0
        updated_future = future_df.withColumn("first_time_runner", lit(0))
        return historical_df, updated_future

    # -----------------------------------------------------------------
    # STEP B: Compute aggregator values (mean or approx median) for each numeric col
    # -----------------------------------------------------------------
    aggregator_vals = {}  # map col-> aggregator value

    if fill_method == "mean":
        agg_exprs = [F.mean(c).alias(c) for c in numeric_features]
        row_of_means = historical_df.agg(*agg_exprs).collect()[0].asDict()
        for c in numeric_features:
            aggregator_vals[c] = row_of_means[c]

    elif fill_method == "median":
        for c in numeric_features:
            q_val = historical_df.approxQuantile(c, [0.5], rel_error)
            aggregator_vals[c] = q_val[0] if q_val else 0
    else:
        raise ValueError(f"Unsupported fill_method='{fill_method}'. Use 'mean' or 'median'.")

    # -----------------------------------------------------------------
    # STEP C: Create one synthetic row per missing horse for historical_df
    # -----------------------------------------------------------------
    # Start with missing_horses => (distinct horse_id)
    new_rows = missing_horses
    # Fill each numeric feature with aggregator_vals[c]
    for c in numeric_features:
        fill_val = aggregator_vals.get(c, 0)
        new_rows = new_rows.withColumn(c, lit(fill_val))

    # Mark first_time_runner=1 in historical
    new_rows = new_rows.withColumn("first_time_runner", lit(1))

    # If historical_df doesn't have first_time_runner yet, create it =0
    if "first_time_runner" not in historical_df.columns:
        historical_df = historical_df.withColumn("first_time_runner", lit(0))

    # union => updated_hist
    updated_hist = historical_df.unionByName(new_rows, allowMissingColumns=True)
    print(f"Added {new_rows.count()} new horses to historical_df. Updated size = {updated_hist.count()}")

    # -----------------------------------------------------------------
    # STEP D: Mark future_df rows => first_time_runner=1 if horse_id is missing, else 0
    # -----------------------------------------------------------------
    # We'll do a left join from future_df to 'missing_horses', if we find a match => it was missing
    missing_horses_flag = missing_horses.withColumn("temp_flag", lit(1))

    updated_future = (
        future_df.alias("fut")
        .join(missing_horses_flag.alias("mh"), on="horse_id", how="left")
        .withColumn(
            "first_time_runner",
            when(col("temp_flag").isNotNull(), 1).otherwise(0)
        )
        .drop("temp_flag")
    )
    train_df = updated_hist.unionByName(updated_future, allowMissingColumns=True)
    
    return train_df

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

    # critical_cols = ["speed_rating"]  # columns you consider critical
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
    
    future_df = train_df.filter(F.col("data_flag") == "future")
    historical_df = train_df.filter(F.col("data_flag") == "historical")
    
    # Log the counts
    future_count = future_df.count()
    historical_count = historical_df.count()
    logging.info(f"5. Just before impute_performance_columns: Number of rows with data_flag='future': {future_count}")
    logging.info(f"5. Just before impute_performance_columns:  Number of rows with data_flag='historical': {historical_count}")
    
    final_feature_cols = ["previous_class", "class_rating", "previous_distance", "off_finish_last_race", 
                          "speed_rating", "prev_speed_rating","purse", "claimprice", "power", "avgspd", "avg_spd_sd","ave_cl_sd",
                          "hi_spd_sd", "pstyerl", "horse_itm_percentage","total_races_5", "avg_dist_bk_gate1_5", "avg_dist_bk_gate2_5",
                            "avg_dist_bk_gate3_5", "avg_dist_bk_gate4_5", "avg_speed_fullrace_5", "avg_stride_length_5", "avg_strfreq_q1_5",
                            "avg_speed_5","avg_fin_5","best_speed","avg_beaten_len_5","prev_speed",
                            "avg_strfreq_q2_5", "avg_strfreq_q3_5", "avg_strfreq_q4_5", "speed_improvement", "age_at_race_day",
                          "count_workouts_3","avg_workout_rank_3","weight","days_off", "starts", "race_count","has_gps","pace_delta_time", 
                          "cond_starts","cond_win","cond_place","cond_show","cond_fourth","cond_earnings",
                          "all_starts", "all_win", "all_place", "all_show", "all_fourth","all_earnings", 
                          "morn_odds","net_sentiment", 
                          "distance_meters", "jt_itm_percent", "jt_win_percent", 
                          "trainer_win_track", "trainer_itm_track", "trainer_win_percent", "trainer_itm_percent", 
                          "jock_win_track", "jock_win_percent","jock_itm_track",                            
                          "jt_win_track", "jt_itm_track", "jock_itm_percent",
                          "sire_itm_percentage", "sire_roi", "dam_itm_percentage", "dam_roi", "dist_bk_gate4", "running_time", "total_distance_ran"]
        
    train_df = update_historical_and_future_with_imputation(future_df,historical_df, final_feature_cols, "mean")
    
    # train_df = remove_performance_columns(train_df) # do this after next step of creating speed figure dist_bk_gate4, running_time, total_distance_ran
    
    # After loading and performing your earlier data cleansing...
    train_df = impute_performance_columns(train_df) # time_behind, pace_delta_time, speed_rating, prev_speed_rating

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
        