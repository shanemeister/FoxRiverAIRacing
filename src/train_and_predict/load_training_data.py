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
from pyspark.sql.types import DecimalType, DoubleType

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

def fix_outliers(df):
    """
    1) Hard-code certain known columns with suspicious extremes.
    2) Use percentile-based approach for columns we want to clamp in a data-driven manner.
    """
    # Step A: Hard-coded outlier caps
    outlier_bounds = {
        "avg_speed_fullrace": (0, 20),
        "avg_stride_length": (0, 3),
        "distance_meters": (0, 3200),
        "dist_q4": (0, 1000),
        "avg_beaten_len_5": (0, 50.0),
        "max_acceleration": (0, 5),
        "max_jerk": (0, 2.0),
        "speed_q4": (0, 22.0),
        "total_dist_covered": (0, 3655),
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
        # (F.col("has_gps") == 1) |
        (F.col("course_cd").isin(course_cd_list))
    )
            
    return filtered_df

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

def fill_missing_with_race_mean_or_zero(
    df,
    race_cols=("course_cd", "race_date", "race_number"),
    cols_to_impute=None
):
    """
    For each column in `cols_to_impute`, compute the race-level mean within the group
    defined by race_cols. Then fill missing values in that column with:
      - the race mean if not null
      - otherwise 0

    :param df: Spark DataFrame with at least these columns:
               [*race_cols, plus each col in cols_to_impute].
    :param race_cols: tuple/list of columns that uniquely identify a race
                      (e.g. ("course_cd","race_date","race_number")).
    :param cols_to_impute: list of numeric columns to fill with race-level mean or 0.
    :return: a new DataFrame where those columns have been imputed.
    """

    # 1) Group by race, compute the mean for each column
    #    We'll create an aggregation for each column in cols_to_impute, naming it col+"_mean".
    agg_exprs = []
    for c in cols_to_impute:
        agg_exprs.append(F.mean(F.col(c)).alias(f"{c}_mean"))

    means_df = df.groupBy(*race_cols).agg(*agg_exprs)

    # 2) Join the means back to the original DataFrame
    joined = df.join(means_df, on=list(race_cols), how="left")

    # 3) For each column c, fill missing values:
    #    if c is null => coalesce(col(c+"_mean"), lit(0)) => that is the fallback
    #    else keep the original c
    for c in cols_to_impute:
        joined = joined.withColumn(
            c,
            F.when(
                F.col(c).isNull(),
                F.coalesce(F.col(f"{c}_mean"), F.lit(0.0))  # fallback to 0
            ).otherwise(F.col(c))
        )

    # 4) Optionally drop the mean columns we added
    for c in cols_to_impute:
        joined = joined.drop(f"{c}_mean")

    return joined

def analyze_missing_data_by_race(df, columns_to_check=None):
    """
    Splits the data into historical vs. future (data_flag),
    then for each subset, counts:
      1) total number of distinct races,
      2) how many races have no missing data in `columns_to_check`,
      3) how many races have at least one horse missing data in those columns.

    :param df: Spark DataFrame with at least columns:
               [course_cd, race_date, race_number, data_flag, <columns_to_check>...]
    :param columns_to_check: list of string column names to check for nulls.
                             defaults to ['accel_q1','accel_q2','accel_q3','accel_q4']
    :return: None (prints out the statistics).
    """

    if columns_to_check is None:
        columns_to_check = ["accel_q1","accel_q2","accel_q3","accel_q4"]

    # 1) Split into historical vs. future
    df_hist = df.filter(F.col("data_flag") == "historical")
    df_future = df.filter(F.col("data_flag") == "future")

    # =============== HELPER FUNCTION ===============
    def process_subset(subset_df, subset_name):
        """
        For a given subset (historical or future),
        compute:
          - total distinct races
          - number of races with missing data in any of the columns_to_check
          - number of races with no missing data
        Then print the results.
        """

        # a) Distinct race key
        race_cols = ["course_cd", "race_date", "race_number"]

        # b) Count total distinct races
        total_races = (
            subset_df
            .select(*race_cols)
            .distinct()
            .count()
        )

        # c) Build an OR expression: (col1 isNull) OR (col2 isNull) ...
        #    to see if *any* column is null in that row
        any_null_expr = None
        for c in columns_to_check:
            col_null = F.col(c).isNull()
            if any_null_expr is None:
                any_null_expr = col_null
            else:
                any_null_expr = any_null_expr | col_null

        # If none of the columns_to_check exist in the DF, any_null_expr could be None
        # so we handle that case:
        if any_null_expr is None:
            # Means no columns to check are found in the subset
            # We'll treat it as no missing data
            # (or you might want to handle differently)
            print(f"--- {subset_name.upper()} SUBSET ---")
            print(f"No columns to check found in this subset. Total races: {total_races}")
            return

        # d) row_missing_any => 1 if row has ANY missing data in the columns_to_check
        df_with_missing_flag = subset_df.withColumn(
            "row_missing_any",
            F.when(any_null_expr, 1).otherwise(0)
        )

        # e) Group by race, sum up row_missing_any => if sum>0 => race has missing
        race_missing_df = (
            df_with_missing_flag
            .groupBy(*race_cols)
            .agg(F.sum("row_missing_any").alias("sum_missing"))
            .withColumn("has_missing_data",
                        F.when(F.col("sum_missing") > 0, 1).otherwise(0))
        )

        # f) number of races with missing data
        races_with_missing = (
            race_missing_df
            .filter(F.col("has_missing_data") == 1)
            .count()
        )

        # g) number of races with no missing data
        races_no_missing = total_races - races_with_missing

        print(f"--- {subset_name.upper()} SUBSET ---")
        print(f"Total distinct races: {total_races}")
        print(f"Races with NO missing data: {races_no_missing}")
        print(f"Races with SOME missing data: {races_with_missing}")
        print("")

        logging.info(f"--- {subset_name.upper()} SUBSET ---")
        logging.info(f"Total distinct races: {total_races}")
        logging.info(f"Races with NO missing data: {races_no_missing}")
        logging.info(f"Races with SOME missing data: {races_with_missing}")

    # 2) Process historical subset
    process_subset(df_hist, "historical")

    # 3) Process future subset
    process_subset(df_future, "future")

def extract_completely_clean_races(df, required_cols, min_horses=None):
    """
    Returns a subset of `df` containing only those races (course_cd, race_date, race_number)
    where *all* horses have *no* missing values in the specified `required_cols`
    AND (optionally) the race has at least `min_horses` horses.

    :param df: Spark DataFrame with at least:
               [course_cd, race_date, race_number, data_flag] + required_cols
    :param required_cols: list of column names that must be non-null for every horse in a race
    :param min_horses: if set (e.g. 6), then we drop any race with fewer than `min_horses`.
                       if None, we don't filter by field size.
    :return: (clean_df, race_counts_dict)
             where `clean_df` is the filtered subset,
             and `race_counts_dict` has some stats about how many races remain.
    """

    # 1) Create a boolean expression checking if any required_col is null
    #    => row_missing_any = 1 if any are null
    any_null_expr = None
    for c in required_cols:
        col_null = F.col(c).isNull()
        if any_null_expr is None:
            any_null_expr = col_null
        else:
            any_null_expr = any_null_expr | col_null

    # If none of the required_cols exist or something strange, handle gracefully:
    if any_null_expr is None:
        raise ValueError("No required_cols found in the DataFrame schema.")

    df_with_flag = df.withColumn(
        "row_missing_any",
        F.when(any_null_expr, 1).otherwise(0)
    )

    # The columns that define a unique race
    race_cols = ["course_cd","race_date","race_number"]

    # 2) sum up row_missing_any by race => if sum_missing>0 => race has missing data
    race_missing_df = (
        df_with_flag
        .groupBy(*race_cols)
        .agg(F.sum("row_missing_any").alias("sum_missing"))
    )

    # 3) Filter to only races where sum_missing = 0 (i.e. fully complete in required_cols)
    complete_race_keys = race_missing_df.filter(F.col("sum_missing") == 0)

    # 4) Optional: also exclude races that have fewer than `min_horses` horses
    #    if min_horses is provided
    if min_horses is not None:
        # We'll build a DataFrame that counts how many rows/horses each race has
        race_size_df = (
            df.groupBy(*race_cols)
              .agg(F.count("*").alias("num_horses"))
        )
        # Filter to only those that have >= min_horses
        race_size_filtered = race_size_df.filter(F.col("num_horses") >= min_horses)

        # Now we have two sets of race keys to keep:
        # 1) `complete_race_keys`: no missing data in `required_cols`
        # 2) `race_size_filtered`: field size >= min_horses
        # We'll intersect them:
        complete_race_keys = (
            complete_race_keys.join(
                race_size_filtered.select(*race_cols),
                on=race_cols,
                how="inner"  # intersection
            )
        )

    # 5) Join back to original df => keep only those races
    clean_df = (
        df.join(
            complete_race_keys.select(*race_cols),
            on=race_cols,
            how="inner"
        )
    )

    # 6) Some stats
    total_races = df.select(*race_cols).distinct().count()
    total_clean_races = complete_race_keys.count()

    # Count how many future vs historical remain after filtering
    clean_future_races = (
        clean_df
        .filter(F.col("data_flag") == "future")
        .select(*race_cols)
        .distinct()
        .count()
    )
    clean_historical_races = (
        clean_df
        .filter(F.col("data_flag") == "historical")
        .select(*race_cols)
        .distinct()
        .count()
    )

    # Construct a small dict of stats
    race_counts_dict = {
        "total_races_before": total_races,
        "total_clean_races_after": total_clean_races,
        "clean_future_races": clean_future_races,
        "clean_historical_races": clean_historical_races
    }

    return clean_df, race_counts_dict

def analyze_field_size_distribution(df):
    """
    Given a DataFrame of fully cleaned races, compute stats about
    how many horses are in each race. 
    Returns and prints:
      - total distinct races
      - min field size
      - max field size
      - average field size
      - median field size (approx)
      - distribution histogram if desired
    """

    race_cols = ["course_cd", "race_date", "race_number"]

    # 1) Count how many rows/horses per race
    race_size_df = (
        df.groupBy(*race_cols)
          .agg(F.count("*").alias("num_horses"))
    )

    # 2) Overall stats
    total_races = race_size_df.count()
    min_size = race_size_df.agg(F.min("num_horses")).collect()[0][0]
    max_size = race_size_df.agg(F.max("num_horses")).collect()[0][0]
    avg_size = race_size_df.agg(F.avg("num_horses")).collect()[0][0]

    # If you want an approximate median, you can use approxQuantile in Spark
    median_size = race_size_df.select("num_horses").approxQuantile("num_horses", [0.5], 0.001)[0]

    # Print results
    logging.info("=== FIELD SIZE STATS ===")
    logging.info(f"Total Distinct Races: {total_races}")
    logging.info(f"Minimum Field Size:   {min_size}")
    logging.info(f"Maximum Field Size:   {max_size}")
    logging.info(f"Average Field Size:   {avg_size:.2f}")
    logging.info(f"Median Field Size:    {median_size}")
    logging.info("")

    # (Optional) If you want to see a quick distribution/histogram, you could
    # do a groupBy(num_horses) and count:
    size_distribution = (
        race_size_df.groupBy("num_horses")
                    .count()
                    .orderBy("num_horses")
    )
    size_distribution.show(50, truncate=False)

    # Return the DataFrame or relevant stats if needed
    return {
        "total_races": total_races,
        "min_size": min_size,
        "max_size": max_size,
        "avg_size": avg_size,
        "median_size": median_size
    }
        
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
            
    train_df = impute_par_time_all_steps(train_df)
    
    # Log the counts after filtering
    future_count = train_df.filter(F.col("data_flag") == "future").count()
    historical_count = train_df.filter(F.col("data_flag") == "historical").count()
    logging.info(f"Before filtering on has_gps and course_cd list: data_flag='future': {future_count}")
    logging.info(f"Before filtering on has_gps and course_cd list: data_flag='historical': {historical_count}")
    
    ##############################################################
    # Race mean imputation: if no horse has a value for a column,
    # fill the column with 0. Reasoning being is that this could 
    # be a new horse or a horse that has never raced before.
    ##############################################################
    
    cols_to_impute = ["distance_meters", "prev_speed_rating", "previous_distance", "starts", "previous_class",
                      'avg_workout_rank_3','avgtime_gate3','avgtime_gate4','dam_roi','dist_bk_gate3',
                      'dist_bk_gate4','sire_roi']
    race_cols=("course_cd", "race_date", "race_number")
    train_df = fill_missing_with_race_mean_or_zero(train_df, race_cols, cols_to_impute)

    ##############################################################
    # The big function to drope races with missing required columns
    # and to remove races with fewer than 5 horses
    ##############################################################
    
    # Apply the filter to the train_df DataFrame
    train_df = filter_course_cd(train_df)        

    required_cols = ["speed_improvement", 'accel_q1','accel_q2','accel_q3','accel_q4', 
                     'avg_dist_bk_gate1_5','avg_dist_bk_gate2_5','avg_dist_bk_gate3_5',
                    'avg_dist_bk_gate4_5','avg_speed_fullrace_5','avg_strfreq_q1_5',
                    'avg_strfreq_q2_5','avg_strfreq_q3_5','avg_strfreq_q4_5','avg_stride_length_5']
    clean_df, stats = extract_completely_clean_races(train_df, required_cols, min_horses=5)
    analyze_missing_data_by_race(clean_df, required_cols)
    
    logging.info("=== RACE STATS ===")
    for k,v in stats.items():
        logging.info(f"{k} = {v}")
        
    analyze_field_size_distribution(clean_df)
    
    train_df = clean_df
    ####################################################################
    ####################################################################  
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
    for field in train_df.schema.fields:
        if isinstance(field.dataType, DecimalType):
            col_name = field.name
            train_df = train_df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
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

    columns_to_fill = ['net_sentiment', 'jock_itm_percent','jock_win_percent','prev_official_fin', 'trainer_itm_percent',
                       'trainer_itm_track','trainer_win_percent','trainer_win_track']
    logging.info("Filling missing values for columns.")
    
    for column in columns_to_fill:
        if column in train_df.columns:
            # If null, fill with 0 (for numeric columns)
            train_df = train_df.withColumn(
                column,
                when(col(column).isNull(), lit(0)).otherwise(col(column))
            )
      
    # Example usage:
    train_df = fix_outliers(train_df)

    # Log the counts
    future_count = train_df.filter(F.col("data_flag") == "future").count()
    historical_count = train_df.filter(F.col("data_flag") == "historical").count()
    logging.info(f"5. Just before impute_performance_columns: Number of rows with data_flag='future': {future_count}")
    logging.info(f"5. Just before impute_performance_columns:  Number of rows with data_flag='historical': {historical_count}")
    
    train_df = drop_historical_missing_official_fin(train_df)
  
    future_count = train_df.filter(F.col("data_flag") == "future").count()

    # Count rows with data_flag = "historical"
    historical_count = train_df.filter(F.col("data_flag") == "historical").count()

    # Log the counts
    logging.info(f"6. Just AFTER deleting columns with NaN/null: Number of rows with data_flag='future': {future_count}")
    logging.info(f"6. Just AFTER deleting columns with NaN/null: Number of rows with data_flag='historical': {historical_count}")
    
    logging.info("Rows in data_flag: train_df['data_flag'] = 'future'")
    logging.info("Starting the write to parquet.")

    start_time = time.time()
    print(f"Writing to Parquet: {parquet_dir}train_df")
    train_df.write.mode("overwrite").parquet(f"{parquet_dir}train_df")
    logging.info(f"Data written to Parquet in {time.time() - start_time:.2f} seconds")
    logging.info("Data cleansing complete. train_df being returned.")
    
    return train_df
        