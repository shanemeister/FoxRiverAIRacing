import os 
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window
from pyspark.sql.window import Window
import numpy as np
from scipy.stats import norm
from pyspark.sql import DataFrame
import time
from pyspark.sql.types import DoubleType

def mark_and_fill_missing(df):
    """
    1) Identify any races that have missing TPD in [dist_bk_gate4, total_distance_ran, running_time].
       - Mark those races with race_missing_flag=1
    2) For each row (horse) in those races:
       - For dist_bk_gate4 only, check the official_fin value. If it is "1", populate dist_bk_gate4 with "0".
         Otherwise, populate dist_bk_gate4 with the median for the race. If all horses are missing this value,
         populate it with the global median.
    3) For columns total_distance_ran and running_time, populate with the race median. If there is no race median,
       populate it with the global median. If it is not possible to assign a global median, throw an error with a message.
    """
    TPD_COLS = ["dist_bk_gate4", "total_distance_ran", "running_time"]
    RACE_KEYS = ["course_cd", "race_date", "race_number"]

    # Create new columns
    df = df.withColumn("race_missing_flag", F.lit(0))
    df = df.withColumn("missing_gps_flag", F.lit(0))

    # Build a 'race_id' for grouping
    df = df.withColumn("race_id", F.concat_ws("_", *RACE_KEYS))

    # Which races have any missing TPD?
    missing_flags = df.groupBy("race_id").agg(
        F.max(F.when(F.col("dist_bk_gate4").isNull() | F.col("total_distance_ran").isNull() | F.col("running_time").isNull(), 1).otherwise(0)).alias("has_race_miss")
    )
    df = df.join(missing_flags, on="race_id", how="left")
    df = df.withColumn("race_missing_flag", F.when(F.col("has_race_miss") == 1, 1).otherwise(0))

    # Calculate global medians
    global_medians = df.agg(
        *[F.expr(f"percentile_approx({col}, 0.5)").alias(f"global_median_{col}") for col in TPD_COLS]
    ).collect()[0].asDict()

    # Fill row-level missing by official_fin or group median
    for col in TPD_COLS:
        median_col = f"median_{col}"
        global_median_col = f"global_median_{col}"

        # Calculate race medians
        race_medians = df.groupBy("race_id").agg(
            F.expr(f"percentile_approx({col}, 0.5)").alias(median_col)
        )
        df = df.join(race_medians, on="race_id", how="left")

        if col == "dist_bk_gate4":
            df = df.withColumn(
                col,
                F.when(F.col(col).isNull() & (F.col("official_fin") == 1), 0)
                .when(F.col(col).isNull(), F.col(median_col))
                .otherwise(F.col(col))
            )
        else:
            df = df.withColumn(
                col,
                F.when(F.col(col).isNull(), F.col(median_col))
                .otherwise(F.col(col))
            )

        # Fill remaining missing values with global median
        df = df.withColumn(
            col,
            F.when(F.col(col).isNull(), global_medians[global_median_col])
            .otherwise(F.col(col))
        )

        # Update missing_gps_flag and gps_present
        df = df.withColumn(
            "missing_gps_flag",
            F.when(F.col(col).isNull(), 1).otherwise(F.col("missing_gps_flag"))
        )

        df = df.drop(median_col)

    df = df.drop("race_id", "has_race_miss")
    
    return df

def class_multiplier(df):
    """
    Requirements:
    1. Check for class_rating Column: If the class_rating column is not present, the function assigns default 
    values for class_multiplier and class_offset.
    2. Convert class_rating to Numeric: The class_rating column is cast to a double type.
    3. Calculate Global Mean: The global mean for class_rating is calculated.
    4.  Calculate Race Means: The mean class_rating for each race is calculated.
    5. Fill Missing Values: Missing class_rating values are filled with the race mean, and if still missing, 
    with the global mean.
    6. Calculate Min, Max, and Median: The minimum, maximum, and median values for the filled class_rating are calculated.
    7. Calculate class_multiplier and class_offset: The class_multiplier and class_offset are calculated based on the 
    filled class_rating.
    8. Drop Intermediate Columns: Intermediate columns used for calculations are dropped.

    """
    if "class_offset" not in df.columns:
        df = df.withColumn("class_offset", F.lit(0.0))

    df = df.withColumn("class_rating_numeric", F.col("class_rating").cast("double"))

    # Calculate global mean for class_rating
    global_mean = df.agg(F.mean("class_rating_numeric")).collect()[0][0]

    # Calculate race means for class_rating
    race_means = df.groupBy("course_cd", "race_date", "race_number").agg(
        F.mean("class_rating_numeric").alias("race_mean_class_rating")
    )

    # Join race means back to the original DataFrame
    df = df.join(race_means, on=["course_cd", "race_date", "race_number"], how="left")

    # Fill missing class_rating with race mean, and if still missing, with global mean
    df = df.withColumn(
        "class_rating_filled",
        F.when(F.col("class_rating_numeric").isNull(), F.col("race_mean_class_rating"))
        .otherwise(F.col("class_rating_numeric"))
    )
    df = df.withColumn(
        "class_rating_filled",
        F.when(F.col("class_rating_filled").isNull(), global_mean)
        .otherwise(F.col("class_rating_filled"))
    )

    # Calculate min, max, and median for class_rating_filled
    cmin = df.agg(F.min("class_rating_filled")).collect()[0][0]
    cmax = df.agg(F.max("class_rating_filled")).collect()[0][0]
    cmed = df.agg(F.expr("percentile_approx(class_rating_filled, 0.5)")).collect()[0][0]

    # Calculate class_multiplier and class_offset
    df = df.withColumn(
        "class_multiplier",
        0.95 + (F.col("class_rating_filled") - cmin) * (1.15 - 0.95) / (cmax - cmin)
    )
    df = df.withColumn("class_offset", (F.col("class_rating_filled") - cmed) / 2)

    # Drop intermediate columns
    df = df.drop("class_rating_numeric", "race_mean_class_rating", "class_rating_filled")
    
    return df

def calc_raw_perf_score(df, alpha=50.0):
    """
    Raw Performance Score Calculation:
    # Assumption: full_df already includes:
        # 'distance_meters' (official distance),
        # 'running_time' (the horse's running time),
        # 'total_distance_ran' (actual distance run),
        # 'class_rating_numeric' (the numeric class rating),
        # If it has not been done, compute class_min and class_max from the data.
        # In all cases, if a value is null, populate with race mean, and if still missing, with global mean.
    Par Time Differential (D) which is nothing more than average time difference between the horse and the average time of
    all horses on a given course under given conditions for the same distance. 
    1. First, compute the “par time” for a race as the average running_time among horses in that race 
        (grouping by course, race_date, race_number, and track condition). Then, compute: 
        par_diff_ration = (par_time - running_time)/par_time
        This figure is used to compare the horse to other horses that have run the same course, at the same distance, 
        under similar track conditions.
    2. Compute the raw performance score as:
        raw_performance_score = base_speed * wide_factor * class_multiplier * (1 + alpha * par_diff_ratio)
    3. Compute the distance penalty as:
        dist_penalty = min(0.25 * dist_bk_gate4, 10)
    4. Subtract the distance penalty from the raw performance score.
    
    """
    df = df.withColumn("official_distance", F.col("distance_meters"))
    df = df.withColumn("base_speed", F.col("official_distance") / F.col("running_time"))
    df = df.withColumn("wide_factor", F.col("official_distance") / F.col("total_distance_ran"))

    grp_cols = ["course_cd", "race_date", "race_number", "trk_cond", "official_distance"]
    window_spec = Window.partitionBy(*grp_cols)
    df = df.withColumn("par_time", F.mean("running_time").over(window_spec))
    df = df.withColumn("par_diff_ratio", (F.col("par_time") - F.col("running_time")) / F.col("par_time"))

    df = df.withColumn(
        "raw_performance_score",
        F.col("base_speed") *
        F.col("wide_factor") *
        F.col("class_multiplier") *
        (1.0 + alpha * F.col("par_diff_ratio"))
    )
    df = df.withColumn("dist_penalty", F.least(0.25 * F.col("dist_bk_gate4"), F.lit(10.0)))
    df = df.withColumn("raw_performance_score", F.col("raw_performance_score") - F.col("dist_penalty"))

    return df

def compute_global_speed_figure(df):
    """
    1) Compute mean & std of raw_performance_score (mean_rps, std_rps).
    2) Create standardized_score = (raw_performance_score - mean_rps)/std_rps.
    3) normalized_score = tanh(standardized_score) in [-1, +1].
    4) For each (course_cd, race_date, race_number, horse_id), aggregate to get
       median_normalized = percentile_approx(normalized_score, 0.5).
    5) Final: global_speed_score = [40..150], mapping -1 => 40, +1 => 150.
    """

    # --- 1) Mean & Std of raw_performance_score ---
    stats = df.agg(
        F.mean("raw_performance_score").alias("mean_rps"),
        F.stddev("raw_performance_score").alias("std_rps")
    ).collect()[0]
    mean_rps = stats["mean_rps"]
    std_rps = stats["std_rps"]

    # Handle case where std_rps is 0 or null
    if not std_rps or std_rps == 0.0:
        # Just fill with 0's if we can't standardize
        df = df.withColumn("standardized_score", F.lit(0.0))
        df = df.withColumn("normalized_score", F.lit(0.0))
    else:
        # --- 2) standardize -> standardized_score ---
        df = df.withColumn(
            "standardized_score",
            (F.col("raw_performance_score") - F.lit(mean_rps)) / F.lit(std_rps)
        )
        # --- 3) normalized_score in [-1, +1] via tanh ---
        df = df.withColumn("normalized_score", F.tanh(F.col("standardized_score")))

    # --- 4) aggregator: per horse & race, get median of normalized_score
    # (NOT standardized_score) so it stays in [-1, +1]
    median_norm_df = (
        df.groupBy("course_cd", "race_date", "race_number", "horse_id")
          .agg(F.expr("percentile_approx(normalized_score, 0.5)").alias("median_normalized"))
    )

    # Join it back
    df = df.join(
        median_norm_df,
        on=["course_cd", "race_date", "race_number", "horse_id"],
        how="left"
    )

    # --- 5) Map from [-1..+1] => [40..150]
    #   formula: final_score = 40 + ( (x+1)/2 ) * (150 - 40 )
    #   i.e. -1 => 40, +1 => 150
    df = df.withColumn(
        "global_speed_score",
        F.lit(40.0) + (F.lit(150.0) - F.lit(40.0)) * ((F.col("median_normalized") + F.lit(1.0)) / F.lit(2.0))
    )

    return df

def join_and_merge_dataframes(historical_df: DataFrame, future_df: DataFrame) -> DataFrame:
    """
    1) Preserves all columns from historical_df and future_df.
    2) 'future' rows get their global_speed_score from the horse's most recent
       historical row (any date <= that future race_date).
    3) If none exists, use race median. If that is unavailable, use global median.
    4) Return one merged DataFrame with updated global_speed_score for future rows.

    Columns required in both DataFrames at minimum:
      - horse_id
      - race_date (must be a date or timestamp we can sort on)
      - global_speed_score (in historical_df, or computed earlier)
      - plus any other columns you want to keep.

    If your logic needs to also match on course_cd or ensure the same track,
    you can adapt the window's partition or filtering.
    """

    # 1) Mark historical vs. future
    historical_labeled = historical_df.withColumn("is_future", F.lit(False))
    future_labeled = future_df.withColumn("is_future", F.lit(True))

    # 2) Union them (Spark 3.1+ -> allowMissingColumns=True if schemas differ)
    df_all = historical_labeled.unionByName(future_labeled, allowMissingColumns=True)

    # 3) For each row, find the "most_recent_hist_gss":
    #    We'll store the global_speed_score only for historical rows in a new col "hist_gss".
    #    Then use a window partitioned by horse_id, ordered by race_date ascending,
    #    to fill forward the last known "hist_gss".
    
    # If you ONLY want to consider rows with race_date strictly < future race_date,
    # you may need a rangeBetween window with an upper bound of (currentRow - 1),
    # or handle the "equal to" case carefully. For simplicity, we'll allow "on or before" race_date.
    df_all = df_all.withColumn(
        "hist_gss",
        F.when(F.col("is_future") == F.lit(False), F.col("global_speed_score")).otherwise(F.lit(None))
    )

    window_spec = (
        Window
        .partitionBy("horse_id")
        .orderBy("race_date")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    # "most_recent_hist_gss" = the last non-null hist_gss up to this row's race_date
    df_all = df_all.withColumn(
        "most_recent_hist_gss",
        F.last("hist_gss", ignorenulls=True).over(window_spec)
    )

    # 4) Compute race-level median of "most_recent_hist_gss" across *all horses* in that race.
    #    Key for a race is (course_cd, race_date, race_number) or add track_name if needed
    race_group_cols = ["course_cd", "race_date", "race_number"]

    race_medians = (
        df_all
        .groupBy(*race_group_cols)
        .agg(
            F.expr("percentile_approx(most_recent_hist_gss, 0.5)").alias("race_median_gss")
        )
    )

    # 5) Global median across all rows (in practice, might prefer only historical rows).
    global_median = (
        df_all
        .agg(F.expr("percentile_approx(most_recent_hist_gss, 0.5)"))
        .collect()[0][0]
    )

    # Join the race medians so each row knows its race-level median
    df_all = df_all.join(race_medians, on=race_group_cols, how="left")

    # 6) Construct final speed figure for each row:
    #    - If we have "most_recent_hist_gss", use it
    #    - Else if race_median_gss is not null, use that
    #    - Else use the global median
    df_all = df_all.withColumn(
        "final_gss",
        F.when(
            F.col("most_recent_hist_gss").isNotNull(),
            F.col("most_recent_hist_gss")
        ).when(
            F.col("race_median_gss").isNotNull(),
            F.col("race_median_gss")
        ).otherwise(F.lit(global_median))
    )

    # 7) For FUTURE rows, we overwrite global_speed_score with final_gss.
    #    For HISTORICAL rows, keep the existing global_speed_score as is.
    df_all = df_all.withColumn(
        "global_speed_score",
        F.when(F.col("is_future"), F.col("final_gss")).otherwise(F.col("global_speed_score"))
    )

    # Optionally drop the helper columns
    df_all = df_all.drop("is_future", "hist_gss", "most_recent_hist_gss", "race_median_gss", "final_gss")

    return df_all

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

def assign_labels_spark(df, alpha=0.8):
    """
    Adds two columns to the Spark DataFrame:
      1) relevance: Exponential label computed as alpha^(official_fin - 1)
      2) top4_label: 1 if official_fin <= 4, else 0
    This update is applied only for rows where official_fin is not null.
    
    Parameters:
      df (DataFrame): A Spark DataFrame with an 'official_fin' column.
      alpha (float): Base of the exponential transformation.
    
    Returns:
      DataFrame: The input DataFrame with new columns 'relevance' and 'top4_label'.
    """
    df = df.withColumn(
        "relevance",
        F.when(
            F.col("official_fin").isNotNull(),
            F.pow(F.lit(alpha), F.col("official_fin") - 1)
        ).otherwise(F.lit(None).cast(DoubleType()))
    ).withColumn(
        "top4_label",
        F.when(
            F.col("official_fin").isNotNull(),
            F.when(F.col("official_fin") <= 4, F.lit(1)).otherwise(F.lit(0))
        ).otherwise(F.lit(None).cast(IntegerType()))
    )
    return df

def evaluate_and_save_global_speed_score_with_report(
    enhanced_df: DataFrame,
    parquet_dir: str,
    race_group_col: str = "group_id",
    class_col: str = "class_rating"
) -> DataFrame:
    """
    1) Computes correlation and RMSE between global_speed_score and relevance.
    2) Groups data by race (race_group_col) to produce aggregated race-level stats.
    3) Groups data by race and class to produce race-class aggregates.
    4) Joins these aggregated summaries back to the original DataFrame.
    5) Checks for duplicates and verifies that no horse is dropped during the join.
    6) Saves the race-level grouped summary as a Parquet file.
    7) Logs top/bottom rows by global_speed_score.
    
    :param enhanced_df: Spark DataFrame with columns including global_speed_score, official_fin,
                        a pre-assigned "relevance", horse_id, and class_col.
    :param parquet_dir: Destination directory to store the Parquet file.
    :param db_url: (Not used in this version) JDBC URL for PostgreSQL.
    :param db_properties: (Not used in this version) Database properties dictionary.
    :param race_group_col: Column name used for grouping by race (default "group_id").
    :param class_col: Column representing class (e.g. "class_rating").
    :return: The enriched DataFrame.
    """
    import os, time
    from pyspark.sql import functions as F

    start_time = time.time()

    # 1) Filter rows with valid relevance.
    metric_df = enhanced_df.filter(F.col("relevance").isNotNull())
    valid_count = metric_df.count()
    if valid_count == 0:
        logging.warning("No rows with valid relevance. Skipping correlation, RMSE, and report.")
        return enhanced_df

    # 2) Calculate correlation and RMSE.
    corr_val = metric_df.select(F.corr("global_speed_score", "relevance").alias("corr_score")).first()["corr_score"]
    mse_val = metric_df.select(F.mean(F.pow(F.col("global_speed_score") - F.col("relevance"), 2)).alias("mse")).first()["mse"]
    rmse_val = (mse_val ** 0.5) if mse_val is not None else None
    logging.info(f"Rows with valid relevance: {valid_count}")
    logging.info(f"Correlation (global_speed_score vs relevance): {corr_val:.4f}")
    if rmse_val is not None:
        logging.info(f"RMSE (global_speed_score vs relevance): {rmse_val:.4f}")
    else:
        logging.info("RMSE could not be computed (MSE was null).")

    # 3) Compute race-level aggregates (grouped by race_group_col).
    race_summary_df = (
        enhanced_df
        .groupBy(race_group_col)
        .agg(
            F.count("*").alias("race_count_agg"),
            F.mean("global_speed_score").alias("race_avg_speed_agg"),
            F.stddev("global_speed_score").alias("race_std_speed_agg"),
            F.mean("relevance").alias("race_avg_relevance_agg"),
            F.stddev("relevance").alias("race_std_relevance_agg")
        )
        .orderBy(race_group_col)
    )

    # 4) Compute race-class aggregates (grouped by race_group_col and class_col).
    race_class_summary_df = (
        enhanced_df
        .groupBy(race_group_col, class_col)
        .agg(
            F.count("*").alias("race_class_count_agg"),
            F.mean("global_speed_score").alias("race_class_avg_speed_agg"),
            F.stddev("global_speed_score").alias("race_class_std_speed_agg"),
            F.min("global_speed_score").alias("race_class_min_speed_agg"),
            F.max("global_speed_score").alias("race_class_max_speed_agg")
        )
        .orderBy(race_group_col, class_col)
    )

    # Save the race-level summary to Parquet for reference.
    race_summary_path = os.path.join(parquet_dir, "global_speed_score_accuracy.parquet")
    race_summary_df.write.mode("overwrite").parquet(race_summary_path)
    logging.info(f"Race-level grouped stats saved to Parquet: {race_summary_path}")

    # 5) Join the aggregated summaries back to the original DataFrame.
    # Note: We do not join the finishing position distribution summary to avoid duplicates.
    enriched_df = enhanced_df \
        .join(race_summary_df, on=race_group_col, how="left") \
        .join(race_class_summary_df, on=[race_group_col, class_col], how="left")

    # 6) Duplicate check.
    # Check for duplicates using composite key (race_group_col, horse_id, class_col).
    dup_df = enriched_df.groupBy(race_group_col, "horse_id", class_col) \
        .agg(F.count("*").alias("dup_count")) \
        .filter(F.col("dup_count") > 1)
    dup_count = dup_df.count()
    if dup_count > 0:
        logging.warning(f"Found {dup_count} duplicate rows based on {race_group_col}, horse_id, and {class_col}.")
        dup_df.show(10, truncate=False)
    else:
        logging.info("No duplicates found based on composite key (race_group_col, horse_id, class_col).")

    # Verify row counts.
    original_count = enhanced_df.count()
    enriched_count = enriched_df.count()
    logging.info(f"Original record count: {original_count}, enriched record count: {enriched_count}")
    if original_count != enriched_count:
        logging.warning("Row counts differ after join. Check join keys for mismatches.")
    else:
        logging.info("Row counts match after joining aggregated data.")

    # 7) (Optional) Log top/bottom 5 horses by global_speed_score.
    logging.info("Top 5 horses by global_speed_score:")
    for row in enhanced_df.orderBy(F.desc("global_speed_score")).limit(5).collect():
        logging.info(f"Horse {row['horse_id']} / FinPos={row['official_fin']} => Speed={row['global_speed_score']}")
    logging.info("Bottom 5 horses by global_speed_score:")
    for row in enhanced_df.orderBy("global_speed_score").limit(5).collect():
        logging.info(f"Horse {row['horse_id']} / FinPos={row['official_fin']} => Speed={row['global_speed_score']}")

    elapsed = time.time() - start_time
    logging.info(f"Evaluation & report generation completed in {elapsed:.2f} seconds.")

    return enriched_df

def enrich_with_race_and_class_stats(enhanced_df: DataFrame) -> DataFrame:
    """
    Enriches the horse-level DataFrame with aggregated race-level and race-class statistics.
    
    This function does the following:
      1. Creates a composite key "race_class_id" as the concatenation of group_id and class_rating.
      2. Computes race-level aggregates (e.g. count, average speed, stddev, etc.) grouped by group_id.
      3. Computes race-class aggregates (aggregated for each race-class combination) grouped by the composite key.
      4. Drops any columns from enhanced_df that conflict with the new aggregated column names.
      5. Joins the race-level aggregates (on group_id) and race-class aggregates (on race_class_id) back to the original DataFrame.
      6. Verifies row counts and checks for duplicates based on (group_id, horse_id).
      
    :param enhanced_df: Input DataFrame that includes at least:
                        - group_id (unique race identifier),
                        - class_rating (or the class column you use),
                        - global_speed_score, relevance, horse_id, etc.
    :return: The enriched DataFrame.
    """
    import time
    start_time = time.time()

    # 0) Drop any previously computed aggregate columns to avoid ambiguity.
    columns_to_drop = [
        "race_count_agg", "race_avg_speed_agg", "race_std_speed_agg", "race_avg_relevance_agg", "race_std_relevance_agg",
        "race_class_count_agg", "race_class_avg_speed_agg", "race_class_std_speed_agg", "race_class_min_speed_agg", "race_class_max_speed_agg"
    ]
    enhanced_df = enhanced_df.drop(*columns_to_drop)

    # 1) Create a composite key "race_class_id" combining group_id and class_rating.
    enhanced_df = enhanced_df.withColumn(
        "race_class_id",
        F.concat_ws("_", F.col("group_id"), F.col("class_rating").cast("string"))
    )

    # 2) Compute race-level aggregates, grouped by group_id.
    race_summary_df = (
        enhanced_df
        .groupBy("group_id")
        .agg(
            F.count("*").alias("race_count_agg"),
            F.mean("global_speed_score").alias("race_avg_speed_agg"),
            F.stddev("global_speed_score").alias("race_std_speed_agg"),
            F.mean("relevance").alias("race_avg_relevance_agg"),
            F.stddev("relevance").alias("race_std_relevance_agg")
        )
    ).na.fill({"race_std_relevance_agg": 0})

    # 3) Compute race-class aggregates, grouped by race_class_id.
    race_class_summary_df = (
        enhanced_df
        .groupBy("race_class_id")
        .agg(
            F.count("*").alias("race_class_count_agg"),
            F.mean("global_speed_score").alias("race_class_avg_speed_agg"),
            F.stddev("global_speed_score").alias("race_class_std_speed_agg"),
            F.min("global_speed_score").alias("race_class_min_speed_agg"),
            F.max("global_speed_score").alias("race_class_max_speed_agg")
        )
    )

    horse_summary_df = (
    enhanced_df
        .groupBy("horse_id")
        .agg(
            F.count("*").alias("horse_race_count_agg"),
            F.mean("global_speed_score").alias("horse_avg_speed_agg"),
            F.stddev("global_speed_score").alias("horse_std_speed_agg"),
            F.min("global_speed_score").alias("horse_min_speed_agg"),
            F.max("global_speed_score").alias("horse_max_speed_agg")
        )
    ).na.fill({"horse_std_speed_agg": 0})
    
    # 4) Join the aggregated summaries back to the original DataFrame.
    #    - Race-level aggregates join on "group_id".
    #    - Race-class aggregates join on "race_class_id".
    enriched_df = enhanced_df \
        .join(race_summary_df, on="group_id", how="left") \
        .join(race_class_summary_df, on="race_class_id", how="left") \
        .join(horse_summary_df, on="horse_id", how="left")
        
    # 5) Duplicate check: ensure each horse (identified by group_id and horse_id) appears only once.
    original_count = enhanced_df.count()
    enriched_count = enriched_df.count()
    print(f"Original record count: {original_count}, enriched record count: {enriched_count}")
    logging.info(f"Original record count: {original_count}, enriched record count: {enriched_count}")

    dup_df = enriched_df.groupBy("group_id", "horse_id") \
        .agg(F.count("*").alias("dup_count")) \
        .filter(F.col("dup_count") > 1)
    dup_count = dup_df.count()
    if dup_count > 0:
        logging.warning(f"Found {dup_count} duplicate rows based on group_id and horse_id:")
        dup_df.show(10, truncate=False)
    else:
        logging.info("No duplicates found based on group_id and horse_id.")

    elapsed = time.time() - start_time
    logging.info(f"Aggregation and join completed in {elapsed:.2f} seconds.")

    return enriched_df
  
def create_custom_speed_figure(df_input, jdbc_url, jdbc_properties, parquet_dir):
    """
        Consolidated pipeline:

        1) Separate historical and future data
        2) Process historical data to compute speed figures
        3) Re-join historical and future data
        4) Populate future data with historical speed figures if horse_id matches
        5) Return final DataFrame
    """
       # 1) Create a "relevance" column for finishing position
    enhanced_df = assign_labels_spark(df_input, alpha=0.8)
    
    # Separate historical and future data
    historical_df = enhanced_df.filter(F.col("data_flag") == "historical")
    future_df = enhanced_df.filter(F.col("data_flag") == "future")

    # Log the counts
    historical_count = historical_df.count()
    future_count = future_df.count()
    logging.info(f"Number of historical rows: {historical_count}")
    logging.info(f"Number of future rows: {future_count}")
    
    # Process historical data
    historical_df = mark_and_fill_missing(historical_df)         # Step 2
    historical_df = class_multiplier(historical_df)              # Step 3
    historical_df = calc_raw_perf_score(historical_df, alpha=50) # Step 4
    # historical_df.printSchema()
    # future_df.printSchema()
    
    historical_df = compute_global_speed_figure(historical_df)   # Step 5
    
    final_df = join_and_merge_dataframes(historical_df, future_df) # Step 6
    
    df_with_group = (
    final_df
    .withColumn("race_date_str", F.date_format("race_date", "yyyy-MM-dd"))
    .withColumn(
        "group_id",
        F.concat(
            F.col("course_cd"),
            F.lit("_"),
            F.col("race_date_str"),
            F.lit("_"),
            F.col("race_number").cast("string")
            )
        )
    )
    
    enriched_df = evaluate_and_save_global_speed_score_with_report( df_with_group, parquet_dir, "group_id", "class_rating")
    final_df = enrich_with_race_and_class_stats(enriched_df)
    
    return final_df
