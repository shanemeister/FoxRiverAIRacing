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
    df = df.withColumn("gps_present", F.lit(1))

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
        df = df.withColumn(
            "gps_present",
            F.when(F.col(col).isNull(), 0).otherwise(F.col("gps_present"))
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
    if "class_rating" not in df.columns:
        df = df.withColumn("class_multiplier", F.lit(1.0))
        df = df.withColumn("class_offset", F.lit(0.0))
        return df

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

@F.udf(DoubleType())
def piecewise_log_relevance(finishing_position):
    # Same as your previous code
    import numpy as np
    if finishing_position == 1:
        return 40.0
    elif finishing_position == 2:
        return 38.0
    elif finishing_position == 3:
        return 36.0
    elif finishing_position == 4:
        return 34.0
    else:
        alpha = 30.0
        beta  = 4.0
        if finishing_position is None or finishing_position <= 0:
            return None
        return float(alpha / np.log(beta + finishing_position))

def evaluate_and_save_global_speed_score_with_report(
    enhanced_df: DataFrame,
    parquet_dir: str,
    race_group_col: str = "group_id"
) -> None:
    """
    1) Assigns a piecewise log-based relevance for the finishing position.
    2) Calculates correlation and RMSE between `global_speed_score` and `relevance`.
    3) Groups data by `group_id` (or another column) to produce aggregated stats and saves them to Parquet.
    4) Produces an additional "report" grouped by official_fin: a quick distribution summary.
    5) Optionally logs top/bottom rows by global_speed_score.

    :param enhanced_df: Spark DataFrame with columns:
                       [global_speed_score, official_fin, <race_group_col>, ...]
    :param parquet_dir: Destination directory to store the Parquet file.
    :param race_group_col: Column name to group by for the main Parquet summary (defaults to group_id).
    """

    start_time = time.time()

    # 1) Create a "relevance" column for finishing position
    df_with_rel = enhanced_df.withColumn(
        "relevance", piecewise_log_relevance(F.col("official_fin"))
    )

    # Filter out rows lacking relevance
    metric_df = df_with_rel.filter(F.col("relevance").isNotNull())

    # If no rows remain, handle gracefully
    valid_count = metric_df.count()
    if valid_count == 0:
        logging.warning("No rows with valid relevance. Skipping correlation, RMSE, and report.")
        return

    # 2) Correlation and RMSE
    corr_val = metric_df.select(F.corr("global_speed_score", "relevance").alias("corr_score")).first()["corr_score"]
    mse_val = metric_df.select(F.mean(F.pow(F.col("global_speed_score") - F.col("relevance"), 2)).alias("mse")).first()["mse"]
    rmse_val = (mse_val ** 0.5) if mse_val is not None else None

    logging.info(f"Number of rows with relevance: {valid_count}")
    logging.info(f"Correlation (global_speed_score vs relevance): {corr_val:.4f}")
    if rmse_val is not None:
        logging.info(f"RMSE (global_speed_score vs relevance): {rmse_val:.4f}")
    else:
        logging.info("RMSE could not be computed (MSE was null).")

    # 3) Create a grouped summary by your race grouping column
    #    If you don't have group_id, you can do groupBy("course_cd", "race_date", "race_number"), etc.
    grouped_df = (
        df_with_rel
        .groupBy(race_group_col)
        .agg(
            F.count("*").alias("count_rows"),
            F.mean("global_speed_score").alias("avg_speed"),
            F.stddev("global_speed_score").alias("std_speed"),
            F.mean("relevance").alias("avg_relevance"),
            F.stddev("relevance").alias("std_relevance")
        )
        .orderBy(race_group_col)
    )

    # Save to Parquet
    grouped_parquet_path = os.path.join(parquet_dir, "global_speed_score_accuracy.parquet")
    grouped_df.write.mode("overwrite").parquet(grouped_parquet_path)
    logging.info(f"Grouped stats saved to Parquet: {grouped_parquet_path}")

    # 4) Produce an additional "report" grouped by official_fin
    #    This shows a quick distribution of speed score vs. finishing position.
    logging.info("Generating finishing position distribution stats...")
    finpos_stats_df = (
        df_with_rel
        .groupBy("official_fin")
        .agg(
            F.count("*").alias("count_positions"),
            F.mean("global_speed_score").alias("mean_speed"),
            F.stddev("global_speed_score").alias("std_speed"),
            F.mean("relevance").alias("mean_relevance"),
            F.stddev("relevance").alias("std_relevance")
        )
        .orderBy("official_fin")
    )

    def safe_fmt(val):
        if val is None:
            return "None"
        else:
            return f"{val:.2f}"
    
    
    
    # Collect to driver or just show:
    finpos_stats = finpos_stats_df.collect()
    for row in finpos_stats:
        logging.info(
            f"Finishing Pos: {row['official_fin']} | Count: {row['count_positions']} | "
            f"Mean Speed: {safe_fmt(row['mean_speed'])} | Std Speed: {safe_fmt(row['std_speed'])} | "
            f"Mean Rel: {safe_fmt(row['mean_relevance'])} | Std Rel: {safe_fmt(row['std_relevance'])}"
        )
    
    # 5) (Optional) Log top/bottom 5 by global_speed_score
    #    Sometimes helps see if extremely high/low speed scores match finishing positions
    logging.info("Top 5 horses by global_speed_score:")
    top_5_df = df_with_rel.orderBy(F.desc("global_speed_score")).limit(5).collect()
    for row in top_5_df:
        logging.info(f"Horse {row['horse_id']} / FinPos={row['official_fin']} => Speed={row['global_speed_score']}")

    logging.info("Bottom 5 horses by global_speed_score:")
    bot_5_df = df_with_rel.orderBy("global_speed_score").limit(5).collect()
    for row in bot_5_df:
        logging.info(f"Horse {row['horse_id']} / FinPos={row['official_fin']} => Speed={row['global_speed_score']}")

    elapsed = time.time() - start_time
    logging.info(f"Evaluation & report generation completed in {elapsed:.2f} seconds.")

def evaluate_horse_and_class_summaries(
    enhanced_df: DataFrame,
    parquet_dir: str,
    class_col: str = "previous_class"
) -> None:
    """
    Generate summary statistics (count, mean, std) of global_speed_score:
      1) Per horse_id,
      2) Per class_col (e.g. previous_class, class_rating, etc.),
      3) Per (horse_id, class_col) combination (optional),
    And then save each summary to Parquet for further inspection.

    :param enhanced_df: Spark DataFrame with at least:
        - horse_id
        - global_speed_score
        - [class_col] (the column that denotes class/grouping)
    :param parquet_dir: Directory to store output Parquet files
    :param class_col: Name of the column that represents "class" grouping
    """

    start_time = time.time()

    # 1) Summaries at the horse level
    horse_summary_df = (
        enhanced_df
        .groupBy("horse_id")
        .agg(
            F.count("*").alias("race_count"),
            F.mean("global_speed_score").alias("avg_speed"),
            F.stddev("global_speed_score").alias("std_speed"),
            F.min("global_speed_score").alias("min_speed"),
            F.max("global_speed_score").alias("max_speed"),
        )
        .orderBy("horse_id")
    )

    # Save it
    horse_summary_path = os.path.join(parquet_dir, "horse_speed_summary.parquet")
    horse_summary_df.write.mode("overwrite").parquet(horse_summary_path)
    logging.info(f"Horse-level speed summaries saved to: {horse_summary_path}")

    # 2) Summaries at the class level
    #    If you have a numeric class column (e.g. class_rating), or a text column (e.g. previous_class),
    #    it works the same. Just confirm the column is in your DataFrame.
    class_summary_df = (
        enhanced_df
        .groupBy(class_col)
        .agg(
            F.count("*").alias("race_count"),
            F.mean("global_speed_score").alias("avg_speed"),
            F.stddev("global_speed_score").alias("std_speed"),
            F.min("global_speed_score").alias("min_speed"),
            F.max("global_speed_score").alias("max_speed"),
        )
        .orderBy(class_col)
    )

    class_summary_path = os.path.join(parquet_dir, "class_speed_summary.parquet")
    class_summary_df.write.mode("overwrite").parquet(class_summary_path)
    logging.info(f"Class-level speed summaries saved to: {class_summary_path}")

    # 3) If you want a combined grouping (horse_id, class_col):
    #    This can reveal how each horse performs in each class level.
    horse_class_summary_df = (
        enhanced_df
        .groupBy("horse_id", class_col)
        .agg(
            F.count("*").alias("race_count"),
            F.mean("global_speed_score").alias("avg_speed"),
            F.stddev("global_speed_score").alias("std_speed"),
            F.min("global_speed_score").alias("min_speed"),
            F.max("global_speed_score").alias("max_speed"),
        )
        .orderBy("horse_id", class_col)
    )

    horse_class_summary_path = os.path.join(parquet_dir, "horse_class_speed_summary.parquet")
    horse_class_summary_df.write.mode("overwrite").parquet(horse_class_summary_path)
    logging.info(f"(Horse, Class)-level speed summaries saved to: {horse_class_summary_path}")

    # 4) (Optional) Print or log a small sample from each summary if desired
    #    For illustration, we show just the first 5 rows from each:
    logging.info("Sample: Horse-level summary (first 5 rows):")
    horse_sample = horse_summary_df.limit(5).collect()
    for row in horse_sample:
        logging.info(row)

    logging.info("Sample: Class-level summary (first 5 rows):")
    class_sample = class_summary_df.limit(5).collect()
    for row in class_sample:
        logging.info(row)

    logging.info("Sample: Horse-Class summary (first 5 rows):")
    hc_sample = horse_class_summary_df.limit(5).collect()
    for row in hc_sample:
        logging.info(row)

    elapsed = time.time() - start_time
    logging.info(f"Horse & Class summary evaluation completed in {elapsed:.2f} seconds.")
    
def evaluate_and_save_global_speed_score(enhanced_df: DataFrame, parquet_dir: str) -> None:
    """
    1) Calculates correlation and RMSE between `global_speed_score` and a "relevance" score
       derived from finishing position via a piecewise log-based formula.
    2) Groups data by `group_id`, sorts it, and saves grouped results to a Parquet file.
    3) Logs the completion.

    :param enhanced_df: Spark DataFrame with 'global_speed_score', 'official_fin', 'group_id'
    :param parquet_dir: Destination directory to store the Parquet file.
    """

    start_time = time.time()

    # --- 1) Assign piecewise log relevance ---
    # This is the Spark approach using a UDF:
    df_with_rel = enhanced_df.withColumn(
        "relevance",
        piecewise_log_relevance(F.col("official_fin"))
    )
    
    # Filter out rows lacking relevance
    metric_df = df_with_rel.filter(F.col("relevance").isNotNull())

    # If no rows remain, handle gracefully
    if metric_df.count() == 0:
        logging.warning("No rows with valid relevance. Skipping correlation and RMSE.")
    else:
        # --- 2) Calculate correlation (global_speed_score vs. relevance) ---
        corr_row = (metric_df
                    .select(F.corr("global_speed_score", "relevance").alias("corr_score"))
                    .collect()[0])
        correlation_value = corr_row["corr_score"]
        logging.info(f"Correlation (global_speed_score vs. piecewise relevance): {correlation_value:.4f}")

        # RMSE
        mse_row = metric_df.agg(
            F.mean(F.pow(F.col("global_speed_score") - F.col("relevance"), 2)).alias("mse")
        ).collect()[0]
        mse = mse_row["mse"]
        rmse_value = (mse ** 0.5) if mse is not None else None
        if rmse_value is not None:
            logging.info(f"RMSE (global_speed_score vs. piecewise relevance): {rmse_value:.4f}")
        else:
            logging.info("RMSE could not be computed (MSE was null).")

    # --- 3) Group, sort, save the summary ---
    grouped_df = (df_with_rel
                  .groupBy("group_id")
                  .agg(
                      F.count("*").alias("count_rows"),
                      F.mean("global_speed_score").alias("avg_speed"),
                      F.stddev("global_speed_score").alias("std_speed"),
                      F.mean("relevance").alias("avg_relevance"),
                      F.stddev("relevance").alias("std_relevance")
                  )
                  .orderBy("group_id"))

    grouped_parquet_path = os.path.join(parquet_dir, "global_speed_score_accuracy.parquet")
    grouped_df.write.mode("overwrite").parquet(grouped_parquet_path)
    logging.info(f"Grouped accuracy stats saved to Parquet: {grouped_parquet_path}")

    # --- 4) Log completion ---
    elapsed = time.time() - start_time
    logging.info(f"Evaluation with piecewise log relevance completed in {elapsed:.2f} seconds.")
    
# Spark UDF that replicates your piecewise logic
@F.udf(DoubleType())
def piecewise_log_relevance(finishing_position):
    if finishing_position == 1:
        return 40.0
    elif finishing_position == 2:
        return 38.0
    elif finishing_position == 3:
        return 36.0
    elif finishing_position == 4:
        return 34.0
    else:
        alpha = 30.0
        beta  = 4.0
        # if finishing_position is None or not numeric, handle gracefully:
        if finishing_position is None or finishing_position <= 0:
            return None
        # Example: alpha / log(beta + finishing_position)
        return float(alpha / np.log(beta + finishing_position))
      
def create_custom_speed_figure(spark, df_input, parquet_dir):
    """
        Consolidated pipeline:

        1) Separate historical and future data
        2) Process historical data to compute speed figures
        3) Re-join historical and future data
        4) Populate future data with historical speed figures if horse_id matches
        5) Return final DataFrame
    """
    
    # Separate historical and future data
    historical_df = df_input.filter(F.col("data_flag") == "historical")
    future_df = df_input.filter(F.col("data_flag") == "future")

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
    
    evaluate_and_save_global_speed_score(df_with_group , parquet_dir)
    evaluate_horse_and_class_summaries(df_with_group, parquet_dir,"previous_class")

    return df_with_group
