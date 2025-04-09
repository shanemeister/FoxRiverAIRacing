import os 
import logging
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import numpy as np
from scipy.stats import norm
from pyspark.sql import DataFrame
import time
import pyspark.sql.window as W
from math import exp, log, sqrt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import (
    col, when, lit, row_number, expr as F_expr, udf as F_udf,
    min as F_min, max as F_max, datediff, exp as F_exp,
    lag, count, trim, mean as F_mean, stddev as F_stddev, coalesce as F_coalesce
)
import math

def impute_race_avg_relevance_agg(
    enhanced_df,
    horse_id_col="horse_id",
    date_col="race_date",
    race_key=("course_cd","race_date","race_number"),
    col_name="race_avg_relevance_agg"
):
    """
    Imputes the column 'race_avg_relevance_agg' in the following order:
      1) LOCF within each horse partition, sorted by date_col ascending.
      2) Race-level mean (grouping by race_key).
      3) Global mean if still null.

    Returns a new DataFrame with the 'race_avg_relevance_agg' column imputed.

    Parameters:
      df (DataFrame): The input Spark DataFrame containing 'race_avg_relevance_agg'.
      horse_id_col (str): Column identifying the horse (for LOCF).
      date_col (str): Column used to order rows ascending for forward fill.
      race_key (tuple or list of str): Columns defining a race partition (e.g. course_cd,race_date,race_number).
      col_name (str): The column to be imputed (default "race_avg_relevance_agg").

    Steps:
      A) Forward-fill LOCF by (horse_id_col), ascending on date_col.
      B) Race-level mean => groupBy(*race_key).agg(mean of col_name).
      C) Global mean => fill all remaining null with that.
    """

    # ---------------------------------------------
    # 1) LOCF (forward fill) in ascending date order, partitioned by horse
    # ---------------------------------------------
    
    w_locf = (
        Window
        .partitionBy(horse_id_col)
        .orderBy(F.col(date_col).asc())
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    
    # For a single column, we do last(col_name, ignorenulls=True)
    locf_expr = F.last(F.col(col_name), ignorenulls=True).over(w_locf)
    
    df_locf = enhanced_df.withColumn(
        "col_name",
        F.coalesce(F.col(col_name), locf_expr)
    )

    # ---------------------------------------------
    # 2) Race-level mean
    #    group by race_key => average of col_name => left join => coalesce
    # ---------------------------------------------
    race_means_expr = F.mean(col_name).alias(f"{col_name}_race_mean")

    df_race_means = df_locf.groupBy(*race_key).agg(race_means_expr)

    df_joined = df_locf.join(df_race_means, on=list(race_key), how="left")

    df_joined = df_joined.withColumn(
        col_name,
        F.coalesce(F.col(col_name), F.col(f"{col_name}_race_mean"))
    )

    # ---------------------------------------------
    # 3) Global mean
    #    single aggregator => fill remaining null with that
    # ---------------------------------------------
    global_mean = df_joined.agg(F.mean(col_name).alias("gm")).collect()[0]["gm"]

    df_imputed = df_joined.withColumn(
        col_name,
        F.coalesce(F.col(col_name), F.lit(global_mean))
    )

    # (Optional) drop the helper race mean column
    df_imputed = df_imputed.drop(f"{col_name}_race_mean")

    return df_imputed

def fill_null_with_global_or_constant(df, cols, fill_val=None):
    """
    For each column in cols, fill any remaining null with either
    a global mean or a given constant fill_val.
    If fill_val is None, we compute the global mean of that column.
    """
    from pyspark.sql import functions as F
    
    df_out = df
    for c in cols:
        if fill_val is not None:
            # use a constant
            df_out = df_out.withColumn(c, F.coalesce(F.col(c), F.lit(fill_val)))
        else:
            # compute global mean
            gmean = df_out.agg(F.mean(c).alias("gm")).collect()[0]["gm"]
            df_out = df_out.withColumn(c, F.coalesce(F.col(c), F.lit(gmean)))
    return df_out

def fill_forward_and_backward_locf(
    df,
    columns_to_update,
    partition_col="horse_id",
    date_col="race_date",
    race_num_col="race_number"
):
    """
    Performs BOTH forward and backward fill (LOCF) on specified columns:
      1) Forward fill in ascending (date_col, race_num_col).
      2) Then backward fill in descending (date_col, race_num_col).
    
    This ensures that if forward fill didn't catch a null (i.e., first row(s) were null),
    the backward fill might fill it from a future row (and vice versa).
    
    :param df:               Spark DataFrame.
    :param columns_to_update: List of column names to fill in.
    :param partition_col:    Partitioning column (e.g. 'horse_id').
    :param date_col:         Date/time column for ordering.
    :param race_num_col:     Additional ordering column to break ties in the same date.
    
    :return: DataFrame with the columns forward+backward-filled.
    """
    
    # 1) Forward fill in ascending order
    w_asc = (
        W.Window
        .partitionBy(partition_col)
        .orderBy(F.col(date_col).asc(), F.col(race_num_col).asc())
        .rowsBetween(W.Window.unboundedPreceding, W.Window.currentRow)
    )
    df_ff = df
    for col_name in columns_to_update:
        df_ff = df_ff.withColumn(
            col_name,
            F.last(F.col(col_name), ignorenulls=True).over(w_asc)
        )
    
    # 2) Backward fill in descending order
    w_desc = (
        W.Window
        .partitionBy(partition_col)
        .orderBy(F.col(date_col).desc(), F.col(race_num_col).desc())
        .rowsBetween(W.Window.unboundedPreceding, W.Window.currentRow)
    )
    df_bf = df_ff
    for col_name in columns_to_update:
        df_bf = df_bf.withColumn(
            col_name,
            F.last(F.col(col_name), ignorenulls=True).over(w_desc)
        )
    
    return df_bf

def acklam_icdf(p: float) -> float:
    """
    Approximate the inverse CDF (quantile) of the standard normal distribution.
    Implementation of the algorithm by Peter J. Acklam (2000/2010).
    Returns z such that Phi(z) = p, for 0 < p < 1.
    
    References:
      - https://web.archive.org/web/20151030215612/http://home.online.no/~pjacklam/notes/invnorm/
      - Original paper: "An algorithm for computing the inverse normal cumulative distribution function"
      
    Edge cases:
      - p <= 0.0 => -inf
      - p >= 1.0 => +inf
    """
    if p <= 0.0:
        return float('-inf')
    if p >= 1.0:
        return float('inf')
    
    # Coefficients in rational approximations
    a1 = -3.969683028665376e+01
    a2 =  2.209460984245205e+02
    a3 = -2.759285104469687e+02
    a4 =  1.383577518672690e+02
    a5 = -3.066479806614716e+01
    a6 =  2.506628277459239e+00

    b1 = -5.447609879822406e+01
    b2 =  1.615858368580409e+02
    b3 = -1.556989798598866e+02
    b4 =  6.680131188771972e+01
    b5 = -1.328068155288572e+01

    c1 = -7.784894002430293e-03
    c2 = -3.223964580411365e-01
    c3 = -2.400758277161838e+00
    c4 = -2.549732539343734e+00
    c5 =  4.374664141464968e+00
    c6 =  2.938163982698783e+00

    d1 =  7.784695709041462e-03
    d2 =  3.224671290700398e-01
    d3 =  2.445134137142996e+00
    d4 =  3.754408661907416e+00
    d5 =  1.000000000000000e+00
    
    # Define break-points
    p_low  = 0.02425
    p_high = 1.0 - p_low
    
    # Rational approximation for lower region
    if p < p_low:
        q = math.sqrt(-2.0 * math.log(p))
        return (((((c1*q + c2)*q + c3)*q + c4)*q + c5)*q + c6) / \
               ((((d1*q + d2)*q + d3)*q + d4)*q + d5)
    
    # Rational approximation for upper region
    if p > p_high:
        q = math.sqrt(-2.0 * math.log(1.0 - p))
        return -(((((c1*q + c2)*q + c3)*q + c4)*q + c5)*q + c6) / \
                ((((d1*q + d2)*q + d3)*q + d4)*q + d5)
    
    # Rational approximation for central region
    q = p - 0.5
    r = q * q
    return (((((a1*r + a2)*r + a3)*r + a4)*r + a5)*r + a6)*q / \
           (((((b1*r + b2)*r + b3)*r + b4)*r + b5)*r + 1.0)
           
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
    # RACE_KEYS = ["course_cd", "race_date", "race_number"]

    # Create new columns
    df = df.withColumn("race_missing_flag", F.lit(0))
    df = df.withColumn("missing_gps_flag", F.lit(0))

    # Build a 'race_id' for grouping
    # df = df.withColumn("race_id", F.concat_ws("_", *RACE_KEYS))

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

    df = df.drop("has_race_miss")
    
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
        col("base_speed") *
        col("wide_factor") *
        col("class_multiplier") *
        (1.0 + alpha * col("par_diff_ratio"))
    )
    df = df.withColumn("dist_penalty", F.least(0.25 * col("dist_bk_gate4"), lit(10.0)))
    df = df.withColumn("raw_performance_score", col("raw_performance_score") - col("dist_penalty"))

    return df

def compute_horse_speed_figure(df: DataFrame, alpha_logistic=1.0, epsilon=1e-6) -> DataFrame:
    """
    Computes a horse-specific speed figure.
    Each horse is standardized against its own distribution of raw_performance_score.
    
    Steps:
      A) Group by horse_id => compute horse_mean_rps, horse_std_rps
      B) Join back to main DF => each row sees the horse's mean/std
      C) Standardize => (raw_performance_score - horse_mean_rps) / horse_std_rps
         If std=0 => set standardized_score=0
      D) [Optional] Apply logistic transform => median => inverseCDF => final score
         Illustrative example shown here.

    :param df: DataFrame with columns:
               - horse_id
               - raw_performance_score
               - course_cd, race_date, race_number, etc. (for grouping/identification)
    :param alpha_logistic: logistic transform steepness
    :param epsilon: small clamp to avoid 0/1 extremes
    :return: DataFrame with new columns:
             - horse_mean_rps, horse_std_rps (for debugging)
             - standardized_score (horse-based z-score)
             - logistic_score, median_logistic, global_speed_score_iq (if you choose to replicate IQ transform)
    """

    # A) Compute horse-level stats
    horse_stats = (
        df.groupBy("horse_id")
          .agg(
              F.mean("raw_performance_score").alias("horse_mean_rps"),
              F.stddev("raw_performance_score").alias("horse_std_rps")
          )
    )

    # A2) Fill any null std with 0.0
    #    i.e., if a horse has only one race or perfect uniform scores => stddev is null => set to 0.0
    horse_stats_filled = horse_stats.na.fill({"horse_std_rps": 0.0})
    
    # B) Join these stats back
    # C) Join back
    df_joined = df.join(horse_stats_filled, on="horse_id", how="left")

    # E) Create standardized_score
    #    If horse_std_rps==0 => standardized_score=0
    #    Else => (raw - mean)/std
    df_joined = df_joined.withColumn(
        "standardized_score",
        F.when(F.col("horse_std_rps") == 0.0, F.lit(0.0))
         .otherwise(
            (F.col("raw_performance_score") - F.col("horse_mean_rps")) / F.col("horse_std_rps")
         )
    )
    
    # ----- OPTIONAL: replicate or adapt the logistic -> median -> inverse CDF -> IQ scale

    # 1) logistic transform => (0,1)
    df_joined = df_joined.withColumn(
        "logistic_score",
        1.0 / (1.0 + F.exp(-lit(alpha_logistic) * col("standardized_score")))
    )

    # 2) Group by (course_cd, race_date, race_number, horse_id) => median logistic_score
    median_logistic_df = (
        df_joined.groupBy("course_cd", "race_date", "race_number", "horse_id")
                 .agg(F.expr("percentile_approx(logistic_score, 0.5)").alias("median_logistic"))
    )
    df_joined = df_joined.join(
        median_logistic_df,
        on=["course_cd", "race_date", "race_number", "horse_id"],
        how="left"
    )

    # 3) Clamp median_logistic in [epsilon, 1 - epsilon]
    df_joined = df_joined.withColumn(
        "median_logistic_clamped",
        F.when(col("median_logistic") < epsilon, epsilon)
         .when(col("median_logistic") > 1 - epsilon, 1 - epsilon)
         .otherwise(col("median_logistic"))
    )

    # 4) Inverse CDF (Acklam) => z_iq => scale to ~ IQ
    from pyspark.sql.types import DoubleType
    invcdf_udf = F.udf(lambda x: float(acklam_icdf(x)) if x is not None else None, DoubleType())

    df_joined = df_joined.withColumn(
        "global_speed_score_iq",
        lit(100.0) + lit(15.0) * invcdf_udf(col("median_logistic_clamped"))
    )

    # 5) Optional clamp => [0..200]
    df_joined = df_joined.withColumn(
        "global_speed_score_iq",
        F.when(col("global_speed_score_iq") < 0, 0)
         .when(col("global_speed_score_iq") > 200, 200)
         .otherwise(col("global_speed_score_iq"))
    )

    return df_joined

def join_and_merge_dataframes(historical_df: DataFrame, future_df: DataFrame) -> DataFrame:
    """
    1) Preserves all columns from historical_df and future_df.
    2) 'future' rows get their global_speed_score_iq from the horse's most recent
       historical row (any date <= that future race_date).
    3) If none exists, use race median. If that is unavailable, use global median.
    4) Return one merged DataFrame with updated global_speed_score_iq for future rows.

    Columns required in both DataFrames at minimum:
      - horse_id
      - race_date (must be a date or timestamp we can sort on)
      - global_speed_score_iq (in historical_df, or computed earlier)
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
    #    We'll store the global_speed_score_iq only for historical rows in a new col "hist_gss".
    #    Then use a window partitioned by horse_id, ordered by race_date ascending,
    #    to fill forward the last known "hist_gss".
    
    # If you ONLY want to consider rows with race_date strictly < future race_date,
    # you may need a rangeBetween window with an upper bound of (currentRow - 1),
    # or handle the "equal to" case carefully. For simplicity, we'll allow "on or before" race_date.
    df_all = df_all.withColumn(
        "hist_gss",
        F.when(F.col("is_future") == F.lit(False), F.col("global_speed_score_iq")).otherwise(F.lit(None))
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

    # 7) For FUTURE rows, we overwrite global_speed_score_iq with final_gss.
    #    For HISTORICAL rows, keep the existing global_speed_score_iq as is.
    df_all = df_all.withColumn(
        "global_speed_score_iq",
        F.when(F.col("is_future"), F.col("final_gss")).otherwise(F.col("global_speed_score_iq"))
    )

    # Optionally drop the helper columns
    df_all = df_all.drop("is_future", "hist_gss", "most_recent_hist_gss", "race_median_gss", "final_gss")

    return df_all

from pyspark.sql import functions as F

# def assign_piecewise_log_labels_spark(df, alpha = 30.0, beta = 4.0):
#     """
#     Assigns a 'relevance' column based on finishing position:
#       1st => 70
#       2nd => 56
#       3rd => 44
#       4th => 34
#       else => alpha / log(beta + official_fin)
#     Also assigns 'top4_label' = 1 if official_fin <= 4 else 0.

#     Parameters:
#       df (DataFrame): A Spark DataFrame that has 'official_fin' column.

#     Returns:
#       DataFrame: Spark DataFrame with new columns 'relevance' and 'top4_label'.
#     """

#     # Build the piecewise logic using Spark's 'when' chains:
#     df_out = df.withColumn(
#         "relevance",
#         F.when(F.col("official_fin") == 1, 70.0)
#          .when(F.col("official_fin") == 2, 56.0)
#          .when(F.col("official_fin") == 3, 44.0)
#          .when(F.col("official_fin") == 4, 34.0)
#          # For 5th or worse:
#          .otherwise(
#              F.lit(alpha) / F.log(F.lit(beta) + F.col("official_fin"))
#          )
#     ).withColumn(
#         "top4_label",
#         F.when(F.col("official_fin") <= 4, F.lit(1)).otherwise(F.lit(0))
#     )

#     return df_out

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

def evaluate_and_save_global_speed_score_iq_with_report(
    enhanced_df: DataFrame,
    parquet_dir: str,
    race_group_col: str = "group_id",
    class_col: str = "class_rating"
) -> DataFrame:
    """
    1) Computes correlation and RMSE between global_speed_score_iq and relevance.
    2) Groups data by race (race_group_col) to produce aggregated race-level stats.
    3) Groups data by race and class to produce race-class aggregates.
    4) Joins these aggregated summaries back to the original DataFrame.
    5) Checks for duplicates and verifies that no horse is dropped during the join.
    6) Saves the race-level grouped summary as a Parquet file.
    7) Logs top/bottom rows by global_speed_score_iq.
    
    :param enhanced_df: Spark DataFrame with columns including global_speed_score_iq, official_fin,
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
    corr_val = metric_df.select(F.corr("global_speed_score_iq", "relevance").alias("corr_score")).first()["corr_score"]
    mse_val = metric_df.select(F.mean(F.pow(F.col("global_speed_score_iq") - F.col("relevance"), 2)).alias("mse")).first()["mse"]
    rmse_val = (mse_val ** 0.5) if mse_val is not None else None
    logging.info(f"Rows with valid relevance: {valid_count}")
    logging.info(f"Correlation (global_speed_score_iq vs relevance): {corr_val:.4f}")
    if rmse_val is not None:
        logging.info(f"RMSE (global_speed_score_iq vs relevance): {rmse_val:.4f}")
    else:
        logging.info("RMSE could not be computed (MSE was null).")

    # 3) Compute race-level aggregates (grouped by race_group_col).
    race_summary_df = (
        enhanced_df
        .groupBy(race_group_col)
        .agg(
            F.count("*").alias("race_count_agg"),
            F.mean("global_speed_score_iq").alias("race_avg_speed_agg"),
            F.stddev("global_speed_score_iq").alias("race_std_speed_agg"),
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
            F.mean("global_speed_score_iq").alias("race_class_avg_speed_agg"),
            F.min("global_speed_score_iq").alias("race_class_min_speed_agg"),
            F.max("global_speed_score_iq").alias("race_class_max_speed_agg")
        )
        .orderBy(race_group_col, class_col)
    )

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

    # 7) (Optional) Log top/bottom 5 horses by global_speed_score_iq.
    logging.info("Top 5 horses by global_speed_score_iq:")
    for row in enhanced_df.orderBy(F.desc("global_speed_score_iq")).limit(5).collect():
        logging.info(f"Horse {row['horse_id']} / FinPos={row['official_fin']} => Speed={row['global_speed_score_iq']}")
    logging.info("Bottom 5 horses by global_speed_score_iq:")
    for row in enhanced_df.orderBy("global_speed_score_iq").limit(5).collect():
        logging.info(f"Horse {row['horse_id']} / FinPos={row['official_fin']} => Speed={row['global_speed_score_iq']}")

    elapsed = time.time() - start_time
    logging.info(f"Evaluation & report generation completed in {elapsed:.2f} seconds.")

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

    # # Separate historical and future data
    # historical_df = enhanced_df.filter(F.col("data_flag") == "historical")
    # future_df = enhanced_df.filter(F.col("data_flag") == "future")

    # # Log the counts
    # historical_count = historical_df.count()
    # future_count = future_df.count()
    # logging.info(f"Number of historical rows: {historical_count}")
    # logging.info(f"Number of future rows: {future_count}")
    
    # Process historical data
    # enhanced_df = mark_and_fill_missing(enhanced_df)         # Step 2
    enhanced_df = class_multiplier(enhanced_df)              # Step 3
    enhanced_df = calc_raw_perf_score(enhanced_df, alpha=50) # Step 4
    enhanced_df = compute_horse_speed_figure(enhanced_df)

    
    df_with_group = (
    enhanced_df
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

    enriched_df = evaluate_and_save_global_speed_score_iq_with_report( df_with_group, parquet_dir, "group_id", "class_rating")

    columns_to_update_final = ['race_std_speed_agg',
                'race_std_relevance_agg']
        
    enriched_df = fill_forward_and_backward_locf(enriched_df, columns_to_update_final, "horse_id", "race_date", "race_number") 
    
    enriched_df = fill_null_with_global_or_constant(enriched_df, columns_to_update_final, fill_val=0.0)
    
    race_key=["course_cd","race_date","race_number"]
    
    enriched_df = impute_race_avg_relevance_agg(enriched_df, "horse_id","race_date", race_key, "race_avg_relevance_agg")
    
    count_hist = enriched_df.filter(F.col("data_flag") == "historical").count()
    count_fut = enriched_df.filter(F.col("data_flag") == "future").count()
    count_total = enriched_df.count()

    logging.info(f"Final DF total count: {count_total}")
    logging.info(f"Final DF count for historical: {count_hist}")
    logging.info(f"Final DF count for future: {count_fut}")
    
    # Drop the column "col_name"
    enriched_df = enriched_df.drop("col_name")
    
    return enriched_df
