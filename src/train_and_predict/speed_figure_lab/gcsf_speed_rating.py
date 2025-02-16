import pandas as pd
import numpy as np
import warnings
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error
from src.data_preprocessing.data_prep1.data_utils import save_parquet, initialize_environment

# Suppress the SQLAlchemy warning for non-SQLAlchemy connections.
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

# ------------------------------------------------------------------------------
# Compute GCSF target tied to track (course_cd)
# ------------------------------------------------------------------------------
def compute_gcsf_target(df, group_keys, ALPHA=0.1):
    """
    Compute a continuous GCSF target for each race.
    The idea is:
      - For each track (course_cd), determine a par time (if not provided, use the median
        of winners' time_to_finish as par time).
      - If the beaten_lengths feature exists, define an effective finishing time:
            effective_time = time_to_finish + ALPHA * beaten_lengths
        Otherwise, use time_to_finish.
      - Compute speed_diff = part_time - effective_time.
      - Scale speed_diff for each track to the range [40, 120] (with 120 being best).
    
    Parameters:
      df         : DataFrame containing at least the columns:
                   time_to_finish, course_cd, and optionally beaten_lengths.
      group_keys : The grouping keys (e.g., ["horse_id", "course_cd", "race_date", "race_number"])
      ALPHA      : A constant to weight beaten_lengths (adjust as needed).
    
    Returns:
      The original DataFrame with new columns 'gcsf_target' and 'part_time' added.
    """
    # If 'part_time' is not already in the DataFrame, compute it per track.
    if 'part_time' not in df.columns:
        # For each race, the winner's time is the minimum time_to_finish.
        race_winners = df.groupby(["course_cd", "race_date", "race_number"])['time_to_finish'].min().reset_index()
        # For each track, compute the median winning time as the par time.
        par_times = race_winners.groupby("course_cd")['time_to_finish'].median().reset_index().rename(
            columns={'time_to_finish': 'part_time'})
        df = df.merge(par_times, on="course_cd", how='left')
    
    # Define effective_time: if beaten_lengths is available, add its weighted penalty.
    if 'beaten_lengths' in df.columns:
        df['effective_time'] = df['time_to_finish'] + ALPHA * df['beaten_lengths']
    else:
        df['effective_time'] = df['time_to_finish']
    
    # Compute speed_diff as the difference between par time and the effective finishing time.
    df['speed_diff'] = df['part_time'] - df['effective_time']
    
    # Drop any rows where speed_diff is NaN.
    df = df.dropna(subset=['speed_diff'])
    
    def scale_group(group):
        min_diff = group['speed_diff'].min()
        max_diff = group['speed_diff'].max()
        if max_diff == min_diff:
            group['gcsf_target'] = 120.0
        else:
            # Linear scaling: winner (largest diff) gets 120, worst gets 40.
            group['gcsf_target'] = 40 + (group['speed_diff'] - min_diff) / (max_diff - min_diff) * 80
        return group

    # Scale within each track.
    df = df.groupby("course_cd", group_keys=False).apply(scale_group)
    return df

# ------------------------------------------------------------------------------
# Helper function: Clamp outliers in a Pandas DataFrame
# ------------------------------------------------------------------------------
def fix_outliers_pandas(df):
    outlier_bounds = {
        "sectional_time": (0, 20.0),
        "running_time": (0, 60.0),
        "number_of_strides": (0, 50.0),
        "length_to_finish": (0, 2000.0),
        "distance_ran": (0, 200.0),
        "distance_back": (0, 50.0),
        "speed": (0, 20.0),
        "progress": (0, 2500.0),
    }
    for col, (min_val, max_val) in outlier_bounds.items():
        if col in df.columns:
            df[col] = df[col].clip(lower=min_val, upper=max_val)
    for col in ["claimprice", "purse", "avg_speed_5"]:
        if col in df.columns:
            lower = df[col].quantile(0.01)
            upper = df[col].quantile(0.99)
            df[col] = df[col].clip(lower=lower, upper=upper)
    return df

# ------------------------------------------------------------------------------
# Compute TTR metrics (Time To Reach thresholds)
# ------------------------------------------------------------------------------
def compute_ttr_metrics(gps_df, group_keys, thresholds=[5, 10, 20, 25, 30]):
    """
    For each horse in each race, compute the time from gate off to when the horse
    first reaches each specified speed threshold. Returns a DataFrame with columns:
    TTR_5, TTR_10, etc.
    """
    def ttr_for_group(group):
        group = group.sort_values("time_stamp")
        ttr_vals = {}
        for t in thresholds:
            # Find the first row where speed >= threshold t.
            sub = group[group["speed"] >= t]
            if not sub.empty:
                # Use the time_since_gate of the first record meeting the threshold.
                ttr_vals[f"TTR_{t}"] = sub.iloc[0]["time_since_gate"]
            else:
                ttr_vals[f"TTR_{t}"] = np.nan
        return pd.Series(ttr_vals)

    # Pass include_groups=False to exclude the grouping columns from being passed to ttr_for_group.
    ttr_df = gps_df.groupby(group_keys).apply(ttr_for_group).reset_index()
    return ttr_df

# ------------------------------------------------------------------------------
# Compute ROS (Run Out Speed)
# ------------------------------------------------------------------------------
def compute_ros(gps_df, group_keys, window=2):
    """
    For each horse in each race, compute the average speed over a window (in seconds)
    immediately after finishing. Finish is defined as the first time when progress <= 1.0.
    """
    def ros_for_group(group):
        group = group.sort_values("time_stamp")
        finish_time = group[group["progress"] <= 1.0]["time_since_gate"].min()
        if pd.isna(finish_time):
            return pd.Series({"ROS": np.nan})
        # Filter rows in the window [finish_time, finish_time + window).
        sub = group[(group["time_since_gate"] >= finish_time) & (group["time_since_gate"] < finish_time + window)]
        if sub.empty:
            return pd.Series({"ROS": np.nan})
        return pd.Series({"ROS": sub["speed"].mean()})
    ros_df = gps_df.groupby(group_keys).apply(ros_for_group).reset_index()
    return ros_df

# ------------------------------------------------------------------------------
# Compute VP (Velocity Peak) and PSF (Stride Frequency at Peak Speed)
# ------------------------------------------------------------------------------
def compute_vp_psf(gps_df, group_keys):
    """
    For each horse in each race, compute:
      - VP: The maximum speed reached during the race.
      - PSF: The stride frequency at the moment when VP is reached.
    """
    def vp_psf_for_group(group):
        group = group.sort_values("time_stamp")
        idx = group["speed"].idxmax()
        return pd.Series({
            "VP": group.loc[idx, "speed"],
            "PSF": group.loc[idx, "stride_frequency"]
        })
    vp_psf_df = gps_df.groupby(group_keys).apply(vp_psf_for_group).reset_index()
    return vp_psf_df

# ------------------------------------------------------------------------------
# Compute Best Position (BP) from sectionals (minimum distance_back)
# ------------------------------------------------------------------------------
def compute_best_position(sectionals_df, group_keys):
    """
    For each horse in each race, compute the best in-running position as the minimum
    distance_back value from the sectionals data (assuming lower is better).
    """
    bp_df = sectionals_df.groupby(group_keys)["distance_back"].min().reset_index().rename(
        columns={"distance_back": "BP"})
    return bp_df

# ------------------------------------------------------------------------------
# Main Data Processing and Feature Engineering Function
# ------------------------------------------------------------------------------
def gps_composite_speed_figure(conn):
    from src.train_and_predict.speed_figure_lab.gcsf_query import gcsp_sql_queries
    queries = gcsp_sql_queries()

    # Load data using your psycopg2 connection.
    gps_query = queries['gpspoint']
    gps_df = pd.read_sql(gps_query, conn)
    gps_df = fix_outliers_pandas(gps_df)

    sectionals_query = queries['sectionals']
    sectionals_df = pd.read_sql(sectionals_query, conn)
    sectionals_df = sectionals_df.dropna(subset=["distance_back", "number_of_strides"])
    sectionals_df = fix_outliers_pandas(sectionals_df)

    # Parse datetime columns.
    gps_df['time_stamp'] = pd.to_datetime(gps_df['time_stamp'])
    if 'race_date' in gps_df.columns:
        gps_df['race_date'] = pd.to_datetime(gps_df['race_date'])
    if 'race_date' in sectionals_df.columns:
        sectionals_df['race_date'] = pd.to_datetime(sectionals_df['race_date'])

    # Define grouping keys.
    group_keys = ["horse_id", "course_cd", "race_date", "race_number"]

    # Compute basic time-based features.
    gps_df['start_ts'] = gps_df.groupby(group_keys)['time_stamp'].transform('min')
    gps_df['time_since_gate'] = (gps_df['time_stamp'] - gps_df['start_ts']).dt.total_seconds()
    time_to_finish = (
        gps_df[gps_df['progress'] <= 1.0]
        .groupby(group_keys)['time_since_gate']
        .min()
        .reset_index()
        .rename(columns={'time_since_gate': 'time_to_finish'})
    )
    time_to_200 = (
        gps_df[gps_df['progress'] <= 200.0]
        .groupby(group_keys)['time_since_gate']
        .min()
        .reset_index()
        .rename(columns={'time_since_gate': 'time_to_200'})
    )

    # Aggregate additional features.
    avg_speed = (
        gps_df.groupby(group_keys)['speed']
        .mean()
        .reset_index()
        .rename(columns={'speed': 'avg_speed'})
    )
    speed_stddev = (
        gps_df.groupby(group_keys)['speed']
        .std()
        .reset_index()
        .rename(columns={'speed': 'speed_stddev'})
    )
    avg_stride_freq = (
        gps_df.groupby(group_keys)['stride_frequency']
        .mean()
        .reset_index()
        .rename(columns={'stride_frequency': 'avg_stride_freq'})
    )
    progress_rate = (
        gps_df.groupby(group_keys)['progress']
        .mean()
        .reset_index()
        .rename(columns={'progress': 'progress_rate'})
    )
    avg_sectional_time = (
        sectionals_df.groupby(group_keys)['sectional_time']
        .mean()
        .reset_index()
        .rename(columns={'sectional_time': 'avg_sectional_time'})
    )
    final_sprint_speed = (
        gps_df[gps_df['progress'] <= 200.0]
        .groupby(group_keys)['speed']
        .mean()
        .reset_index()
        .rename(columns={'speed': 'final_sprint_speed'})
    )
    total_distance = (
        sectionals_df.groupby(group_keys)['distance_ran']
        .sum()
        .reset_index()
        .rename(columns={'distance_ran': 'total_distance'})
    )
    sectionals_df['stride_efficiency_temp'] = sectionals_df['distance_ran'] / sectionals_df['number_of_strides']
    stride_efficiency = (
        sectionals_df.groupby(group_keys)['stride_efficiency_temp']
        .mean()
        .reset_index()
        .rename(columns={'stride_efficiency_temp': 'stride_efficiency'})
    )
    avg_stride_freq_200m = (
        gps_df[gps_df['progress'] <= 200.0]
        .groupby(group_keys)['stride_frequency']
        .mean()
        .reset_index()
        .rename(columns={'stride_frequency': 'avg_stride_freq_200m'})
    )

    # Merge basic aggregated features.
    features_df = avg_speed.merge(speed_stddev, on=group_keys, how='outer')
    features_df = features_df.merge(avg_stride_freq, on=group_keys, how='outer')
    features_df = features_df.merge(progress_rate, on=group_keys, how='outer')
    features_df = features_df.merge(avg_sectional_time, on=group_keys, how='outer')
    features_df = features_df.merge(final_sprint_speed, on=group_keys, how='outer')
    features_df = features_df.merge(total_distance, on=group_keys, how='outer')
    features_df = features_df.merge(stride_efficiency, on=group_keys, how='outer')
    features_df = features_df.merge(avg_stride_freq_200m, on=group_keys, how='outer')
    features_df = features_df.merge(time_to_200, on=group_keys, how='outer')
    features_df = features_df.merge(time_to_finish, on=group_keys, how='outer')
    features_df = features_df.dropna(subset=['time_to_finish'])  # Ensure target can be computed

    # ---------------------------
    # Add beaten_lengths to features.
    # ---------------------------
    # Assume beaten_lengths is equivalent to the minimum distance_back at the finish.
    beaten_df = (sectionals_df[sectionals_df['gate_name'].str.upper() == 'FINISH']
                 .groupby(group_keys)['distance_back']
                 .min()
                 .reset_index()
                 .rename(columns={'distance_back': 'beaten_lengths'}))
    features_df = features_df.merge(beaten_df, on=group_keys, how='left')
    
    # ---------------------------
    # Compute Additional Metrics.
    # ---------------------------
    # Compute TTR (Time To Reach) for multiple speed thresholds.
    ttr_df = compute_ttr_metrics(gps_df, group_keys, thresholds=[5, 10, 20, 25, 30])
    features_df = features_df.merge(ttr_df, on=group_keys, how='left')
    
    # Compute ROS (Run Out Speed): average speed for 2 seconds after finish.
    ros_df = compute_ros(gps_df, group_keys, window=2)
    features_df = features_df.merge(ros_df, on=group_keys, how='left')
    
    # Compute VP (Velocity Peak) and PSF (Stride Frequency at Peak Speed).
    vp_psf_df = compute_vp_psf(gps_df, group_keys)
    features_df = features_df.merge(vp_psf_df, on=group_keys, how='left')
    
    # Compute Best Position (BP) from sectionals data (minimum distance_back).
    bp_df = compute_best_position(sectionals_df, group_keys)
    features_df = features_df.merge(bp_df, on=group_keys, how='left')
    
    # ---------------------------
    # Compute the track-specific GCSF target.
    # ---------------------------
    # The target is computed from par time and effective finishing time.
    # If beaten_lengths is available, we use:
    #   effective_time = time_to_finish + ALPHA * beaten_lengths.
    # Then, speed_diff = part_time - effective_time, scaled to [40, 120].
    final_df = compute_gcsf_target(features_df, group_keys, ALPHA=0.1)
    final_df['label'] = final_df['gcsf_target']
    
    # *** Preserve the original track identifier for ordering ***
    final_df["course_raw"] = final_df["course_cd"]
    
    # One-hot encode the course_cd column.
    final_df = pd.get_dummies(final_df, columns=["course_cd"], prefix="course_cd")
    
    # Define the list of numeric features.
    numeric_features = [
        "avg_speed", "speed_stddev", "avg_stride_freq", "progress_rate",
        "avg_sectional_time", "final_sprint_speed", "total_distance",
        "stride_efficiency", "avg_stride_freq_200m", "time_to_200", "time_to_finish",
        "beaten_lengths",                      # Newly added feature.
        # Additional metrics:
        "TTR_5", "TTR_10", "TTR_20", "TTR_25", "TTR_30",  # Time To Reach thresholds.
        "ROS",      # Run Out Speed.
        "VP",       # Velocity Peak.
        "PSF",      # Stride Frequency at Peak Speed.
        "BP"        # Best in-run Position (minimum distance_back).
    ]
    # Fill missing numeric values with zero.
    final_df[numeric_features] = final_df[numeric_features].fillna(0.0)
    onehot_cols = [col for col in final_df.columns if col.startswith("course_cd_")]
    feature_cols = numeric_features + onehot_cols
    
    X = final_df[feature_cols]
    y = final_df["label"]
    
    valid_idx = y.notna()
    X = X.loc[valid_idx]
    y = y.loc[valid_idx]
    final_df = final_df.loc[valid_idx]
    
    return X, y, final_df

# ------------------------------------------------------------------------------
# Model Training Function using scikit-learn
# ------------------------------------------------------------------------------
def train_gcsf_model_with_tuning(X, y, use_scaler="minmax"):
    if use_scaler == "minmax":
        scaler = MinMaxScaler()    
    elif use_scaler == "standard":
        scaler = StandardScaler()
    else:
        scaler = None

    X_scaled = scaler.fit_transform(X) if scaler else X.values
    model = GradientBoostingRegressor()
    model.fit(X_scaled, y)
    preds = model.predict(X_scaled)
    mae = mean_absolute_error(y, preds)
    print("Mean Absolute Error:", mae)
    preds_rounded = np.round(preds)
    return model, preds_rounded

# ------------------------------------------------------------------------------
# Example Main Function
# ------------------------------------------------------------------------------
if __name__ == '__main__':
    import configparser, os, sys, logging
    from psycopg2 import pool
    from pyspark.sql import SparkSession

    # Read configuration and create connection pool.
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../config.ini')
    config.read(config_path)
    
    try:
        db_pool = pool.SimpleConnectionPool(
            1, 20,
            user=config['database']['user'],
            password=config['database'].get('password'),
            host=config['database']['host'],
            port=config['database']['port'],
            database=config['database']['dbname']
        )
    except Exception as e:
        logging.error("Error creating connection pool: %s", e)
        sys.exit(1)
    
    conn = db_pool.getconn()
    try:
        X, y, final_df = gps_composite_speed_figure(conn)
        model, final_predictions = train_gcsf_model_with_tuning(X, y, use_scaler="minmax")
    finally:
        db_pool.putconn(conn)
    
    # Add predictions as a new column.
    final_df["gcsf_speed_figure"] = final_predictions
    
    # Convert to a Spark DataFrame.
    spark, jdbc_url, jdbc_properties, parquet_dir, _ = initialize_environment()
    
    # Convert the final Pandas DataFrame to a Spark DataFrame
    spark_pred_df = spark.createDataFrame(final_df)

    # Save the Spark DataFrame to Parquet using the save_parquet function.
    save_parquet(spark, spark_pred_df, "gcsf_features", parquet_dir)
    # Use the preserved course_raw for ordering.
    spark_pred_df.orderBy("course_raw", "race_date", "race_number", "beaten_lengths").select(
        "course_raw", "race_date", "race_number", "horse_id", "beaten_lengths", "gcsf_speed_figure"
    ).show(50, truncate=False)
    
    spark_pred_df.orderBy("course_raw", "race_date", "race_number", "gcsf_speed_figure").select(
        "course_raw", "race_date", "race_number", "horse_id", "gcsf_speed_figure"
    ).show(50, truncate=False)