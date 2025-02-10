import pandas as pd
import numpy as np
import warnings
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error

# Suppress the SQLAlchemy warning for non-SQLAlchemy connections.
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

# ------------------------------------------------------------------------------
# Helper: Clamp outliers in a Pandas DataFrame
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
# Compute GCSF target tied to track (course_cd)
# ------------------------------------------------------------------------------
def compute_gcsf_target(df, group_keys):
    """
    Compute a continuous GCSF target for each race.
    The idea is:
      - For each track (course_cd), determine a par time (if not provided, use the median of winners).
      - Compute speed_diff = part_time - time_to_finish.
      - Then scale the speed_diff for each track to the range [40, 120].
    """
    # If 'part_time' is not available, compute it per track.
    if 'part_time' not in df.columns:
        race_winners = df.groupby(["course_cd", "race_date", "race_number"])['time_to_finish'].min().reset_index()
        par_times = race_winners.groupby("course_cd")['time_to_finish'].median().reset_index().rename(
            columns={'time_to_finish': 'part_time'})
        df = df.merge(par_times, on="course_cd", how='left')

    df['speed_diff'] = df['part_time'] - df['time_to_finish']
    df = df.dropna(subset=['speed_diff'])
    
    def scale_group(group):
        min_diff = group['speed_diff'].min()
        max_diff = group['speed_diff'].max()
        if max_diff == min_diff:
            group['gcsf_target'] = 120.0
        else:
            group['gcsf_target'] = 40 + (group['speed_diff'] - min_diff) / (max_diff - min_diff) * 80
        return group

    df = df.groupby("course_cd", group_keys=False).apply(scale_group)
    return df

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

    # Compute time-based features.
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

    # Merge features.
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
    features_df = features_df.dropna(subset=['time_to_finish'])  # ensure target can be computed

    # Compute the track-specific GCSF target.
    final_df = compute_gcsf_target(features_df, group_keys)
    final_df['label'] = final_df['gcsf_target']

    # *** Preserve the original track identifier for ordering ***
    final_df["course_raw"] = final_df["course_cd"]

    # One-hot encode the course_cd column.
    final_df = pd.get_dummies(final_df, columns=["course_cd"], prefix="course_cd")

    numeric_features = [
        "avg_speed", "speed_stddev", "avg_stride_freq", "progress_rate",
        "avg_sectional_time", "final_sprint_speed", "total_distance",
        "stride_efficiency", "avg_stride_freq_200m", "time_to_200", "time_to_finish"
    ]
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
    spark = SparkSession.builder.getOrCreate()
    spark_pred_df = spark.createDataFrame(final_df)

    # Use the preserved course_raw for ordering.
    spark_pred_df.orderBy("course_raw", "race_date", "race_number", "beaten_lengths").select(
        "course_raw", "race_date", "race_number", "horse_id", "beaten_lengths", "gcsf_speed_figure"
    ).show(50, truncate=False)

    spark_pred_df.orderBy("course_raw", "race_date", "race_number", "gcsf_speed_figure").select(
        "course_raw", "race_date", "race_number", "horse_id", "gcsf_speed_figure"
    ).show(50, truncate=False)