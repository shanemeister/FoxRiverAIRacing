import logging
import datetime
import os
import sys
import configparser
import pandas as pd
import numpy as np
import optuna
from scipy.stats import norm
from catboost import CatBoostRegressor, Pool
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.metrics import roc_auc_score, accuracy_score
from psycopg2 import pool, DatabaseError
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split

from src.data_preprocessing.data_prep1.data_utils import save_parquet, initialize_environment
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql
from src.train_and_predict.fox_query_speed_figure import full_query_df

def drop_races(df):
    
    race_cols = ['course_cd', 'race_date', 'race_number']
    
    # Define key sectional features to check for missing values.
    key_features = ['dist_bk_gate4', 'total_distance_ran', 'running_time']
    
    # Identify races with missing values in any of the key features.
    # This returns a DataFrame with one row per race and a boolean flag.
    races_missing_flag = (
        df.groupby(race_cols)[key_features]
            .apply(lambda grp: grp.isnull().any().any())
            .reset_index(name='has_missing')
    )
    
    # Display how many races have missing data.
    num_races_total = races_missing_flag.shape[0]
    num_races_with_missing = races_missing_flag[races_missing_flag['has_missing']].shape[0]
    print(f"Total races: {num_races_total}")
    print(f"Races with missing key features: {num_races_with_missing}")

    # Merge back to the original DataFrame to filter out races with missing data.
    # Only keep races where has_missing is False.
    # races_to_keep = races_missing_flag[races_missing_flag['has_missing'] == False][race_cols]
    # df_complete = df.merge(races_to_keep, on=race_cols, how='inner')
    
    # Report how many horse entries (rows) are lost.
    # num_horses_before = len(df)
    # num_horses_after = len(df_complete)
    # num_horses_lost = num_horses_before - num_horses_after

    # print(f"Number of horse entries before dropping: {num_horses_before}")
    # print(f"Number of horse entries after dropping races with missing data: {num_horses_after}")
    # print(f"Number of horse entries lost: {num_horses_lost}")
    df = df.copy()
    return df

def fill_missing_gps_cols(df, key_cols):
    """
    Fills any NaNs in the specified key columns with 
    the column-wide mean. Also flags 'gps_present' = 0 if it was missing.
    """
    # For each key column:
    for col in key_cols:
        col_mean = df[col].mean(skipna=True)
        missing_mask = df[col].isna()
        # Fill with mean
        df.loc[missing_mask, col] = col_mean
        # Also set gps_present=0 if that column was missing
        # But do it only if you want to systematically mark that row "incomplete."
        # For example, if gps_present is already mostly 1, then:
        df.loc[missing_mask, "gps_present"] = 0
    
    return df

def class_muliplier(df):

    # Convert the class_rating column to numeric
    df['class_rating_numeric'] = pd.to_numeric(df['class_rating'], errors='coerce')

    # Print summary statistics to see the range and median
    class_min = df['class_rating_numeric'].min()
    class_max = df['class_rating_numeric'].max()
    class_median = df['class_rating_numeric'].median()

    print("Class rating - min:", class_min, "max:", class_max, "median:", class_median)

    # Create a class multiplier.
    # For example, map the class ratings linearly to a multiplier range of 0.9 to 1.1.
    # You can adjust these values if you want a different impact.
    df['class_multiplier'] = 0.95 + (df['class_rating_numeric'] - class_min) * (1. - .95) / (class_max - class_min)

    # Optionally, you might also compute a baseline offset:
    df['class_offset'] = (df['class_rating_numeric'] - class_median)/2

    print(df[['class_rating', 'class_rating_numeric', 'class_multiplier', 'class_offset']].head(5))
    df = df.copy()
    return df

def cal_raw_performance_score(df, alpha):
    # Assume df already includes:
    # 'distance_meters' (official distance),
    # 'running_time' (the horse's running time),
    # 'total_distance_ran' (actual distance run),
    # 'class_rating_numeric' (the numeric class rating),
    # and that you've computed class_min and class_max from your data.

    # For clarity, rename official_distance to distance_meters.
    df['official_distance'] = df['distance_meters']

    # 1. Base Speed (in meters per second)
    # Higher base_speed means faster.
    df['base_speed'] = df['official_distance'] / df['running_time']

    # 2. Running-Wide Adjustment
    # If the horse ran extra distance, the factor is < 1.
    df['wide_factor'] = df['official_distance'] / df['total_distance_ran']
    
    # 4. Compute Par Time per race group.
    group_cols = ['course_cd', 'race_date', 'race_number', 'trk_cond', 'official_distance']
    df['par_time'] = df.groupby(group_cols)['running_time'].transform('mean')
    
    # 5. Compute Par Time Differential Ratio.
    # Positive if the horse ran faster than the race average.
        
    df['par_diff_ratio'] = (df['par_time'] - df['running_time']) / df['par_time']


    # 7. Combine all components into a raw performance score.
    df['raw_performance_score'] = ( df['base_speed'] *
                                        df['wide_factor'] *
                                        df['class_multiplier'] *
                                        (1 + alpha * df['par_diff_ratio']) )
    
    # Penalty applied for dist_bk_gate4
    # Adjust of needed to make the penalty more or less severe.
    df['dist_penalty'] = np.minimum(0.25 * df['dist_bk_gate4'], 10)
    df['raw_performance_score'] = df['raw_performance_score'] - df['dist_penalty']
    df = df.copy()
    return df

def compute_global_normalized_speed_figure(df):
    # Compute global mean and standard deviation for normalization
    global_mean = df['raw_performance_score'].mean()
    global_std = df['raw_performance_score'].std()

    # Standardize the raw performance score (Z-score normalization)
    
    df['standardized_score'] = (df['raw_performance_score'] - global_mean) / global_std
    df = df.copy()
    return df
    
def apply_nonlinear_tranformation(df):
    """
    Since we want fewer extreme scores (approaching 40 and 140) and most horses clustering between 60 and 120, we apply a nonlinear transformation to the standardized score.

    Sigmoid Transformation (Preferred for Gaussian-like distribution):
    """
    # tanh transformation
    # tanh scales values between -1 and 1, preserving rank order while reducing extreme outliers.
    df['normalized_score'] = np.tanh(df['standardized_score'])
    df = df.copy()
    return df

def scale_from_40_140(df):
    """
     Scale to the 40-140 Range: Direct Gaussian Mapping
        •	Convert standardized scores to a percentile using scipy.stats.norm.cdf()
        •	Directly map percentiles to the 40-140 range.
    """
    # Convert standardized scores to percentiles
    df['percentile_score'] = norm.cdf(df['standardized_score'])

    # Scale percentiles to the 40-140 range
    df['global_speed_score'] = 41 + (df['percentile_score'] * 97)
    df = df.copy()
    return df

def computer_accuracy_with_dq_horses(df):
        # Define the function to compute match percentage per race.
    def compute_match_percentage_adjusted(df, race_cols, official_col='official_fin', speed_col='global_speed_score', id_col='horse_id'):
        race_stats = []
        # Group by each race.
        for race_id, group in df.groupby(race_cols):
            # Official finish order: sort by official_fin (ascending, lower is better)
            official_order = group.sort_values(by=official_col, ascending=True)[id_col].tolist()
            # Order by speed score: sort by global_speed_score (descending, higher is better)
            speed_order = group.sort_values(by=speed_col, ascending=False)[id_col].tolist()
            matches = sum(o == s for o, s in zip(official_order, speed_order))
            total = len(official_order)
            percent_match = (matches / total) * 100
            # Create a dict with the race keys and computed statistics.
            race_stats.append({**dict(zip(race_cols, race_id)), 'match_percentage': percent_match, 'num_horses': total})
        return pd.DataFrame(race_stats).sort_values(by=race_cols)

    # Define race-identifying columns.
    race_cols = ['course_cd', 'race_date', 'race_number']

    # Re-create the match_df from full_df.
    match_df = compute_match_percentage_adjusted(df, race_cols, speed_col='global_speed_score')

    # Identify races with at least one DQ horse (horse_name starting with "dq", case-insensitive).
    dq_mask = df['horse_name'].str.lower().str.startswith('dq')
    dq_races = df[dq_mask].groupby(race_cols).size().reset_index(name='dq_count')
    print("Number of races that include at least one DQ horse:", dq_races.shape[0])

    # Merge the DQ information with match_df.
    match_with_dq = match_df.merge(dq_races, on=race_cols, how='left')
    match_with_dq['dq_count'] = match_with_dq['dq_count'].fillna(0)

    # Split into races with DQ horses and races without.
    dq_races_only = match_with_dq[match_with_dq['dq_count'] > 0]
    non_dq_races = match_with_dq[match_with_dq['dq_count'] == 0]

    print("\nMatch percentage stats for races with DQ horses:")
    print(dq_races_only['match_percentage'].describe())

    print("\nMatch percentage stats for races without DQ horses:")
    print(non_dq_races['match_percentage'].describe())

    # Optionally, list a few races with DQ horses that have less than 100% match.
    print("\nRaces with DQ horses and less than 100% match:")
    print(dq_races_only[dq_races_only['match_percentage'] < 100].sort_values('match_percentage').head(10))

def create_custom_speed_figure(df):
    """
    Load Parquet file used horse_embedding and custom_speed_figure
    """
       
    df = df.toPandas()

    # Create race_id for grouping
    df["race_number"] = df["race_number"].astype(float)
    
    df["race_id"] = (
        df["course_cd"].astype(str) + "_" +
        df["race_date"].astype(str) + "_" +
        df["race_number"].astype(str)
    )
    
    full_df = drop_races(df)

    full_df['race_date'] = pd.to_datetime(full_df['race_date'])
    
    full_df = class_muliplier(full_df)
    
    # 3. Set a tuning parameter for the effect of par differential.
    alpha = 50  # Adjust this parameter as needed.

    full_df = cal_raw_performance_score(full_df, alpha)
    
    full_df = compute_global_normalized_speed_figure(full_df)
    
    full_df = apply_nonlinear_tranformation(full_df)
    
    full_df = scale_from_40_140(full_df)
    
    # Ensure the horse_name column is available. 
    # Filter rows where horse_name starts with "dq" (case-insensitive)
    dq_horses = full_df[full_df['horse_name'].str.lower().str.startswith('dq')]

    # Print the number of such entries
    print("Number of horses with names starting with 'dq':", dq_horses.shape[0])

    # Optionally, display a few rows for inspection
    print(dq_horses[['horse_name', 'course_cd', 'race_date', 'race_number', 'official_fin']].head(10))
  
    computer_accuracy_with_dq_horses(full_df)
    full_df = full_df.copy()
    return full_df