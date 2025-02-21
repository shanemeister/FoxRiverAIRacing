import logging
import datetime
import os
import sys
import configparser
import pandas as pd
import numpy as np
import optuna
from scipy.stats import norm

def drop_races(df):
    """
    Renamed logic to:
      - Mark entire races that have ANY missing in
        [dist_bk_gate4, total_distance_ran, running_time] => race_missing_flag=1
      - For horses that are missing any of those columns:
          * If official_fin==1 => fill with 0
          * else fill with the race-level mean
          * Also set missing_gps_flag=1, gps_present=0
      - DO NOT actually drop anything.
    """

    race_cols = ['course_cd', 'race_date', 'race_number']
    key_features = ['dist_bk_gate4', 'total_distance_ran', 'running_time']

    # 1) Add columns to hold flags
    if 'race_missing_flag' not in df.columns:
        df['race_missing_flag'] = 0
    if 'missing_gps_flag' not in df.columns:
        df['missing_gps_flag'] = 0
    # Ensure gps_present exists
    if 'gps_present' not in df.columns:
        df['gps_present'] = 1  # default to 1

    # 2) Create a grouping “race_id” so we can handle each race
    df['race_id'] = df[race_cols].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)

    # 3) Find which races have ANY missing in key_features
    #    We'll create a helper that checks if group has missing
    def group_has_missing(subdf):
        return subdf[key_features].isna().any().any()  # True if any col is missing

    missing_flags = df.groupby('race_id').apply(group_has_missing).rename('has_missing_in_race')
    missing_flags_df = missing_flags.reset_index()  # so we can merge

    # 4) Merge so each row knows if its race had missing
    df = df.merge(missing_flags_df, on='race_id', how='left')

    # If race has_missing_in_race==True => mark race_missing_flag=1
    df.loc[df['has_missing_in_race'] == True, 'race_missing_flag'] = 1

    # 5) Now fill row-level missing in key features:
    #    * official_fin==1 => fill with 0
    #    * else fill with race-level mean
    #    * also set missing_gps_flag=1, gps_present=0 for those rows

    def fill_missing_for_race(subdf):
        for col in key_features:
            # compute race-level mean
            mean_val = subdf[col].mean(skipna=True)
            # find which rows are missing
            is_missing = subdf[col].isna()
            # fill with 0 if official_fin==1, else fill with mean
            subdf.loc[is_missing & (subdf['official_fin'] == 1), col] = 0
            subdf.loc[is_missing & (subdf['official_fin'] != 1), col] = mean_val

            # for those missing rows, mark them
            subdf.loc[is_missing, 'missing_gps_flag'] = 1
            subdf.loc[is_missing, 'gps_present'] = 0
        return subdf

    df = df.groupby('race_id', group_keys=False).apply(fill_missing_for_race)

    # 6) Clean up
    df.drop(columns=['race_id','has_missing_in_race'], inplace=True, errors='ignore')
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

    # Convert race_number to float if needed
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
    print("Number of horses with names starting with 'dq':", dq_horses.shape[0])
    print(dq_horses[['horse_name', 'course_cd', 'race_date', 'race_number', 'official_fin']].head(10))
  
    computer_accuracy_with_dq_horses(full_df)
    full_df = full_df.copy()
    return full_df

# ##############################################################
# import pandas as pd
# import numpy as np
# from scipy.stats import norm

# def mark_and_fill_missing(df):
#     """
#     1) Identify any races that have missing TPD in [dist_bk_gate4, total_distance_ran, running_time].
#        - Mark those races with race_missing_flag=1
#     2) For each row (horse) in those races:
#        - If row is missing that column:
#          => If official_fin=1, fill with 0
#          => else fill with that race’s mean for that column
#          => Mark missing_gps_flag=1, gps_present=0
#     3) If an entire race is 100% missing for a TPD column, 
#        the group-mean is NaN, so those remain NaN 
#        => we'll fix them in a final leftover-impute step.
#     """
#     df = df.copy()
#     TPD_COLS  = ["dist_bk_gate4","total_distance_ran","running_time"]
#     RACE_KEYS = ["course_cd","race_date","race_number"]

#     # Create new columns in a single pass
#     new_cols = {
#         "race_missing_flag": 0,
#         "missing_gps_flag": 0
#     }
#     if "gps_present" not in df.columns:
#         new_cols["gps_present"] = 1

#     # Build a 'race_id' for grouping
#     race_id_series = df[RACE_KEYS].astype(str).agg("_".join, axis=1)
#     new_cols["race_id"] = race_id_series

#     df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

#     # Which races have any missing TPD?
#     def group_has_missing(g):
#         return g[TPD_COLS].isna().any().any()
#     missing_flags = df.groupby("race_id").apply(group_has_missing).rename("has_race_miss").reset_index()
#     df = df.merge(missing_flags, on="race_id", how="left")

#     df.loc[df["has_race_miss"], "race_missing_flag"] = 1

#     # Fill row-level missing by official_fin or group mean
#     def fill_race_na(g):
#         for col in TPD_COLS:
#             mean_val = g[col].mean(skipna=True)  # may be NaN if all are missing
#             mask_na  = g[col].isna()

#             # If official_fin=1 => fill=0
#             g.loc[mask_na & (g["official_fin"] == 1), col] = 0
#             # else fill=mean
#             g.loc[mask_na & (g["official_fin"] != 1), col] = mean_val

#             # Mark missing
#             g.loc[mask_na, "missing_gps_flag"] = 1
#             g.loc[mask_na, "gps_present"]      = 0
#         return g

#     df = df.groupby("race_id", group_keys=False).apply(fill_race_na)
#     df.drop(columns=["race_id","has_race_miss"], inplace=True, errors="ignore")
#     return df


# def class_multiplier(df):
#     df = df.copy()
#     if "class_rating" not in df.columns:
#         df["class_multiplier"] = 1.0
#         df["class_offset"]     = 0.0
#         return df

#     df["class_rating_numeric"] = pd.to_numeric(df["class_rating"], errors="coerce")
#     cmin = df["class_rating_numeric"].min()
#     cmax = df["class_rating_numeric"].max()
#     cmed = df["class_rating_numeric"].median()
#     # Example range: [0.95..1.0]
#     df["class_multiplier"] = 0.95 + (df["class_rating_numeric"] - cmin)*(1.0 - 0.95)/(cmax - cmin)
#     df["class_offset"] = (df["class_rating_numeric"] - cmed)/2
#     return df


# def calc_raw_perf_score(df, alpha=50.0):
#     """
#     1) base_speed = distance_meters / running_time
#     2) wide_factor = distance_meters / total_distance_ran
#     3) par_time = mean(running_time) by race
#     4) raw_performance_score = base_speed * wide_factor * class_multiplier * (1 + alpha*par_diff_ratio)
#     5) dist_penalty = min(0.25*dist_bk_gate4, 10)
#     => raw_perf - dist_penalty
#     """
#     df = df.copy()
#     df["official_distance"] = df.get("distance_meters", 0.0)
#     df["base_speed"] = df["official_distance"] / df["running_time"]
#     df["wide_factor"] = df["official_distance"] / df["total_distance_ran"]

#     grp_cols = ["course_cd","race_date","race_number","trk_cond","official_distance"]
#     df["par_time"] = df.groupby(grp_cols)["running_time"].transform("mean")
#     df["par_diff_ratio"] = (df["par_time"] - df["running_time"])/df["par_time"]

#     df["raw_performance_score"] = (
#         df["base_speed"] *
#         df["wide_factor"] *
#         df.get("class_multiplier", 1.0) *
#         (1.0 + alpha*df["par_diff_ratio"])
#     )
#     df["dist_penalty"] = np.minimum(0.25 * df["dist_bk_gate4"], 10.0)
#     df["raw_performance_score"] -= df["dist_penalty"]
#     return df



# def compute_global_speed_figure(df):
#     """
#     1) standardize => tanh => percentile => scale [40..140]
#     """
#     df = df.copy()
#     mean_rps = df["raw_performance_score"].mean()
#     std_rps  = df["raw_performance_score"].std()

#     df["standardized_score"] = (df["raw_performance_score"] - mean_rps)/std_rps
#     df["normalized_score"]   = np.tanh(df["standardized_score"])
#     df["percentile_score"]   = norm.cdf(df["standardized_score"])  # [0..1]
#     # map => [40..140]
#     df["global_speed_score"] = 40.0 + 100.0*df["percentile_score"]
#     return df


# def final_impute_leftovers(df):
#     """
#     If any TPD or newly-created columns remain NaN (e.g. entire race was missing),
#     fill with global mean. Mark missing_gps_flag=1 if not already.
#     """
#     leftover_cols = [
#         # original TPD columns
#         "dist_bk_gate4","total_distance_ran","running_time",
#         # new numeric columns
#         "base_speed","wide_factor","par_time","par_diff_ratio",
#         "raw_performance_score","dist_penalty","standardized_score",
#         "normalized_score","percentile_score","global_speed_score"
#     ]
#     df = df.copy()
#     for col in leftover_cols:
#         if col in df.columns:
#             mask_na  = df[col].isna()
#             if mask_na.any():
#                 fill_val = df[col].mean(skipna=True)
#                 df.loc[mask_na, col] = fill_val
#                 if "missing_gps_flag" in df.columns:
#                     df.loc[mask_na, "missing_gps_flag"] = 1
#                 if "gps_present" in df.columns:
#                     df.loc[mask_na, "gps_present"] = 0
#     return df


# def check_accuracy_sample(df):
#     """
#     Compute the race-level match percentage between the official finish order
#     (sorted by official_fin ascending) and the model’s ranking (sorted by global_speed_score descending).
#     Returns a DataFrame with a unique column 'rm_pct' for race match percentage.
#     """
#     race_cols = ["course_cd", "race_date", "race_number"]

#     def match_count(g):
#         off_order = g.sort_values("official_fin")["horse_id"].tolist()
#         spd_order = g.sort_values("global_speed_score", ascending=False)["horse_id"].tolist()
#         matches = sum(o == s for o, s in zip(off_order, spd_order))
#         return 100.0 * matches / len(g)
    
#     input("Press Enter to continue...1 ")
    
#     # Ensure 'rm_pct' column does not already exist
#     if 'rm_pct' in df.columns:
#         df = df.drop(columns=['rm_pct'])
    
#     # Use a unique column name ('rm_pct') so it doesn’t conflict with any existing name.
#     stats = df.groupby(race_cols).apply(match_count).reset_index(name="rm_pct")
    
#     input("Press Enter to continue...2 ")
    
#     print("Match % distribution:\n", stats["rm_pct"].describe())
    
#     input("Press Enter to continue...3 ")
    
#     return stats

# def create_custom_speed_figure(df_input):
#     """
#     Consolidated pipeline:

#     1) Convert Spark => Pandas if needed
#     2) Mark + fill TPD missing => [dist_bk_gate4, total_distance_ran, running_time]
#        - no dropping races
#        - official_fin=1 => fill=0, else fill=race-mean
#        => also sets race_missing_flag + missing_gps_flag
#     3) class_multiplier
#     4) calc_raw_perf_score => alpha=50
#     5) compute_global_speed_figure => standardize + tanh + [40..140]
#     6) final_impute_leftovers => fill any leftover NaNs in new columns
#     7) optional accuracy check or DQ horse checks
#     8) Return final DataFrame
#     """
#     if not isinstance(df_input, pd.DataFrame):
#         df = df_input.toPandas()
#     else:
#         df = df_input.copy()

#     df = mark_and_fill_missing(df)         # Step 2
#     df = class_multiplier(df)              # Step 3
#     df = calc_raw_perf_score(df, alpha=50) # Step 4
#     df = compute_global_speed_figure(df)   # Step 5
#     df = final_impute_leftovers(df)        # Step 6

#     check_accuracy_sample(df)             # Step 7 (optional for debug)

#     return df