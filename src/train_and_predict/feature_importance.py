import logging
import pandas as pd
import numpy as np
from catboost import CatBoostRanker, Pool
from datetime import datetime
import json

def main():
    # 1) Load your horse_embeddings Parquet into a Pandas DataFrame
    horse_embedding_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/horse_embedding_data-20250404_1946.parquet"
    df = pd.read_parquet(horse_embedding_path)

    print("\n=== Columns in horse_embeddings DF ===")
    print(df.columns.tolist())

    # 2) Define cat_cols & excluded_cols (same as training)
    cat_cols = [
        "course_cd","trk_cond","sex","equip","surface","med",
        "race_type","stk_clm_md","turf_mud_mark","layoff_cat","previous_surface"
    ]

    excluded_cols = [
        "axciskey", "official_fin", "relevance", "top4_label",
        "post_time", "horse_id", "horse_name", "race_date", "race_number",
        "saddle_cloth_number", "race_id", "date_of_birth","time_behind","track_name",
        "data_flag","group_id"
    ]

    # 3) Create a function to build embed_cols
    def build_embed_cols(pdf):
        """
        1) Create a list of combined columns for embedding (start with 'combined_').
        2) Return the list of combined columns sorted by suffix integer.
        """
        all_combined = [c for c in pdf.columns if c.startswith("combined_")]
        sorted_combined = sorted(all_combined, key=lambda x: int(x.split("_")[1]))
        return sorted_combined

    # 4) Add a 'relevance' label from official_fin (like in training)
    def assign_piecewise_log_labels(df):
        """
        If official_fin <= 4, assign them to a high relevance band (with small differences).
        Otherwise, use a log-based formula that drops more sharply.
        """
        def _relevance(fin):
            if fin == 1:
                return 100 #40  # Could be 40 for 1st
            elif fin == 2:
                return 75 # 38  # Slightly lower than 1st, but still high
            else:
                alpha = 20.0
                beta  = 4.0
                return alpha / np.log(beta + fin)
        
        df["relevance"] = df["official_fin"].apply(_relevance)
        return df


    df = assign_piecewise_log_labels(df)
    # remove any row that has a missing official_fin or missing relevance
    df = df.dropna(subset=["relevance"])
    # or if official_fin is missing, do:
    # df = df.dropna(subset=["official_fin"])
    # 5) Convert race_date to datetime, create group_id if needed
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df["group_id"] = (
        df["course_cd"].astype(str)
        + "_"
        + df["race_date"].astype(str)
        + "_"
        + df["race_number"].astype(str)
    ).astype("category").cat.codes

    # 6) Convert selected date columns to numeric, if you used that in training
    datetime_cols = ["first_race_date_5","most_recent_race_5","prev_race_date"]
    new_numeric_cols = {}
    for dc in datetime_cols:
        if dc in df.columns:
            df[dc] = pd.to_datetime(df[dc], errors="coerce")
            new_numeric_cols[dc + "_numeric"] = (df[dc] - pd.Timestamp("1970-01-01")).dt.days
    df.drop(columns=datetime_cols, inplace=True, errors="ignore")
    df = pd.concat([df, pd.DataFrame(new_numeric_cols, index=df.index)], axis=1)

    # 7) Hardcode or load your numeric final features
    final_feature_cols = ["sec_score", "sec_dim1", "sec_dim2", "sec_dim3", "sec_dim4", "sec_dim5", "sec_dim6", "sec_dim7", "sec_dim8",
                          "sec_dim9", "sec_dim10", "sec_dim11", "sec_dim12", "sec_dim13", "sec_dim14", "sec_dim15", "sec_dim16",
                          "class_rating", "par_time", "running_time", "total_distance_ran", 
                          "avgtime_gate1", "avgtime_gate2", "avgtime_gate3", "avgtime_gate4", 
                          "dist_bk_gate1", "dist_bk_gate2", "dist_bk_gate3", "dist_bk_gate4", 
                          "speed_q1", "speed_q2", "speed_q3", "speed_q4", "speed_var", "avg_speed_fullrace", 
                          "accel_q1", "accel_q2", "accel_q3", "accel_q4", "avg_acceleration", "max_acceleration", 
                          "jerk_q1", "jerk_q2", "jerk_q3", "jerk_q4", "avg_jerk", "max_jerk", 
                          "dist_q1", "dist_q2", "dist_q3", "dist_q4", "total_dist_covered", 
                          "strfreq_q1", "strfreq_q2", "strfreq_q3", "strfreq_q4", "avg_stride_length", 
                          "net_progress_gain", "prev_speed_rating", "previous_class", "weight",
                          "claimprice", "previous_distance", "prev_official_fin",
                          "power","avgspd", "starts", "avg_spd_sd", "ave_cl_sd", "hi_spd_sd", "pstyerl",
                          "purse", "distance_meters", "morn_odds", "jock_win_percent",
                          "jock_itm_percent", "trainer_win_percent", "trainer_itm_percent", "jt_win_percent",
                          "jt_itm_percent", "jock_win_track", "jock_itm_track", "trainer_win_track", "trainer_itm_track",
                          "jt_win_track", "jt_itm_track", "sire_itm_percentage", "sire_roi", "dam_itm_percentage",
                          "dam_roi", "all_starts", "all_win", "all_place", "all_show", "all_fourth", "all_earnings",
                          "horse_itm_percentage", "cond_starts", "cond_win", "cond_place", "cond_show", "cond_fourth",
                          "cond_earnings", "net_sentiment", "total_races_5", "avg_fin_5", "avg_speed_5", "best_speed",
                          "avg_beaten_len_5", "avg_dist_bk_gate1_5",
                          "avg_dist_bk_gate2_5", "avg_dist_bk_gate3_5", "avg_dist_bk_gate4_5", "avg_speed_fullrace_5",
                          "avg_stride_length_5", "avg_strfreq_q1_5", "avg_strfreq_q2_5", "avg_strfreq_q3_5", "avg_strfreq_q4_5",
                          "prev_speed", "speed_improvement", "days_off", "avg_workout_rank_3",
                          "count_workouts_3", "age_at_race_day", "class_offset", "class_multiplier",
                          "official_distance", "base_speed", "dist_penalty", "horse_mean_rps", "horse_std_rps",
                          "global_speed_score_iq", "race_count_agg", "race_avg_speed_agg", "race_std_speed_agg",
                          "race_avg_relevance_agg", "race_std_relevance_agg", "race_class_count_agg", "race_class_avg_speed_agg",
                          "race_class_min_speed_agg", "race_class_max_speed_agg", "post_position", "avg_purse_val"]
    
    # 8) Build embed_cols
    embed_cols = build_embed_cols(df)

    # 9) Now combine them => all_feature_cols
    #    plus the cat_cols, except we skip any that appear in excluded_cols
    cat_cols_filtered = [c for c in cat_cols if c not in excluded_cols]
    # Also filter final_feature_cols to drop any that appear in excluded_cols:
    final_feature_cols_filtered = [f for f in final_feature_cols if f not in excluded_cols]
    # embed_cols likely do not appear in excluded, but let's be safe:
    embed_cols_filtered = [e for e in embed_cols if e not in excluded_cols]

    all_feature_cols_use = final_feature_cols_filtered + embed_cols_filtered + cat_cols_filtered

    print("Final feature cols used:", all_feature_cols_use)

    # 10) Convert cat_cols to category dtype if present
    for c in cat_cols_filtered:
        if c in df.columns:
            df[c] = df[c].astype("category")

    # 11) Figure out cat features indices
    cat_features_indices = []
    for catf in cat_cols_filtered:
        if catf in all_feature_cols_use:
            idx = all_feature_cols_use.index(catf)
            cat_features_indices.append(idx)

    print("Categorical feature indices:", cat_features_indices)

    # 12) Sort by group_id
    df = df.sort_values("group_id").reset_index(drop=True)

    # 13) Build X,y,group_id
    X = df[all_feature_cols_use].copy()
    y = df["relevance"].values
    group_id = df["group_id"].values

    # 14) Create the Pool
    train_pool = Pool(
        data=X,
        label=y,
        group_id=group_id,
        cat_features=cat_features_indices
    )

    # 15) Load the existing model
    model_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/catboost/catboost_YetiRankPairwise_NDCG:top=1_20250405_110718.cbm"
    model = CatBoostRanker()
    model.load_model(model_path)

    # 16) Compute feature importances
    importances = model.get_feature_importance(train_pool) #, type="FeatureImportance", prettified=True)
    imp_df = pd.DataFrame({
        "feature": all_feature_cols_use,
        "importance": importances
    }).sort_values("importance", ascending=False)

    print("\n=== Feature Importances ===")
    print(imp_df.to_string(index=False))

if __name__ == "__main__":
    main()