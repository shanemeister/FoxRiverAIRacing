import logging
import pandas as pd
import numpy as np
from catboost import CatBoostRanker, Pool
from datetime import datetime
import json

def main():
    # 1) Load your horse_embeddings Parquet into a Pandas DataFrame
    horse_embedding_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/horse_embedding_data-20250312_1948.parquet"
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
    def assign_labels(df, alpha=0.8):
        def _exp_label(fin):
            return alpha ** (fin - 1)
        df["relevance"] = df["official_fin"].apply(_exp_label)
        df["top4_label"] = (df["official_fin"] <= 4).astype(int)
        return df

    df = assign_labels(df, alpha=0.8)
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
    final_feature_cols = [
        "global_speed_score_iq", "previous_class", "class_rating", "previous_distance", 
        "off_finish_last_race", "prev_speed_rating", "purse", "claimprice", "power", 
        "avgspd", "avg_spd_sd","ave_cl_sd", "hi_spd_sd", "pstyerl", "horse_itm_percentage",
        "total_races_5", "avg_dist_bk_gate1_5", "avg_dist_bk_gate2_5", "avg_dist_bk_gate3_5", 
        "avg_dist_bk_gate4_5","avg_speed_fullrace_5","avg_stride_length_5","avg_strfreq_q1_5",
        "avg_speed_5","avg_fin_5","best_speed","avg_beaten_len_5","prev_speed",
        "avg_strfreq_q2_5", "avg_strfreq_q3_5", "avg_strfreq_q4_5", "speed_improvement", 
        "age_at_race_day", "count_workouts_3","avg_workout_rank_3","weight","days_off", 
        "starts", "race_count","has_gps", "cond_starts","cond_win","cond_place","cond_show",
        "cond_fourth","cond_earnings","all_starts", "all_win", "all_place", "all_show", 
        "all_fourth","all_earnings","morn_odds","net_sentiment", "distance_meters", 
        "jt_itm_percent","jt_win_percent","trainer_win_track","trainer_itm_track",
        "trainer_win_percent","trainer_itm_percent","jock_win_track","jock_win_percent",
        "jock_itm_track","jt_win_track","jt_itm_track","jock_itm_percent",
        "sire_itm_percentage","sire_roi","dam_itm_percentage","dam_roi"
    ]
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
    model_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/catboost/catboost_YetiRank:top=4_NDCG:top=4_20250312_214018.cbm"
    model = CatBoostRanker()
    model.load_model(model_path)

    # 16) Compute feature importances
    importances = model.get_feature_importance(train_pool, type="FeatureImportance")
    imp_df = pd.DataFrame({
        "feature": all_feature_cols_use,
        "importance": importances
    }).sort_values("importance", ascending=False)

    print("\n=== Feature Importances ===")
    print(imp_df.to_string(index=False))

if __name__ == "__main__":
    main()