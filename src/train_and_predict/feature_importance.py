import logging
import pandas as pd
import numpy as np
from catboost import CatBoostRanker, Pool
from datetime import datetime

def main():
    # 1) Load your horse_embeddings Parquet into a Pandas DataFrame
    horse_embedding_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/horse_embedding_data-20250222_0031.parquet"
    df = pd.read_parquet(horse_embedding_path)

    # Just to show we have many columns, including "official_fin"...
    print("\n=== Columns in horse_embeddings DF ===")
    print(df.columns.tolist())

    # 2) If we want to create 'relevance' from official_fin, do it explicitly
    #    You said official_fin is in DF. We'll keep it but not pass it as a feature.
    def compute_relevance(fin):
        if fin == 1:
            return 40
        elif fin == 2:
            return 38
        elif fin == 3:
            return 36
        elif fin == 4:
            return 34
        else:
            alpha = 30.0
            beta  = 4.0
            return alpha / np.log(beta + fin)

    # Add a 'relevance' column for training label
    df["relevance"] = df["official_fin"].apply(compute_relevance)
    # Convert main race_date to datetime
    df["race_date"] = pd.to_datetime(df["race_date"])
    # 3) We create or define 'group_id' for CatBoost ranking
    #    E.g., combine (course_cd, race_date, race_number)
    # Create race_id + group_id
    df["group_id"] = (
        df["course_cd"].astype(str)
        + "_"
        + df["race_date"].astype(str)
        + "_"
        + df["race_number"].astype(str)
    ).astype("category").cat.codes

    # 4) Now define your final features for the model.
    #    *Anything not listed here is simply not used,
    #    but remains in df for you to refer to if needed.
    # Convert selected datetime columns to numeric
    datetime_columns = ["first_race_date_5", "most_recent_race_5", "prev_race_date"]
    new_numeric_cols = {}
    for col in datetime_columns:
        df[col] = pd.to_datetime(df[col])
        new_numeric_cols[col + "_numeric"] = (df[col] - pd.Timestamp("1970-01-01")).dt.days
    # Drop the original datetime columns
    df.drop(columns=datetime_columns, inplace=True, errors="ignore")
    df = pd.concat([df, pd.DataFrame(new_numeric_cols, index=df.index)], axis=1)
# Then in final_feature_cols, use 'first_race_date_5_numeric' instead of 'first_race_date_5'
    
    final_feature_cols = ["has_gps","recent_avg_speed", "pace_delta_time", "time_behind" , "avgspd", "prev_race_date_numeric", "avg_dist_bk_gate1_5","gps_present", 
                          "trainer_itm_track", "net_sentiment","dam_roi", "distance_meters", "jt_itm_percent", "sire_roi", "combined_3", "morn_odds", "avg_speed_5", 
                          "avg_workout_rank_3", "jock_win_percent", "trainer_win_percent", "avg_strfreq_q1_5", "cond_show", "avg_dist_bk_gate4_5", 
                          "standardized_score", "all_starts", "raw_performance_score", "weight", "jock_win_track", "avg_dist_bk_gate2_5", "jock_itm_track", 
                          "count_workouts_3", "par_time", "sire_itm_percentage", "cond_starts", "normalized_score", "speed_improvement", "ave_cl_sd", 
                          "all_earnings", "best_speed", "official_distance", "jt_win_percent", "all_show", "cond_fourth", "base_speed", "hi_spd_sd", 
                           "avg_beaten_len_5", "wide_factor", "avg_dist_bk_gate3_5", "total_races_5", "days_off", "class_multiplier", "all_fourth", 
                          "starts", "age_at_race_day", "trainer_itm_percent", "cond_win", "avg_speed_fullrace_5", "purse", "all_win", 
                          "avg_spd_sd", "combined_0", "trainer_win_track", "avg_fin_5", "prev_speed_rating", "combined_2", "avg_stride_length_5", "avg_strfreq_q2_5", 
                          "jt_win_track", "class_offset", "horse_itm_percentage", "jt_itm_track", "speed_rating", "jock_itm_percent", "avg_strfreq_q3_5", "pstyerl", 
                          "all_place", "combined_1", "avg_strfreq_q4_5", "power", "previous_class", "race_count", "prev_speed", "claimprice", "first_race_date_5_numeric", 
                          "off_finish_last_race", "cond_place", "most_recent_race_5_numeric", "previous_distance", "par_diff_ratio", "class_rating",  
                          "combined_4", "dam_itm_percentage", "cond_earnings"]

    # 5) Mark which of these are categorical:
    cat_cols = []
    # # We'll figure out their indices in final_features_cols
    # cat_features_in_data = []
    # for cat in cat_cols:
    #     if cat in final_features_cols:
    #         cat_features_in_data.append(final_features_cols.index(cat))

    # 6) Build X, y for training
    #    This doesn't remove official_fin or anything from df—just not in final_features_cols => not used.
    # 1) Sort df by group_id first
    df = df.sort_values("group_id").reset_index(drop=True)

    # 2) Then build X, y, group_id from the sorted DataFrame
    X = df[final_feature_cols]
    y = df["relevance"].values
    group_id = df["group_id"].values

    # 3) Create the Pool
    train_pool = Pool(
        data=X,
        label=y,
        group_id=group_id
    )

    # 8) If you already have a trained model, load it to do feature importances.
    model_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/catboost/catboost_YetiRank:top=2_NDCG:top=2_20250222_045419.cbm"
    model = CatBoostRanker()
    model.load_model(model_path)

    # 9) Compute feature importances
    importances = model.get_feature_importance(train_pool, type="FeatureImportance")

    imp_df = pd.DataFrame({
        "feature": final_feature_cols,
        "importance": importances
    }).sort_values("importance", ascending=False)

    print("\n=== Feature Importances ===")
    print(imp_df.to_string(index=False))

if __name__ == "__main__":
    main()