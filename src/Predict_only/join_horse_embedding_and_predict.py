import pandas as pd
import sys
import logging
import os
import re
import sys
import logging
import datetime
from datetime import datetime, date
import pandas as pd
import numpy as np
from catboost import CatBoostRanker, Pool
from pyspark.sql.functions import col, when, isnan, lit

def make_future_predictions(
    pdf,
    all_feature_cols,
    cat_cols,
    model_path,
    model_type="ranker"
):
    """
    1) Load CatBoost model from model_path
    2) Create Pool for inference (cat_features, group_id if ranker)
    3) Predict
    4) Return pdf with new column 'model_score' (raw predictions, no exponentiation).
    """
    from catboost import CatBoostRanker, CatBoostRegressor, Pool
    
    # 1) Load model
    if model_type.lower() == "ranker":
        model = CatBoostRanker()
    else:
        model = CatBoostRegressor()

    model.load_model(model_path)
    logging.info(f"Loaded CatBoost model: {model_path}")

    # 2) Sort by group_id if available (contiguous blocks for ranker)
    if "group_id" in pdf.columns:
        pdf.sort_values("group_id", inplace=True)

    # 3) Prepare data for inference
    X_infer = pdf[all_feature_cols].copy()
    group_ids = pdf["group_id"].values if "group_id" in pdf.columns else None

    pred_pool = Pool(data=X_infer, group_id=group_ids, cat_features=cat_cols)
    
    # 4) Raw predictions
    predictions = model.predict(pred_pool)
    pdf["model_score"] = predictions  # no exponentiation

    return pdf

def do_future_inference_multi(
    spark,
    fut_df,
    cat_cols,
    embed_cols,
    final_feature_cols,
    db_url,
    db_properties,
    models_dir="./data/models/catboost",
    output_dir="./data/predictions"
):
    """
    1) Combine final_feature_cols, embed_cols, cat_cols
    2) Load each .cbm CatBoost model from 'models_dir'
    3) Predict => store in 'score' columns
    4) Sort descending by that score, do cumcount()+1 => rank
    5) Write final predictions to DB
    """

    logging.info("=== Starting Multi-Model Future Inference ===")

    # 1) Combine columns for inference
    all_feature_cols = final_feature_cols + embed_cols + cat_cols

    # 2) Copy fut_df
    fut_df = fut_df.copy()

    # 3) Convert cat columns
    for c in cat_cols:
        if c in fut_df.columns:
            fut_df[c] = fut_df[c].astype("category")

    # 4) Check missing
    missing_cols = set(all_feature_cols) - set(fut_df.columns)
    if missing_cols:
        logging.warning(f"Future DF is missing columns: {missing_cols}")

    # 5) Find .cbm files
    try:
        model_files = [
            f for f in os.listdir(models_dir)
            if f.endswith(".cbm") and os.path.isfile(os.path.join(models_dir, f))
        ]
        model_files.sort()
        if not model_files:
            logging.error(f"No CatBoost model files found in {models_dir}!")
            return spark.createDataFrame(fut_df)

        logging.info("Found these CatBoost model files:")
        for m in model_files:
            logging.info(f"  {m}")
    except Exception as e:
        logging.error(f"Error accessing model directory '{models_dir}': {e}", exc_info=True)
        raise

    # 6) For each model, run predictions
    for file in model_files:
        model_path = os.path.join(models_dir, file)
        logging.info(f"=== Making predictions with model: {file} ===")

        # 6A) Prepare data for inference
        inference_df = fut_df[all_feature_cols].copy()

        if "group_id" in fut_df.columns:
            inference_df["group_id"] = fut_df["group_id"]
        else:
            logging.warning("No 'group_id' column found. Ranker grouping won't apply correctly.")

        # 6B) Predict (no exponentiation!)
        scored_df = make_future_predictions(
            pdf=inference_df,
            all_feature_cols=all_feature_cols,
            cat_cols=cat_cols,
            model_path=model_path,
            model_type="ranker",  # or "regressor"
        )

        # 6C) Build a stable column prefix
        import re
        model_col = file
        model_col = re.sub(r'^catboost_', '', model_col)
        model_col = re.sub(r'\.cbm$', '', model_col)
        model_col = re.sub(r'_\d{8}_\d{6}$', '', model_col)
        model_col = re.sub(r'[^a-zA-Z0-9_]', '_', model_col)

        # 6D) Insert the raw 'model_score'
        fut_df.loc[inference_df.index, f"{model_col}_score"] = scored_df["model_score"].values

        # 6E) Sort => rank with cumcount
        score_col = f"{model_col}_score"
        rank_col = f"{model_col}_rank"
        
        # Sort by group_id ascending, then score descending
        fut_df = fut_df.sort_values(by=["group_id", score_col], ascending=[True, False])
        
        # rank => cumcount()+1
        fut_df[rank_col] = fut_df.groupby("group_id").cumcount() + 1

        # If you want to keep final DataFrame in same index order as before,
        # you might .reset_index(drop=True) or reorder, but that’s up to you.

    # 7) Write final predictions to DB
    today_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name = f"predictions_{today_str}_1"

    logging.info(f"Writing predictions to DB table: {table_name}")
    scored_sdf = spark.createDataFrame(fut_df)

    # If official_fin is present, convert NaN => NULL
    from pyspark.sql.functions import col, when, isnan, lit
    if "official_fin" in scored_sdf.columns:
        scored_sdf = scored_sdf.withColumn(
            "official_fin",
            when(isnan(col("official_fin")), lit(None).cast("double"))
            .otherwise(col("official_fin"))
        )

    scored_sdf.write.format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", table_name) \
        .option("user", db_properties["user"]) \
        .option("driver", db_properties["driver"]) \
        .mode("overwrite") \
        .save()

    logging.info(f"Wrote {fut_df.shape[0]} predictions to DB table '{table_name}'.")
    logging.info("=== Finished Multi-Model Future Inference ===")

    return scored_sdf

def merge_with_embedding_and_coalesce(
    predictions_df: pd.DataFrame,
    horse_embedding_df: pd.DataFrame,
    join_key: str = "horse_id"
) -> pd.DataFrame:
    """
    1) Identify which columns appear in both vs. only in horse_embedding_df.
    2) Merge them on `join_key`.
    3) For common columns, if predictions_df col is NULL or 0, use the embed col.
    4) Return the resulting DataFrame.
    """

    pred_cols = set(predictions_df.columns)
    embed_cols = set(horse_embedding_df.columns)

    # Columns that exist in both dataframes
    common_cols = pred_cols & embed_cols

    # Columns that exist only in the embedding => brand-new columns in predictions
    missing_cols = embed_cols - pred_cols

    # We want to retrieve from `horse_embedding_df` both the columns that
    # are missing plus the columns that are common (so we can coalesce).
    columns_to_merge = [join_key] + list(common_cols) + list(missing_cols)

    # Filter embed to only the columns we want
    embed_subset = horse_embedding_df[columns_to_merge].copy()

    # Merge
    # We'll add a suffix (e.g. "_emb") for the embedding columns so we can coalesce easily
    merged_df = predictions_df.merge(
        embed_subset,
        on=join_key,
        how="left",
        suffixes=("", "_emb")  # so common columns from embedding become colname_emb
    )

    # For each column that appears in both:
    # If predictions_df col is null or 0 => use the embedding col
    for col in common_cols:
        embed_col = f"{col}_emb"
        if embed_col in merged_df.columns:
            # Condition: (col is null) OR (col == 0)
            # np.where(condition, if_true, if_false)
            merged_df[col] = np.where(
                (merged_df[col].isna()) | (merged_df[col] == 0),
                merged_df[embed_col],
                merged_df[col]
            )
            # Drop the temporary col from embedding
            merged_df.drop(columns=[embed_col], inplace=True, errors="ignore")

    return merged_df

def transform_horse_df_to_pandas(df, drop_label=False):
    """
    3) Create race_id, group_id, convert date columns, etc.
    4) Return the transformed Pandas DataFrame.
    """

    # Check the dtypes in Pandas (should show datetime64[ns] for the above columns)
    # Now you can safely convert to Pandas
    # Create group_id and sort ascending
    df["group_id"] = df["race_id"].astype("category").cat.codes
    df = df.sort_values("group_id", ascending=True).reset_index(drop=True)
    
    # Historical: Convert selected datetime columns to numeric
    datetime_columns = ["first_race_date_5", "most_recent_race_5", "prev_race_date"]
    new_numeric_cols = {}
    for col in datetime_columns:
        df[col] = pd.to_datetime(df[col])
        new_numeric_cols[col + "_numeric"] = (df[col] - pd.Timestamp("1970-01-01")).dt.days
    # Historical: Drop the original datetime columns
    df.drop(columns=datetime_columns, inplace=True, errors="ignore")
    df = pd.concat([df, pd.DataFrame(new_numeric_cols, index=df.index)], axis=1)


    # Historical: Convert main race_date to datetime
    df["race_date"] = pd.to_datetime(df["race_date"])
    
    # # Make certain columns categorical
    cat_cols = ["course_cd", "trk_cond", "sex", "equip", "surface", "med",
                 "race_type", "stk_clm_md", "turf_mud_mark", "layoff_cat","previous_surface"]
    # cat_cols = []
    
    # Historical: Convert to categorical data type
    for c in cat_cols:
        if c in df.columns:
            df[c] = df[c].astype("category")
            
    
    # Future: Convert to categorical data type
    for c in cat_cols:
        if c in df.columns:
            df[c] = df[c].astype("category")
                          
    # Return the transformed Pandas DataFrame, along with cat_cols and excluded_cols
    return df, cat_cols

def build_embed_cols(df):
    """
    1) Create a list of embedding columns.
    2) Return the list of embedding columns for historical and future data.
    """
    all_embed_cols = [c for c in df.columns if c.startswith("embed_")]
    all_embed_cols_sorted = sorted(all_embed_cols, key=lambda x: int(x.split("_")[1]))
    
    # For both historical and future datasets, use the same sorted list.
    embed_cols = all_embed_cols_sorted
    return embed_cols

def race_predictions(spark, predictions_df, horse_embedding, jdbc_url, jdbc_properties, action):
    # Print all column names prediction_df
    #print("Columns in prediction_df DataFrame:", predictions_df.columns.tolist())
    # Print all column names prediction_df
    #print("Columns in horse_embedding DataFrame:", horse_embedding.columns.tolist())
    
    # Merge the two DataFrames
    merged_df = merge_with_embedding_and_coalesce(predictions_df,horse_embedding)
    
    # Print all column names prediction_df
    print("Columns in horse_embedding DataFrame:", merged_df.columns.tolist())
    
    embed_cols = build_embed_cols(merged_df)
    
            # 1) Prepare columns to match training
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

    pred_pdf, cat_cols = transform_horse_df_to_pandas(merged_df, drop_label=False)
    
    if 'group_id' not in merged_df.columns:
        print("The 'group_id' column is NOT present in fut_df. Exiting the program.")
        sys.exit(1)
        
    scored_sdf = do_future_inference_multi(
        spark=spark,
        fut_df=pred_pdf,
        cat_cols=cat_cols,
        embed_cols=embed_cols,       # or hist_embed_cols if they match
        final_feature_cols=final_feature_cols,
        db_url=jdbc_url,
        db_properties=jdbc_properties,
        models_dir="/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/catboost",  # or your actual path
        output_dir="/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/predictions"
    )
