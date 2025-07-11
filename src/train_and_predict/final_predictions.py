import logging
import datetime
import os
import re
import numpy as np
import pandas as pd
from pyspark.sql.functions import col
from catboost import CatBoostRanker, Pool
from pandas.api.types import is_categorical_dtype
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import timedelta

def make_future_predictions(
    pdf, 
    all_feature_cols, 
    cat_cols,
    model_path, 
    model_type="ranker"
):
    """
    1) Load CatBoost model from model_path
    2) Build a Pool (with cat_features) for inference
    3) Predict
    4) Return pdf with new column 'prediction'
    """
    try:
        from catboost import CatBoostRanker, Pool
        if model_type.lower() == "ranker":
            model = CatBoostRanker()
        else:
            from catboost import CatBoostRegressor
            model = CatBoostRegressor()

        model.load_model(model_path)
        logging.info(f"Loaded CatBoost model: {model_path}")
    except Exception as e:
        logging.error(f"Error loading CatBoost model: {e}", exc_info=True)
        raise   
    try:
        # Sort by group_id so the ranker sees each group in contiguous rows
        if "group_id" in pdf.columns:
            pdf.sort_values("group_id", inplace=True)
        # Build X from final_feature_cols
        X_infer = pdf[all_feature_cols].copy()
        group_ids = pdf["group_id"].values if "group_id" in pdf.columns else None
        pred_pool = Pool(
            data=X_infer,
            group_id=group_ids,
            cat_features=cat_cols
        )

        predictions = model.predict(pred_pool)
        pdf["model_score"] = predictions
    except Exception as e:
        logging.error(f"Error making predictions: {e}", exc_info=True)
        raise
    
    return pdf

# Helper function to derive a safe column name from a model filename.
def derive_model_column_name(file_name):
    """
    Given a filename like:
       catboost_YetiRank:top=1_NDCG:top=1_20250220_160539.cbm
    this function returns a column name like:
       YetiRank_top1_NDCG_top1
    """
    col_name = file_name
    # Remove the leading prefix and the trailing timestamp and extension.
    col_name = re.sub(r'^catboost_', '', col_name)
    col_name = re.sub(r'_\d{8}_\d{6}\.cbm$', '', col_name)
    # Replace colon with underscore
    col_name = col_name.replace(":", "_")
    # Remove any character that is not alphanumeric or underscore.
    col_name = re.sub(r'[^A-Za-z0-9_]', '', col_name)
    return col_name

def run_inference_for_future_multi(
    spark, 
    cat_cols,
    excluded_cols,
    embed_cols,
    fut_df, 
    db_url,
    db_properties,
    models_dir="./data/models/catboost", 
    output_dir="./data/predictions"
):
    """
    1) Build final feature list for fut_df (the future DataFrame)
    2) For each .cbm model in models_dir:
       - Make predictions -> a temporary 'model_score' column
       - Parse model filename -> a safe column name
       - fut_df[safe_name] = scored_df["model_score"]
       - Drop 'model_score'
    3) Write the final DataFrame to a DB table, etc.
    4) Return a Spark DataFrame of the results.
    """
    # Load the exact feature order from your JSON file.
    #with open("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/training/final_feature_cols_20250310_215902.json", "r") as f:
    #    final_feature_cols = json.load(f)
    
    final_feature_cols = ["global_speed_score_iq_prev", "previous_class", "class_rating", "previous_distance", "off_finish_last_race", 
                          "prev_speed_rating","purse", "claimprice", "power", "avgspd", "avg_spd_sd","ave_cl_sd",
                          "hi_spd_sd", "pstyerl", "horse_itm_percentage","total_races_5", "avg_dist_bk_gate1_5", "avg_dist_bk_gate2_5",
                            "avg_dist_bk_gate3_5", "avg_dist_bk_gate4_5", "avg_speed_fullrace_5", "avg_stride_length_5", "avg_strfreq_q1_5",
                            "avg_speed_5","avg_fin_5","best_speed","avg_beaten_len_5","prev_speed",
                            "avg_strfreq_q2_5", "avg_strfreq_q3_5", "avg_strfreq_q4_5", "speed_improvement", "age_at_race_day",
                          "count_workouts_3","avg_workout_rank_3","weight","days_off", "starts", "race_count","has_gps",
                          "cond_starts","cond_win","cond_place","cond_show","cond_fourth","cond_earnings",
                          "all_starts", "all_win", "all_place", "all_show", "all_fourth","all_earnings", 
                          "morn_odds","net_sentiment", 
                          "distance_meters", "jt_itm_percent", "jt_win_percent", 
                          "trainer_win_track", "trainer_itm_track", "trainer_win_percent", "trainer_itm_percent", 
                          "jock_win_track", "jock_win_percent","jock_itm_track",                            
                          "jt_win_track", "jt_itm_track", "jock_itm_percent",
                          "sire_itm_percentage", "sire_roi", "dam_itm_percentage", "dam_roi"]
    # Do NOT sort final_feature_cols here—use the order as loaded.
    # Suppose you already did:
    # final_feature_cols = json.load(...)

    # Merge cat_cols to ensure they appear in final_feature_cols    
    all_feature_cols = final_feature_cols + embed_cols + cat_cols 
    fut_df = fut_df.copy()
    for c in cat_cols:
        if c in fut_df.columns:
            #fut_df[c] = fut_df[c].astype("category")
            fut_df.loc[:, c] = fut_df[c].astype("category")        
    try:
        # Gather model files.
        model_files = [
            f for f in os.listdir(models_dir) 
            if f.endswith(".cbm") and os.path.isfile(os.path.join(models_dir, f))
        ]
        model_files.sort()  # sort the filenames if needed, but NOT the feature list
        logging.info("Found these CatBoost model files:")
        for m in model_files:
            logging.info(f"  {m}")

        # Loop over each model and make predictions.
        for file in model_files:
            try:
                inference_df = fut_df[all_feature_cols].copy()
                model_path = os.path.join(models_dir, file)
                logging.info(f"Making predictions with model: {file}")
                logging.info(f"Inference DF nulls:\n{inference_df.isnull().sum()}")
                logging.info(f"inference_df columns before reindexing: {inference_df.columns.tolist()}")
                logging.info("Inference DF dtypes:")
                logging.info(inference_df.dtypes)
                logging.info("Null counts:")
                logging.info(inference_df.isnull().sum())
                logging.info(f"inference_df columns after reindexing: {inference_df.columns.tolist()}")
                try:
                    missing_cols = [col for col in all_feature_cols if col not in inference_df.columns]
                    if missing_cols:
                        logging.error(f"Missing columns in inference_df: {missing_cols}")
                except Exception as e:
                    logging.error(f"Error checking missing columns: {e}", exc_info=True)
                    raise
            except Exception as e:
                logging.error(f"Error preparing inference data: {e}", exc_info=True)
                raise   
            
            try:

                scored_df = make_future_predictions(
                    inference_df, 
                    all_feature_cols,
                    cat_cols, 
                    model_path, 
                    model_type="ranker"
                )
            except Exception as e:
                logging.error(f"Error making predictions: {e}", exc_info=True)
                raise
            # Derive a safe column name from the model filename.
            model_col = file
            model_col = re.sub(r'^catboost_', '', model_col)
            model_col = re.sub(r'\.cbm$', '', model_col)
            model_col = re.sub(r'_\d{8}_\d{6}$', '', model_col)
            model_col = re.sub(r'[^a-zA-Z0-9_]', '_', model_col)
            
            # Store predictions in fut_df.
            fut_df[model_col] = scored_df["model_score"]
            
            # Drop the temporary "model_score" column if it exists.
            if "model_score" in fut_df.columns:
                fut_df.drop(columns=["model_score"], inplace=True)

    except Exception as e:
        logging.error(f"Error running inference: {e}", exc_info=True)
        logging.info("Make sure you have models in the catboost directory.")
        raise
    
    # (E) Date predictions are run.
    
    # Set 'today' to yesterday's date
    today = datetime.date.today()
    
    # (F) Convert fut_df back to a Spark DataFrame.
    scored_sdf = spark.createDataFrame(fut_df)
    
    # (G) Create a dynamic table name (e.g., predictions_YYYY_MM_DD_1) and write to the database.
    today_str = today.strftime('%Y_%m_%d')
    table_name = f"predictions_{today_str}_1"
    logging.info(f"Writing predictions to DB table: {table_name}")
    
    scored_sdf.write.format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", table_name) \
        .option("user", db_properties["user"]) \
        .option("driver", db_properties["driver"]) \
        .mode("overwrite") \
        .save()
    
    logging.info(f"Appended {fut_df.shape[0]} predictions to DB table '{table_name}'.")
    
    return scored_sdf

def main_inference(spark, fut_pdf, cat_cols, embed_cols, excluded_cols, jdbc_url, jdbc_properties):
    """
    Example main function to run multi-model inference, 
    produce CSV, and append results to DB.
    """
    tpd_tracks = [
    'CNL','SAR','PIM','TSA','BEL','MVR','TWO','CLS','KEE','TAM','TTP','TKD','ELP','PEN','HOU',
    'DMR','TLS','AQU','MTH','TGP','TGG','CBY','LRL','TED','IND','CTD','ASD','TCD','LAD','TOP'
    ]
    fut_pdf = fut_pdf[fut_pdf["course_cd"].isin(tpd_tracks)]

    predictions_sdf = run_inference_for_future_multi(
        spark, 
        cat_cols,
        excluded_cols,
        embed_cols,
        fut_df=fut_pdf,
        db_url=jdbc_url,
        db_properties=jdbc_properties,
        models_dir="/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/catboost",
        output_dir="./data/predictions"
    )
    logging.info("Inference complete.")
    return predictions_sdf