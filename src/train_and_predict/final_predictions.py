import logging
import datetime
import os
import re
import numpy as np
import pandas as pd
from pyspark.sql.functions import col
from catboost import CatBoostRanker, Pool
from pandas.api.types import is_categorical_dtype

def defines_final_feature_cols(pdf, cat_cols, excluded_cols):
    """
    Filter the Spark DF to future races (data_flag == 'future'),
    convert to Pandas, do transformations, and return:
       - pdf (Pandas DataFrame of future rows)
       - final_feature_cols (list of columns for CatBoost)
       - cat_feature_names (list of categorical feature names)
    """
    # Categorical features - we will cast them to "category" type

    # If you have some embedding columns
    embed_cols = [f"embed_{i}" for i in range(4)]

    # Build final_feature_cols
    all_cols = pdf.columns.tolist()
    base_feature_cols = [c for c in all_cols if c not in excluded_cols]
    final_feature_cols = list(set(base_feature_cols + cat_cols + embed_cols))
    final_feature_cols.sort()

    logging.info(f"Future races PDF shape: {pdf.shape}")
    logging.info(f"Final feature columns: {final_feature_cols}")

    return pdf, final_feature_cols

def make_future_predictions(
    pdf, 
    final_feature_cols, 
    model_path, 
    model_type="ranker"
):
    """
    1) Load CatBoost model from model_path
    2) Build a Pool (with cat_features) for inference
    3) Predict
    4) Return pdf with new column 'prediction'
    """
    from catboost import CatBoostRanker, Pool
    if model_type.lower() == "ranker":
        model = CatBoostRanker()
    else:
        from catboost import CatBoostRegressor
        model = CatBoostRegressor()

    model.load_model(model_path)
    logging.info(f"Loaded CatBoost model: {model_path}")

    # Sort by group_id so the ranker sees each group in contiguous rows
    if "group_id" in pdf.columns:
        pdf.sort_values("group_id", inplace=True)

    # Build X from final_feature_cols
    X_infer = pdf[final_feature_cols].copy()
    group_ids = pdf["group_id"].values if "group_id" in pdf.columns else None

    cat_features_in_data = [c for c in X_infer.columns if X_infer[c].dtype == "category"]
    pred_pool = Pool(
        data=X_infer,
        group_id=group_ids,
        cat_features=cat_features_in_data
    )

    predictions = model.predict(pred_pool)
    pdf["model_score"] = predictions
    
    return pdf

import re
import os
import datetime
import logging

def run_inference_for_future_multi(
    spark, 
    cat_cols,
    excluded_cols,
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
       - Parse model filename -> model_base
       - fut_df[model_base] = scored_df["model_score"]
       - Drop 'model_score'
    3) Write the final DataFrame (with one column per model) to a DB table in append mode
    4) Return a Spark DataFrame of the results
    """

    # A) Prepare columns
    fut_df, final_feature_cols = defines_final_feature_cols(fut_df, cat_cols, excluded_cols)
    num_columns = fut_df.shape[1]
    row_count = fut_df.shape[0]
    logging.info(f"Number of columns (start): {num_columns}")
    logging.info(f"Row count (start): {row_count}")

    try:
        # B) Gather CatBoost .cbm model paths
        model_files = [
            f for f in os.listdir(models_dir) 
            if f.endswith(".cbm") and os.path.isfile(os.path.join(models_dir, f))
        ]
        model_files.sort()
        logging.info("Found these CatBoost model files:")
        for m in model_files:
            logging.info(f"  {m}")

        # C) Loop over each model, make predictions, store in new column
        for file in model_files:
            model_path = os.path.join(models_dir, file)
            logging.info(f"Making predictions with model: {file}")

            # 1) Predict
            scored_df = make_future_predictions(
                fut_df, 
                final_feature_cols, 
                model_path, 
                model_type="ranker"
            )
            # 'scored_df["model_score"]' is the numeric predictions

            # 2) Derive a column name from the model filename
            # Remove leading "catboost_", trailing ".cbm", final "_YYYYmmdd_HHMMSS"
            model_base = file
            model_base = re.sub(r'^catboost_', '', model_base)
            model_base = re.sub(r'\.cbm$', '', model_base)
            model_base = re.sub(r'_\d{8}_\d{6}$', '', model_base)

            # 3) Store predictions in fut_df under that parse-based column name
            fut_df[model_base] = scored_df["model_score"]

            # 4) Remove 'model_score' from fut_df to avoid conflicts next iteration
            if "model_score" in fut_df.columns:
                fut_df.drop(columns=["model_score"], inplace=True)

    except Exception as e:
        logging.error(f"Error running inference: {e}", exc_info=True)
        logging.info("Make sure you have models in the catboost directory.")
        raise

    # D) Write to database
    # Create a dynamic table name: predictions_YYYY_MM_DD_1
    today_str = datetime.date.today().strftime('%Y_%m_%d')
    table_name = f"predictions_{today_str}_1"
    logging.info(f"Writing predictions to DB table: {table_name}")

    # Convert back to Spark
    scored_sdf = spark.createDataFrame(fut_df)

    try:
        # Append data to the table
        scored_sdf.write.format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", table_name) \
            .option("user", db_properties["user"]) \
            .option("driver", db_properties["driver"]) \
            .mode("append") \
            .save()
        logging.info(f"Appended {fut_df.shape[0]} predictions to DB table '{table_name}'.")
    except Exception as e:
        logging.error(f"Error saving predictions to DB: {e}", exc_info=True)
        raise

    return scored_sdf

def main_inference(spark, fut_df, cat_cols, excluded_cols, jdbc_url, jdbc_properties):
    """
    Example main function to run multi-model inference, 
    produce CSV, and append results to DB.
    """
    predictions_sdf = run_inference_for_future_multi(
        spark, 
        cat_cols=cat_cols,
        excluded_cols=excluded_cols,
        fut_df=fut_df,
        db_url=jdbc_url,
        db_properties=jdbc_properties,
        models_dir="/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/catboost",
        output_dir="./data/predictions"
    )
    logging.info("Inference complete.")
    return predictions_sdf