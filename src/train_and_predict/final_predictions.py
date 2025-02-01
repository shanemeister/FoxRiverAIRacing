import logging
import datetime
import os
import numpy as np
import pandas as pd
from pyspark.sql.functions import col
from catboost import CatBoostRanker, Pool

def load_models(df):
    """
    Filter the Spark DF to future races (data_flag == 'future'),
    convert to Pandas, do transformations, and return:
       - pdf (Pandas DataFrame of future rows)
       - final_feature_cols (list of columns for CatBoost)
       - cat_feature_names (list of categorical feature names)
    """
    future_races_df = df.filter(col("data_flag") == "future")
    count_future = future_races_df.count()
    logging.info(f"Row count of future_races_df: {count_future}")

    pdf = future_races_df.toPandas()
    pdf.drop(columns=["official_fin"], inplace=True, errors="ignore")

    pdf["race_id"] = (
        pdf["course_cd"].astype(str)
        + "_"
        + pdf["race_date"].astype(str)
        + "_"
        + pdf["race_number"].astype(str)
    )
    pdf["group_id"] = pdf["race_id"].astype("category").cat.codes

    # Convert these date columns to numeric
    datetime_columns = ["first_race_date_5", "most_recent_race_5", "prev_race_date"]
    new_numeric_cols = {}
    for dcol in datetime_columns:
        pdf[dcol] = pd.to_datetime(pdf[dcol])
        new_numeric_cols[dcol + "_numeric"] = (
            pdf[dcol] - pd.Timestamp("1970-01-01")
        ).dt.days

    pdf.drop(columns=datetime_columns, inplace=True, errors="ignore")
    pdf = pd.concat([pdf, pd.DataFrame(new_numeric_cols, index=pdf.index)], axis=1)

    pdf["race_date"] = pd.to_datetime(pdf["race_date"])

    cat_feature_names = [
        "course_cd", "trk_cond", "sex", "equip", "surface", "med",
        "race_type", "stk_clm_md", "turf_mud_mark", "layoff_cat",
        "previous_surface"
    ]
    embed_cols = [f"embed_{i}" for i in range(4)]
    excluded_cols = [
        "horse_name", "axciskey", "race_date", "race_number", "horse_id",
        "race_id", "track_name", "perf_target", "data_flag"  # exclude data_flag too
    ]

    # Convert cat_features to category dtype
    for c in cat_feature_names:
        if c in pdf.columns:
            pdf[c] = pdf[c].astype("category")

    # Build final_feature_cols
    all_cols = pdf.columns.tolist()
    base_feature_cols = [c for c in all_cols if c not in excluded_cols]
    final_feature_cols = list(set(base_feature_cols + cat_feature_names + embed_cols))
    final_feature_cols.sort()

    logging.info(f"Future races PDF shape: {pdf.shape}")
    logging.info(f"Final feature columns: {final_feature_cols}")

    return pdf, final_feature_cols, cat_feature_names

def make_future_predictions(
    pdf, 
    final_feature_cols, 
    cat_feature_names, 
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

    X = pdf[final_feature_cols].copy()
    group_ids = pdf["group_id"].values if "group_id" in pdf.columns else None

    # Which cat features actually exist in X
    cat_features_in_data = [c for c in cat_feature_names if c in X.columns]

    # In newer CatBoost versions, you can pass cat_features as names
    pred_pool = Pool(
        data=X,
        group_id=group_ids,
        cat_features=cat_features_in_data
    )

    predictions = model.predict(pred_pool)
    pdf["prediction"] = predictions
    return pdf

def run_inference_for_future_multi(
    spark, 
    df, 
    db_url,         # e.g. "jdbc:postgresql://host:port/dbname"
    db_properties,  # dict with e.g. { "user": "X", "password": "Y", "driver": "org.postgresql.Driver" }
    models_dir="./data/models/catboost", 
    output_dir="./data/predictions"
):
    """
    1) Transform the Spark DF (historical + future) to a Pandas DF of future rows.
    2) Find all .cbm model files in models_dir.
    3) For each model, add a 'score_X' column (score_A, score_B, etc.).
    4) Write the final scored dataset to CSV in output_dir (optional).
    5) Write the final scored dataset to a DB table in 'append' mode.
    6) Return a Spark DF of the results.
    """

    # A) Prepare the Pandas DataFrame & feature list
    pdf, final_feature_cols, cat_feature_names = load_models(df)

    # B) Gather CatBoost .cbm model paths
    model_files = [
        f for f in os.listdir(models_dir) 
        if f.endswith(".cbm") and os.path.isfile(os.path.join(models_dir, f))
    ]
    model_files.sort()
    logging.info("Found these CatBoost model files:")
    for m in model_files:
        logging.info(f"  {m}")

    # C) Inference loop
    for i, model_file in enumerate(model_files, start=1):
        model_path = os.path.join(models_dir, model_file)
        scored_df = make_future_predictions(
            pdf, 
            final_feature_cols, 
            cat_feature_names,
            model_path, 
            model_type="ranker"
        )
        score_col = f"score_{chr(ord('A') + i - 1)}"
        pdf[score_col] = scored_df["prediction"]

    # Remove the last "prediction" column
    if "prediction" in pdf.columns:
        pdf.drop(columns=["prediction"], inplace=True)

    # D) Optionally write to CSV
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "predictions.csv")
    pdf.to_csv(out_path, index=False)
    logging.info(f"Wrote final predictions to: {out_path}")

    # E) Write to database
    # Create a dynamic table name: predictions_YYYY_MM_DD_1 (or similar)
    today_str = datetime.date.today().strftime('%Y_%m_%d')
    table_name = f"predictions_{today_str}_1"

    logging.info(f"Writing predictions to DB table: {table_name}")
    # Convert back to Spark
    scored_sdf = spark.createDataFrame(pdf)

    try:
        # Append data to the table
        scored_sdf.write.format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", table_name) \
            .option("user", db_properties["user"]) \
            .option("driver", db_properties["driver"]) \
            .mode("append") \
            .save()
        logging.info(f"Appended {pdf.shape[0]} predictions to DB table '{table_name}'.")
    except Exception as e:
        logging.error(f"Error saving predictions to DB: {e}", exc_info=True)
        raise

    return scored_sdf

def main_inference(spark, combined_df, db_url, db_properties):
    """
    Example main function to run multi-model inference, 
    produce CSV, and append results to DB.
    """
    predictions_sdf = run_inference_for_future_multi(
        spark, 
        df=combined_df,
        db_url=db_url,
        db_properties=db_properties,
        models_dir="/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/catboost",
        output_dir="./data/predictions"
    )
    # Show some rows
    predictions_sdf.show(50)