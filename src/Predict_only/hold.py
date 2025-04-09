import os
import re
import logging
import numpy as np
import pandas as pd
from catboost import CatBoostRanker, Pool

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
    2) Load each .cbm CatBoost model from 'models_dir' (or just one if you only have one)
    3) Predict => store in 'top_1_score' column
    4) SHIFT => store in 'top_1_score_pos'
    5) rank => store in 'top_1_rank'
    6) Write final predictions to DB
    """

    logging.info("=== Starting Multi-Model Future Inference ===")

    # 1) Combine columns
    all_feature_cols = final_feature_cols + embed_cols + cat_cols

    # 2) Copy fut_df => avoid warnings
    fut_df = fut_df.copy()

    # 3) Convert cat columns
    for c in cat_cols:
        if c in fut_df.columns:
            fut_df[c] = fut_df[c].astype("category")

    # 4) Check missing
    missing_cols = set(all_feature_cols) - set(fut_df.columns)
    if missing_cols:
        logging.warning(f"Future DF is missing columns: {missing_cols}")

    # 5) Locate model files
    try:
        model_files = [
            f for f in os.listdir(models_dir)
            if f.endswith(".cbm") and os.path.isfile(os.path.join(models_dir, f))
        ]
        model_files.sort()
        if not model_files:
            logging.error(f"No CatBoost model files found in {models_dir}!")
            return spark.createDataFrame(fut_df)  # Return empty if no model
        logging.info("Found these CatBoost model files:")
        for m in model_files:
            logging.info(f"  {m}")
    except Exception as e:
        logging.error(f"Error accessing model directory '{models_dir}': {e}", exc_info=True)
        raise

    # 6) For each model, run predictions
    # If you have multiple models but only want to store in top_1_score each time, 
    # you either pick just the first model or overwrite it. 
    # We'll show overwriting logic for demonstration
    for file in model_files:
        model_path = os.path.join(models_dir, file)
        logging.info(f"=== Making predictions with model: {file} ===")

        # 6A) Prepare data for inference
        inference_df = fut_df[all_feature_cols].copy()

        if "group_id" in fut_df.columns:
            inference_df["group_id"] = fut_df["group_id"]
        else:
            logging.warning("No 'group_id' column found. Ranking won't apply well.")

        # 6B) Predict => store raw model score in scored_df["model_score"]
        scored_df = make_future_predictions(
            pdf=inference_df,
            all_feature_cols=all_feature_cols,
            cat_cols=cat_cols,
            model_path=model_path,
            model_type="ranker",
            alpha=5.0
        )

        # 6C) Instead of dynamic naming, we ALWAYS put them in "top_1_score"
        fut_df.loc[inference_df.index, "top_1_score"] = scored_df["model_score"].values

        # 6D) SHIFT => "top_1_score_pos"
        #    RANK  => "top_1_rank"
        score_col = "top_1_score"
        score_pos_col = "top_1_score_pos"
        rank_col = "top_1_rank"

        if "group_id" in fut_df.columns:
            # SHIFT
            fut_df[score_pos_col] = (
                fut_df[score_col]
                - fut_df.groupby("group_id")[score_col].transform("min")
                + 0.1
            )
            # RANK descending
            fut_df[rank_col] = fut_df.groupby("group_id")[score_col] \
                                     .transform(lambda s: s.rank(method="first", ascending=False))
            fut_df[rank_col] = fut_df[rank_col].astype(int)
        else:
            # SHIFT entire set
            min_val = fut_df[score_col].min()
            fut_df[score_pos_col] = fut_df[score_col] - min_val + 0.1
            # RANK entire set descending
            fut_df[rank_col] = fut_df[score_col].rank(method="first", ascending=False).astype(int)

        # If you only have one model, you might break here or just loop once

    # 7) Write final predictions to DB
    today_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name = f"predictions_{today_str}_1"

    logging.info(f"Writing predictions to DB table: {table_name}")
    scored_sdf = spark.createDataFrame(fut_df)

    # Optional: If official_fin is NaN, set to NULL
    if "official_fin" in scored_sdf.columns:
        from pyspark.sql.functions import col, when, isnan, lit
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


def make_future_predictions(
    pdf,
    all_feature_cols,
    cat_cols,
    model_path,
    model_type="ranker",
    alpha=5.0
):
    from catboost import CatBoostRanker, CatBoostRegressor, Pool

    # load the model
    if model_type.lower() == "ranker":
        model = CatBoostRanker()
    else:
        model = CatBoostRegressor()

    model.load_model(model_path)

    # If group_id in columns, sort by it
    if "group_id" in pdf.columns:
        pdf.sort_values("group_id", inplace=True)

    X_infer = pdf[all_feature_cols].copy()
    group_ids = pdf["group_id"].values if "group_id" in pdf.columns else None

    pred_pool = Pool(data=X_infer, group_id=group_ids, cat_features=cat_cols)
    predictions = model.predict(pred_pool)

    # exponentiate
    pdf["model_score"] = np.exp(predictions / alpha)

    return pdf