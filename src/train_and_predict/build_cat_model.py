import io
import json
import os
import logging
import datetime
import pandas as pd
import numpy as np
import optuna
from catboost import CatBoostRanker, Pool, CatBoostError
from sklearn.metrics import ndcg_score
from src.data_preprocessing.data_prep1.data_utils import save_parquet
from src.train_and_predict.final_predictions import main_inference

###############################################################################
# Helper function to get a timestamp for filenames
###############################################################################
def get_timestamp():
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


###############################################################################
# Helper function: The Optuna objective for ranking
###############################################################################
def objective(trial, catboost_loss_functions, eval_metric, y_valid, train_pool, valid_pool):
    """
    An Optuna objective function that trains a CatBoostRanker on (X_train, y_train)
    with group info (race_id_train), evaluates on (X_valid, y_valid) with group info
    (race_id_valid). Returns the validation NDCG:top=4 (or whichever metric we want to track)
    in order to maximize it.
    """
    # Log trial info
    logging.info(f"Starting Optuna Trial {trial.number}: {catboost_loss_functions}, Metric: {eval_metric}")
    params = {
        "loss_function": catboost_loss_functions,
        "eval_metric": eval_metric,
        "task_type": "GPU",
        "devices": "0,1",

        # Let’s allow 1000..3000, step=100
        "iterations": trial.suggest_int("iterations", 1000, 3000, step=100),

        # Depth from 5..10 (a bit wider than 5..9)
        "depth": trial.suggest_int("depth", 5, 10),

        # Expand learning_rate range significantly, from ~1e-4 up to 0.3
        # Use log=True so it can zoom in effectively
        "learning_rate": trial.suggest_float("learning_rate", 1e-4, 0.3, log=True),

        # Keep 1..10 for l2_leaf_reg
        "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1.0, 10.0),

        # Let’s add “SymmetricTree” if you want that as well
        "grow_policy": trial.suggest_categorical(
            "grow_policy",
            ["Depthwise", "Lossguide", "SymmetricTree"]
        ),

        # Expand random_strength range
        "random_strength": trial.suggest_float("random_strength", 1.0, 8.0),

        # Widen min_data_in_leaf to 1..20 (previously 5..15)
        "min_data_in_leaf": trial.suggest_int("min_data_in_leaf", 1, 20),

        "random_seed": 42,
        "verbose": 50,
        "early_stopping_rounds": 50,
        "allow_writing_files": False
    }
    # Suggest hyperparameters
    # params = {
    #         "loss_function": catboost_loss_functions,
    #         "eval_metric": eval_metric,
    #         "task_type": "GPU",
    #         "devices": "0,1",
    #         # narrower search around 1500..2100
    #         "iterations": trial.suggest_int("iterations", 1500, 2100, step=100),
    #         # narrower search for depth
    #         "depth": trial.suggest_int("depth", 5, 9),
    #         # narrower learning_rate in log scale from 0.15..0.3
    #         "learning_rate": trial.suggest_float("learning_rate", 0.15, 0.3, log=True),
    #         # smaller range for l2_leaf_reg, from 1..10
    #         "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1.0, 10.0),
    #         # only these two grow_policies
    #         "grow_policy": trial.suggest_categorical("grow_policy", ["Depthwise", "Lossguide"]),
    #         # random_strength from 2..4
    #         "random_strength": trial.suggest_float("random_strength", 2.0, 4.0),
    #         # min_data_in_leaf from 5..15
    #         "min_data_in_leaf": trial.suggest_int("min_data_in_leaf", 5, 15),
    #         "random_seed": 42,
    #         "verbose": 50,
    #         "early_stopping_rounds": 50,
    #         "allow_writing_files": False
    #     }

    # Build and train the model
    model = CatBoostRanker(**params)
    model.fit(train_pool, eval_set=valid_pool, early_stopping_rounds=100)

    # Evaluate with NDCG@1 or fallback
    score_dict = model.get_best_score()
    val_preds = model.predict(valid_pool)
    if "validation" in score_dict:
        metric_key = f"{eval_metric};type=Base"
        valid_ndcg_k = score_dict["validation"].get(metric_key, 0.0)
    else:
        # fallback if no 'validation' key
        true_vals = y_valid.reshape(1, -1)
        pred_vals = val_preds.reshape(1, -1)
        valid_ndcg_k = ndcg_score(true_vals, pred_vals, k=1)

    return valid_ndcg_k


###############################################################################
# The run_optuna function
###############################################################################
def run_optuna(
    catboost_loss_functions,
    eval_metric,
    train_pool, valid_pool,
    y_valid,  # needed if we do manual fallback
    n_trials=20
):
    """Creates an Optuna study with direction='maximize', runs the objective, returns the study."""
    study = optuna.create_study(direction="maximize")

    def _objective(trial):
        return objective(trial, catboost_loss_functions, eval_metric, y_valid, train_pool, valid_pool)

    study.optimize(_objective, n_trials=n_trials)
    return study


###############################################################################
# Train final model & save (train+valid -> catboost_enriched_results)
###############################################################################
def train_and_save_model(
    catboost_loss_functions,
    eval_metric,
    best_params,
    train_pool,
    valid_pool,
    train_data,
    valid_data,
    spark,
    jdbc_url,
    jdbc_properties,
    db_table="catboost_enriched_results",  # Single final table
    save_dir="/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/catboost",
):
    """
    Train a final CatBoostRanker using best_params, then save:
    - Model file on disk
    - Train & validation predictions (rank) to catboost_enriched_results
    """
    timestamp = get_timestamp()
    model_key = f"{catboost_loss_functions}_{eval_metric}_{timestamp}"

    recognized_params = dict(best_params)
    recognized_params.pop("early_stopping_rounds", None)
    recognized_params["loss_function"] = catboost_loss_functions
    recognized_params["eval_metric"] = eval_metric
    recognized_params["random_seed"] = 42
    recognized_params["task_type"] = "GPU"

    final_model = CatBoostRanker(**recognized_params)
    final_model.fit(train_pool, verbose=100)

    # Save model to disk
    os.makedirs(save_dir, exist_ok=True)
    model_filename = f"catboost_{model_key}.cbm"
    model_path = os.path.join(save_dir, model_filename)
    final_model.save_model(model_path)
    print(f"Model saved to: {model_path}")

    # Predict on training data
    train_preds = final_model.predict(train_pool)
    train_labels = train_pool.get_label()

    # Predict on validation data
    valid_preds = final_model.predict(valid_pool)
    valid_labels = valid_pool.get_label()

    # Enrich training data
    enriched_train = train_data.copy()
    enriched_train["model_key"] = model_key
    enriched_train["prediction"] = train_preds
    enriched_train["true_label"] = train_labels

    # Enrich validation data
    enriched_valid = valid_data.copy()
    enriched_valid["model_key"] = model_key
    enriched_valid["prediction"] = valid_preds
    enriched_valid["true_label"] = valid_labels

    # Combine train & valid for DB
    combined_data = pd.concat([enriched_train, enriched_valid], ignore_index=True)

    # Sort + rank per group_id
    combined_data = combined_data.sort_values(by=["group_id", "prediction"], ascending=[True, False])
    combined_data["rank"] = combined_data.groupby("group_id").cumcount() + 1

    # Check for duplicates in combined_data
    def check_duplicates(data, subset_cols):
        duplicates = data[data.duplicated(subset=subset_cols, keep=False)]
        if not duplicates.empty:
            print(f"Found duplicates in combined_data based on columns {subset_cols}:")
            print(duplicates)
        else:
            print(f"No duplicates found in combined_data based on columns {subset_cols}.")

    # Check for duplicates based on 'horse_id' and 'group_id'
    check_duplicates(combined_data, ["horse_id", "group_id"])

    # Write to DB (append mode)
    try:
        spark_df = spark.createDataFrame(combined_data)
        (
            spark_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", db_table)  # catboost_enriched_results
            .option("user", jdbc_properties["user"])
            .option("driver", jdbc_properties["driver"])
            .mode("append")
            .save()
        )
        print(f"Appended {len(combined_data)} rows to '{db_table}' (train+valid).")
    except Exception as e:
        print(f"Error saving train+valid predictions: {e}")
        raise

    return final_model, model_path, model_key, combined_data


###############################################################################
# Evaluate Model - Detailed Race-Level Output + Summary (holdout -> catboost_enriched_results)
###############################################################################
def evaluate_and_save_results(
    model_key: str,
    model_path: str,
    holdout_pool: Pool,
    holdout_group_id: np.ndarray,
    spark,
    db_url: str,
    db_properties: dict,
    db_table: str,               # now also catboost_enriched_results
    holdout_df: pd.DataFrame,
    metrics_output_path: str = "./data/training/holdout_metrics"):
    """
    Evaluates a trained CatBoost model on holdout data, merges with full race details,
    saves predictions + rank to catboost_enriched_results, and also saves metrics to JSON.
    """
    logging.info(f"Evaluating model: {model_key} from {model_path}")

    # 1) Load the trained CatBoost model
    final_model = CatBoostRanker()
    final_model.load_model(model_path)
    logging.info(f"Loaded CatBoost model: {model_path}")

    # 2) Predict on holdout data
    holdout_preds = final_model.predict(holdout_pool)
    holdout_labels = holdout_pool.get_label()

    # 3) Create holdout predictions
    holdout_predictions = pd.DataFrame({
        "model_key": model_key,
        "group_id": holdout_group_id,
        "prediction": holdout_preds,
        "true_label": holdout_labels,
        "horse_id": holdout_df["horse_id"].values  # Ensure horse_id is included for merging
    })

    holdout_merged = holdout_predictions.merge(
        holdout_df,
        on=["group_id", "horse_id"],  # Merge on both group_id and horse_id
        how="left"
    )

    logging.info(f"Final holdout_merged dataset size: {holdout_merged.shape}")

    # Convert race_date to str for Spark
    # holdout_merged["race_date"] = holdout_merged["race_date"].astype(str)

    # Sort by prediction descending, compute rank
    holdout_merged = holdout_merged.sort_values(by=["group_id", "prediction"], ascending=[True, False])
    holdout_merged["rank"] = holdout_merged.groupby("group_id").cumcount() + 1

    # 5) Compute Accuracy Metrics (top-4 accuracy, perfect order, NDCG@3)
    grouped = holdout_merged.groupby("group_id")
    top_4_accuracy_list = []
    perfect_order_count = 0
    total_groups = 0
    all_true_labels = []
    all_predictions = []

    for gid, group in grouped:
        if len(group) > 1:
            total_groups += 1
            group_sorted = group.sort_values("prediction", ascending=False).copy()

            # Predicted vs actual top-4
            top_4_predicted = group_sorted.head(4)["true_label"].values
            top_4_actual = group.sort_values("true_label", ascending=True).head(4)["true_label"].values

            correct_top_4 = len(np.intersect1d(top_4_predicted, top_4_actual))
            top_4_accuracy_list.append(correct_top_4 / 4.0)

            if np.array_equal(top_4_predicted, top_4_actual):
                perfect_order_count += 1

            # NDCG@3
            sorted_by_label = group.sort_values("true_label", ascending=True)
            true_labels_np = sorted_by_label["true_label"].values
            preds_np = group_sorted["prediction"].values
            all_true_labels.append(true_labels_np)
            all_predictions.append(preds_np)

    if total_groups > 0:
        avg_top_4_accuracy = float(np.mean(top_4_accuracy_list))
        perfect_order_percentage = float((perfect_order_count / total_groups) * 100)
        ndcg_values = [
            ndcg_score([t], [p], k=3)
            for t, p in zip(all_true_labels, all_predictions)
        ]
        avg_ndcg_3 = float(np.mean(ndcg_values))
    else:
        avg_top_4_accuracy = 0.0
        perfect_order_percentage = 0.0
        avg_ndcg_3 = 0.0
        logging.info("No valid groups found in holdout for evaluation.")

    metrics = {
        "model_key": model_key,
        "avg_top_4_accuracy": avg_top_4_accuracy,
        "perfect_order_percentage": perfect_order_percentage,
        "avg_ndcg_3": avg_ndcg_3,
        "total_groups_evaluated": total_groups
    }
    logging.info(f"Holdout Evaluation Results:\n{json.dumps(metrics, indent=2)}")

    # Check for duplicates in holdout_merged
    def check_duplicates(data, subset_cols):
        duplicates = data[data.duplicated(subset=subset_cols, keep=False)]
        if not duplicates.empty:
            logging.info(f"Found duplicates in holdout_merged based on columns {subset_cols}:")
            logging.info(duplicates)
        else:
            logging.info(f"No duplicates found in holdout_merged based on columns {subset_cols}.")

    # Check for duplicates based on 'horse_id' and 'group_id'
    check_duplicates(holdout_merged, ["horse_id", "group_id"])
    
    # 6) Write holdout predictions to the *same* catboost_enriched_results table
    try:
        spark_df = spark.createDataFrame(holdout_merged)
        spark_df.write.format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", db_table) \
            .option("user", db_properties["user"]) \
            .option("driver", db_properties["driver"]) \
            .mode("append") \
            .save()

        logging.info(f"Appended {len(holdout_merged)} holdout rows to DB table '{db_table}'.")
    except Exception as e:
        logging.error(f"Error saving holdout data to DB: {e}")
        raise

    # 7) Save metrics to JSON
    try:
        os.makedirs(metrics_output_path, exist_ok=True)
        metrics_path = os.path.join(metrics_output_path, f"holdout_metrics_{model_key}.json")

        with open(metrics_path, "w") as fp:
            json.dump(metrics, fp, indent=2)

        logging.info(f"Saved holdout metrics to {metrics_path}")
    except Exception as e:
        logging.error(f"Error saving metrics JSON: {e}")

    return metrics, holdout_merged


###############################################################################
# Main script that orchestrates everything - single table approach
###############################################################################
def main_script(
    spark,
    X_train, y_train, train_pool,
    X_valid, y_valid, valid_pool,
    holdout_pool, holdout_group_id,
    train_data, valid_data, holdout_df,
    catboost_loss_functions,
    catboost_eval_metrics,
    jdbc_url, jdbc_properties,
    db_table  # single final table: catboost_enriched_results
):
    """
    1) For each combination of catboost_loss_functions & catboost_eval_metrics:
       a) Run Optuna
       b) Train model -> catboost_enriched_results (train+valid)
       c) Evaluate holdout -> catboost_enriched_results
    2) Returns a dictionary of all trained models & best params.
    """
    all_models = {}
    for loss_func in catboost_loss_functions:
        for eval_met in catboost_eval_metrics:
            # timestamp = get_timestamp()
            # model_key = f"{loss_func}_{eval_met}_{timestamp}"
            # print(f"=== Starting Optuna for {model_key} ===")

            # 1) Run Optuna
            study = run_optuna(
                catboost_loss_functions=loss_func,
                eval_metric=eval_met,
                train_pool=train_pool,
                valid_pool=valid_pool,
                y_valid=y_valid,
                n_trials=20
            )

            best_score = study.best_value
            best_params = study.best_params
            logging.info(f"Best score: {best_score}, Best params: {best_params}")

            # 2) Train final model + save train+valid predictions -> catboost_enriched_results
            final_model, model_path, model_key, _ = train_and_save_model(
                catboost_loss_functions=loss_func,
                eval_metric=eval_met,
                best_params=best_params,
                train_pool=train_pool,
                valid_pool=valid_pool,
                train_data=train_data,
                valid_data=valid_data,
                spark=spark,
                jdbc_url=jdbc_url,
                jdbc_properties=jdbc_properties,
                db_table=db_table  # e.g. catboost_enriched_results
            )
            logging.info(f"Saved model with train+valid predictions to {model_path}")

            # 3) Evaluate on holdout -> catboost_enriched_results
            evaluate_and_save_results(
                model_key=model_key,
                model_path=model_path,
                holdout_pool=holdout_pool,
                holdout_group_id=holdout_group_id,
                spark=spark,
                db_url=jdbc_url,
                db_properties=jdbc_properties,
                db_table=db_table,     # same table: catboost_enriched_results
                holdout_df=holdout_df
            )

            all_models[model_key] = {
                "best_score": best_score,
                "best_params": best_params,
                "model_path": model_path
            }

    logging.info("=== Done training & evaluating all models ===")
    return all_models
    
def split_data_and_train(df, label_col, cat_cols, embed_cols, excluded_cols, jdbc_url, jdbc_properties, spark):
    """
    Splits the data into train/valid/holdout, builds CatBoost Pools, and calls main_script(...).
    """
    # Split data into train/valid/holdout
    # Splits
    train_end_date = pd.to_datetime("2023-12-31")
    valid_data_end_date = pd.to_datetime("2024-06-30")
    holdout_start = pd.to_datetime("2024-07-01")

    train_data = df[df["race_date"] <= train_end_date].copy()
    valid_data = df[(df["race_date"] > train_end_date) & (df["race_date"] <= valid_data_end_date)].copy()
    holdout_data = df[df["race_date"] >= holdout_start].copy()

    # Verify uniqueness of horse_id and group_id combination
    def check_combination_uniqueness(data, data_name):
        combination_unique = data[["horse_id", "group_id"]].drop_duplicates().shape[0] == data.shape[0]
        logging.info(f"{data_name} - horse_id and group_id combination unique: {combination_unique}")
        if not combination_unique:
            logging.info(f"Duplicate horse_id and group_id combination in {data_name}:")
            logging.info(data[data.duplicated(subset=["horse_id", "group_id"], keep=False)])

    check_combination_uniqueness(train_data, "train_data")
    check_combination_uniqueness(valid_data, "valid_data")
    check_combination_uniqueness(holdout_data, "holdout_data")

    # Build the final feature list
    all_cols = df.columns.tolist()
    # Exclude text columns + label col
    base_feature_cols = [c for c in all_cols if c not in excluded_cols]
    # Force-include cat_cols + embed_cols
    final_feature_cols = list(set(base_feature_cols + cat_cols + embed_cols))
    final_feature_cols.sort()

    logging.info(f"Final feature columns for training:\n{final_feature_cols}")

    # Saving the final feature columns to a file so I can make sure inference uses the same columns in the same order
    with open("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/training/final_feature_cols.json", "w") as f:
        json.dump(final_feature_cols, f)
    logging.info("Saved final_feature_cols to final_feature_cols.json")

    # Build X,y for train
    X_train = train_data[final_feature_cols].copy()
    y_train = train_data[label_col].values
    train_group_id = train_data["group_id"].values

    # Build X,y for valid
    X_valid = valid_data[final_feature_cols].copy()
    y_valid = valid_data[label_col].values
    valid_group_id = valid_data["group_id"].values

    # Build X,y for holdout
    X_holdout = holdout_data[final_feature_cols].copy()
    y_holdout = holdout_data[label_col].values
    holdout_group_id = holdout_data["group_id"].values

    cat_features_in_data = [c for c in X_train.columns if X_train[c].dtype == "category"]
   
    # Build CatBoost Pools
    train_pool = Pool(X_train, label=y_train, group_id=train_group_id, cat_features=cat_features_in_data)
    valid_pool = Pool(X_valid, label=y_valid, group_id=valid_group_id, cat_features=cat_features_in_data)
    holdout_pool = Pool(X_holdout, label=y_holdout, group_id=holdout_group_id, cat_features=cat_features_in_data)

    catboost_loss_functions = [
        "YetiRank:top=1",
        "YetiRank:top=2",
        "YetiRank:top=3",
        "YetiRank:top=4",
        "YetiRankPairwise"
    ]
    catboost_eval_metrics = ["NDCG:top=1", "NDCG:top=2", "NDCG:top=3", "NDCG:top=4"]

    # single final table
    db_table = "catboost_enriched_results"

    all_models = main_script(
        spark,
        X_train, y_train, train_pool,
        X_valid, y_valid, valid_pool,
        holdout_pool, holdout_group_id,
        train_data, valid_data, holdout_data,
        catboost_loss_functions=catboost_loss_functions,
        catboost_eval_metrics=catboost_eval_metrics,
        jdbc_url=jdbc_url,
        jdbc_properties=jdbc_properties,
        db_table=db_table
    )

    # Save all_models
    save_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/all_models.json"
    with open(save_path, "w") as fp:
        json.dump(all_models, fp, indent=2)
    print(f"Saved all_models to {save_path}")

    print("Done building and evaluating CatBoost models.")
    return all_models

###############################################################################
# Build catboost model - single table approach
###############################################################################
def build_catboost_model(spark, horse_embedding, jdbc_url, jdbc_properties, action):
    import logging
    from pyspark.sql.functions import col

    logging.info(f"Schema of horse_embedding_df: {horse_embedding.schema}")
    logging.info(f"Horse_embedding total count: {horse_embedding.count()}")

    historical_spark = horse_embedding.filter(col("data_flag") != "future")
    future_spark     = horse_embedding.filter(col("data_flag") == "future")

    hist_count = historical_spark.count()
    fut_count  = future_spark.count()
    logging.info(f"Row count (historical): {hist_count}")
    logging.info(f"Row count (future): {fut_count}")
    # For training
    if action == "train":
        hist_pdf, cat_cols, excluded_cols = transform_horse_df_to_pandas(historical_spark, drop_label=False)
        logging.info(f"Shape of historical Pandas DF: {hist_pdf.shape}")
                
        split_data_and_train(
            hist_pdf,
            label_col="perf_target",
            cat_cols=cat_cols,
            embed_cols=[f"embed_{i}" for i in range(4)],
            excluded_cols=excluded_cols,
            jdbc_url=jdbc_url,
            jdbc_properties=jdbc_properties,
            spark=spark
        )

    # For inference
    elif action == "predict":
        # Returns a Pandas DataFrame and cat_cols
        fut_pdf, cat_cols, excluded_cols = transform_horse_df_to_pandas(future_spark, drop_label=True)
        logging.info(f"Shape of future Pandas DF: {fut_pdf.shape}")
        main_inference(spark, fut_pdf, cat_cols, excluded_cols, jdbc_url, jdbc_properties)

    else:
        logging.info("Invalid action. Please select 'train' or 'predict'.")

def transform_horse_df_to_pandas(df_spark, drop_label=False):
    """
    1) Convert df_spark to Pandas.
    2) Drop 'official_fin' if present.
    3) Create race_id, group_id, convert date columns, etc.
    4) Return the transformed Pandas DataFrame.
    """
    pdf = df_spark.toPandas()

    # Convert to Panda DataFrame
    # If present, drop "official_fin"
    pdf.drop(columns=["official_fin"], inplace=True, errors="ignore")

    # Create race_id + group_id
    pdf["race_id"] = (
        pdf["course_cd"].astype(str)
        + "_"
        + pdf["race_date"].astype(str)
        + "_"
        + pdf["race_number"].astype(str)
    )
    #Create group_id and sort ascending
    pdf["group_id"] = pdf["race_id"].astype("category").cat.codes
    pdf = pdf.sort_values("group_id", ascending=True).reset_index(drop=True)

    # Convert selected datetime columns to numeric
    datetime_columns = ["first_race_date_5", "most_recent_race_5", "prev_race_date"]
    new_numeric_cols = {}
    for col in datetime_columns:
        pdf[col] = pd.to_datetime(pdf[col])
        new_numeric_cols[col + "_numeric"] = (pdf[col] - pd.Timestamp("1970-01-01")).dt.days
    # Drop the original datetime columns
    pdf.drop(columns=datetime_columns, inplace=True, errors="ignore")
    pdf = pd.concat([pdf, pd.DataFrame(new_numeric_cols, index=pdf.index)], axis=1)

    # Convert main race_date to datetime
    pdf["race_date"] = pd.to_datetime(pdf["race_date"])

    # Optionally drop the label if you want. 
    # e.g. if you're purely doing inference, you might remove perf_target.
    # Or just keep it if you need it for other checks.
    if drop_label:
        pdf.drop(columns=["perf_target"], inplace=True, errors="ignore")

    # Make certain columns categorical
    cat_cols = ["course_cd", "trk_cond", "sex", "equip", "surface", "med",
                "race_type", "stk_clm_md", "turf_mud_mark", "layoff_cat","previous_surface"]
    
    excluded_cols = ["horse_name", "saddle_cloth_number", "axciskey", "race_date", 
                     "race_number", "horse_id","horse_idx_x", "horse_idx_y", "race_id", 
                     "track_name", "saddle_cloth_number", "perf_target", "data_flag"]

    # Convert to categorical data type
    for c in cat_cols:
        if c in pdf.columns:
            pdf[c] = pdf[c].astype("category")
            
    # Return the transformed Pandas DataFrame, along with cat_cols and excluded_cols
    return pdf, cat_cols, excluded_cols