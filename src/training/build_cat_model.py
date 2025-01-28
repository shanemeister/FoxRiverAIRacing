import io
import json
import os
import logging
import datetime
import pandas as pd
import numpy as np
import optuna
from catboost import CatBoostRanker, Pool
from sklearn.metrics import ndcg_score

from src.data_preprocessing.data_prep1.data_utils import save_parquet

###############################################################################
# Helper function to get a timestamp for filenames
###############################################################################
def get_timestamp():
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

###############################################################################
# Helper function: The Optuna objective for ranking
###############################################################################
def objective(trial, catboost_loss_function, eval_metric, y_valid, train_pool, valid_pool):
    """
    An Optuna objective function that trains a CatBoostRanker on train_pool,
    evaluates on valid_pool. Returns a validation metric (NDCG or similar) to maximize.
    """
    logging.info(f"Starting Trial {trial.number}: {catboost_loss_function}, {eval_metric}")

    # Suggest hyperparameters
    params = {
        "loss_function": catboost_loss_function,
        "eval_metric": eval_metric,
        "task_type": "GPU",
        "devices": "0,1",  # or "0,1" if you have multiple GPUs
        "iterations": trial.suggest_int("iterations", 500, 2000),
        "depth": trial.suggest_int("depth", 4, 10),
        "learning_rate": trial.suggest_float("learning_rate", 1e-3, 0.3, log=True),
        "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1.0, 10.0),
        "random_seed": 42,
        "verbose": 100
    }

    model = CatBoostRanker(**params)
    model.fit(
        train_pool,
        eval_set=valid_pool,
        early_stopping_rounds=100
    )

    # Check best score
    score_dict = model.get_best_score()
    # If the metric is NDCG, it's typically stored under "validation" => "NDCG:top=..."
    # For manual fallback:
    val_preds = model.predict(valid_pool)
    if "validation" in score_dict:
        # For example: "NDCG:top=1;type=Base", "NDCG:top=3;type=Base", etc.
        metric_key = f"{eval_metric};type=Base"
        valid_ndcg_k = score_dict["validation"].get(metric_key, 0.0)
    else:
        # Fallback: compute NDCG@1 or something similar
        true_vals = y_valid.values.reshape(1, -1)
        pred_vals = val_preds.reshape(1, -1)
        valid_ndcg_k = ndcg_score(true_vals, pred_vals, k=1)
    return valid_ndcg_k

###############################################################################
# The run_optuna function
###############################################################################
def run_optuna(
    catboost_loss_function,
    eval_metric,
    train_pool, valid_pool,
    y_valid,  # needed if we do manual fallback
    n_trials=20
):
    """
    Creates an Optuna study with direction='maximize',
    runs the objective, returns the study.
    """
    study = optuna.create_study(direction="maximize")

    def _objective(trial):
        return objective(trial, catboost_loss_function, eval_metric, y_valid, train_pool, valid_pool)

    study.optimize(_objective, n_trials=n_trials)
    return study

###############################################################################
# Train final model & save
###############################################################################
def train_and_save_model(
    catboost_loss_function,
    eval_metric,
    best_params,
    train_pool,
    save_dir="/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/catboost"
):
    """
    Train a final CatBoostRanker using best_params, then save model to disk.
    """
    recognized_params = dict(best_params)
    # Clean up if 'early_stopping_rounds' is in best_params
    recognized_params.pop("early_stopping_rounds", None)

    recognized_params["loss_function"] = catboost_loss_function
    recognized_params["eval_metric"] = eval_metric
    recognized_params["random_seed"] = 42
    recognized_params["task_type"] = "GPU"

    final_model = CatBoostRanker(**recognized_params)
    final_model.fit(train_pool, verbose=100)

    os.makedirs(save_dir, exist_ok=True)
    timestamp = get_timestamp()
    model_filename = f"catboost_{catboost_loss_function.replace(':', '_')}_{eval_metric.replace(':', '_')}_{timestamp}.cbm"
    model_path = os.path.join(save_dir, model_filename)
    final_model.save_model(model_path)

    print(f"Model saved to: {model_path}")
    return final_model, model_path

###############################################################################
# Evaluate Model - Detailed Race-Level Output + Summary
###############################################################################
def evaluate_and_save_results(
    model_key: str,
    model_path: str,
    holdout_pool: Pool,
    holdout_group_id: np.ndarray,
    spark,
    db_url: str,
    db_properties: dict,
    db_table: str
):
    """
    Loads a CatBoost model, predicts on holdout_pool, computes top-4 accuracy,
    top-4 order accuracy, NDCG, and saves detailed results (ranks, scores, etc.)
    to a database table for each race/group.
    """
    print(f"Evaluating model: {model_key} from {model_path}")
    final_model = CatBoostRanker()
    final_model.load_model(model_path)
    print(f"Loaded CatBoost model: {model_path}")

    # Predict
    holdout_preds = final_model.predict(holdout_pool)
    holdout_labels = holdout_pool.get_label()

    # Combine
    holdout_data = pd.DataFrame({
        "model_key": model_key,
        "group_id": holdout_group_id,
        "prediction": holdout_preds,
        "true_label": holdout_labels
    })

    grouped = holdout_data.groupby("group_id")
    detailed_output = []
    top_4_accuracy_list = []
    perfect_order_count = 0
    total_groups = 0
    all_true_labels = []
    all_predictions = []

    for gid, group in grouped:
        if len(group) > 1:
            total_groups += 1
            group_sorted = group.sort_values("prediction", ascending=False).copy()
            group_sorted["rank"] = range(1, len(group_sorted) + 1)

            top_4_predicted = group_sorted.head(4)["true_label"].values
            top_4_actual = group.sort_values("true_label", ascending=True).head(4)["true_label"].values

            correct_top_4 = len(np.intersect1d(top_4_predicted, top_4_actual))
            top_4_accuracy_list.append(correct_top_4 / 4.0)

            if np.array_equal(top_4_predicted[:4], top_4_actual[:4]):
                perfect_order_count += 1

            # For NDCG
            sorted_by_label = group.sort_values("true_label", ascending=True)
            true_labels_np = sorted_by_label["true_label"].values
            preds_np = group_sorted["prediction"].values

            all_true_labels.append(true_labels_np)
            all_predictions.append(preds_np)

            detailed_output.append(group_sorted[["model_key","group_id","prediction","true_label","rank"]])
        else:
            print(f"Skipping group {gid} with only 1 item in holdout.")

    if detailed_output:
        detailed_df = pd.concat(detailed_output, ignore_index=True)
    else:
        detailed_df = pd.DataFrame(columns=["model_key","group_id","prediction","true_label","rank"])

    if total_groups > 0:
        avg_top_4_accuracy = float(np.mean(top_4_accuracy_list))
        perfect_order_percentage = float((perfect_order_count / total_groups) * 100)
        ndcg_values = [
            ndcg_score([t], [p], k=3)
            for t, p in zip(all_true_labels, all_predictions)
        ]
        avg_ndcg = float(np.mean(ndcg_values))
    else:
        avg_top_4_accuracy = 0.0
        perfect_order_percentage = 0.0
        avg_ndcg = 0.0
        print("No valid groups found in holdout for evaluation.")

    results = {
        "model_key": model_key,
        "avg_top_4_accuracy": avg_top_4_accuracy,
        "perfect_order_percentage": perfect_order_percentage,
        "avg_ndcg_3": avg_ndcg,
        "total_groups_evaluated": total_groups
    }
    print("Evaluation Results:\n", json.dumps(results, indent=2))

    # rename columns for clarity
    detailed_df = detailed_df.rename(columns={"prediction": "score"})

    # Write detailed output to DB
    spark_df = spark.createDataFrame(detailed_df)
    (
        spark_df.write.format("jdbc")
        .option("url", db_url)
        .option("dbtable", db_table)
        .option("user", db_properties["user"])
        .option("driver", db_properties["driver"])
        .mode("append")  
        .save()
    )
    print(f"Appended {len(detailed_df)} rows of race-level output to DB table '{db_table}'.")

    # Optionally save a local JSON summary
    eval_metrics_path = f"./data/training/evaluation_metrics_{model_key}.json"
    os.makedirs(os.path.dirname(eval_metrics_path), exist_ok=True)
    with open(eval_metrics_path, "w") as fp:
        json.dump(results, fp, indent=2)
    print(f"Saved summary metrics to {eval_metrics_path}")

    return results, detailed_df

###############################################################################
# The main script that orchestrates everything
###############################################################################
def main_script(
    spark,
    X_train, y_train, train_pool,
    X_valid, y_valid, valid_pool,
    holdout_pool, holdout_group_id,
    catboost_loss_functions,
    catboost_eval_metrics,
    db_url, db_properties, db_table
):
    """
    1) For each combination of catboost_loss_functions & catboost_eval_metrics,
       run Optuna, get best_params, train final model, and evaluate on holdout.
    2) Save final results to DB.
    """
    all_models = {}
    for loss_func in catboost_loss_functions:
        for eval_met in catboost_eval_metrics:
            model_key = f"{loss_func}_{eval_met}"
            print(f"=== Starting Optuna for {model_key} ===")

            # 1) Run Optuna
            study = run_optuna(
                catboost_loss_function=loss_func,
                eval_metric=eval_met,
                train_pool=train_pool,
                valid_pool=valid_pool,
                y_valid=y_valid,
                n_trials=20
            )

            best_score = study.best_value
            best_params = study.best_params
            print(f"Best score: {best_score}, Best params: {best_params}")

            # 2) Train final model with best params
            final_model, model_path = train_and_save_model(
                catboost_loss_function=loss_func,
                eval_metric=eval_met,
                best_params=best_params,
                train_pool=train_pool
            )
            print(f"Saved model to {model_path}")

            # 3) Evaluate on holdout
            evaluate_and_save_results(
                model_key=model_key,
                model_path=model_path,
                holdout_pool=holdout_pool,
                holdout_group_id=holdout_group_id,
                spark=spark,
                db_url=db_url,
                db_properties=db_properties,
                db_table=db_table
            )

            # 4) Store results
            all_models[model_key] = {
                "best_score": best_score,
                "best_params": best_params,
                "model_path": model_path
            }

    print("=== Done training & evaluating all models ===")
    return all_models

###############################################################################
# The main function to build the catboost model from horse_embedding
###############################################################################
def build_catboost_model(spark, horse_embedding, db_url, db_properties, db_table):
    """
    1. Takes the horse_embedding Spark DF (with columns needed for training),
    2. Splits into train/valid/holdout,
    3. Creates CatBoost pools,
    4. Calls main_script(...) to run 8 different ranking configs with Optuna,
    5. Evaluates on holdout, saves detailed results to DB.
    """
    logging.info(f"Schema of horse_embedding_df: {horse_embedding.printSchema()}")
    logging.info(f"Horse_embedding count: {horse_embedding.count()}")

    # Convert to Pandas
    df = horse_embedding.toPandas()
    df.drop(columns=["official_fin"], inplace=True, errors="ignore")

    rows, cols = df.shape
    logging.info(f"Rows: {rows}, Columns: {cols}")
    logging.info(f"Columns: {df.columns.tolist()}")
    logging.info(f"Dtypes:\n{df.dtypes}")

    # Re-create group_id
    df["race_id"] = (
        df["course_cd"].astype(str) + "_" +
        df["race_date"].astype(str) + "_" +
        df["race_number"].astype(str)
    )
    df["group_id"] = df["race_id"].astype("category").cat.codes
    df = df.sort_values("group_id", ascending=True).reset_index(drop=True)

    # Convert some datetime columns to numeric
    datetime_columns = ["first_race_date_5", "most_recent_race_5", "prev_race_date"]
    new_numeric_cols = {}
    for col in datetime_columns:
        df[col] = pd.to_datetime(df[col])
        new_numeric_cols[col + "_numeric"] = (df[col] - pd.Timestamp("1970-01-01")).dt.days

    df.drop(columns=datetime_columns, inplace=True, errors="ignore")
    df = pd.concat([df, pd.DataFrame(new_numeric_cols, index=df.index)], axis=1)

    # Chronological splits
    df["race_date"] = pd.to_datetime(df["race_date"])
    train_end_date = pd.to_datetime("2023-12-31")
    valid_data_end_date = pd.to_datetime("2024-06-30")
    holdout_start = pd.to_datetime("2024-07-01")

    train_data = df[df["race_date"] <= train_end_date].copy()
    valid_data = df[(df["race_date"] > train_end_date) & (df["race_date"] <= valid_data_end_date)].copy()
    holdout_data = df[df["race_date"] >= holdout_start].copy()

    print(f"Train shape: {train_data.shape}")
    print(f"Valid shape: {valid_data.shape}")
    print(f"Holdout shape: {holdout_data.shape}")

    label_col = "perf_target"
    cat_cols = [
        "course_cd", "trk_cond", "sex", "equip", "surface", "med",
        "race_type", "stk_clm_md", "turf_mud_mark", "layoff_cat",
        "previous_surface"
    ]
    # Potential embed columns
    embed_cols = [f"embed_{i}" for i in range(64)]

    # Identify numeric columns
    def get_numeric_cols(df_):
        cand = [c for c in df_.columns if df_[c].dtype in [np.float64, np.int64]]
        cand = [c for c in cand if c != label_col]
        return cand

    numeric_cols_train = get_numeric_cols(train_data)
    for ec in embed_cols:
        if ec not in numeric_cols_train and ec in train_data.columns:
            numeric_cols_train.append(ec)

    if "group_id" in train_data.columns:
        train_data.sort_values("group_id", ascending=True, inplace=True)
    if "group_id" in valid_data.columns:
        valid_data.sort_values("group_id", ascending=True, inplace=True)
    if "group_id" in holdout_data.columns:
        holdout_data.sort_values("group_id", ascending=True, inplace=True)

    all_training_cols = numeric_cols_train + cat_cols
    all_training_cols = list(set(all_training_cols))

    # Build X,y for train
    X_train = train_data[all_training_cols].copy()
    y_train = train_data[label_col]
    train_group_id = train_data["group_id"] if "group_id" in train_data else np.zeros(len(train_data))

    # Build X,y for valid
    X_valid = valid_data[all_training_cols].copy()
    y_valid = valid_data[label_col]
    valid_group_id = valid_data["group_id"] if "group_id" in valid_data else np.zeros(len(valid_data))

    # Build X,y for holdout
    X_holdout = holdout_data[all_training_cols].copy()
    y_holdout = holdout_data[label_col]
    holdout_group_id = holdout_data["group_id"] if "group_id" in holdout_data else np.zeros(len(holdout_data))

    print(f"X_train shape: {X_train.shape}, y_train length: {len(y_train)}")
    print(f"X_valid shape: {X_valid.shape}, y_valid length: {len(y_valid)}")
    print(f"X_holdout shape: {X_holdout.shape}, y_holdout length: {len(y_holdout)}")

    # Create CatBoost Pools
    train_col_list = X_train.columns.tolist()
    cat_features_idx = [train_col_list.index(c) for c in cat_cols if c in train_col_list]

    train_pool = Pool(X_train, label=y_train, group_id=train_group_id, cat_features=cat_features_idx)
    valid_pool = Pool(X_valid, label=y_valid, group_id=valid_group_id, cat_features=cat_features_idx)
    holdout_pool = Pool(X_holdout, label=y_holdout, group_id=holdout_group_id, cat_features=cat_features_idx)

    # Now run the multi-model script
    catboost_loss_functions = [
        "YetiRank:top=1",
        "YetiRank:top=2",
        "YetiRank:top=3",
        "YetiRank:top=4",
        "QueryRMSE"
    ]
    catboost_eval_metrics = ["NDCG:top=1", "NDCG:top=2", "NDCG:top=3", "NDCG:top=4"]

    all_models = main_script(
        spark=spark,
        X_train=X_train, y_train=y_train, train_pool=train_pool,
        X_valid=X_valid, y_valid=y_valid, valid_pool=valid_pool,
        holdout_pool=holdout_pool, holdout_group_id=holdout_group_id,
        catboost_loss_functions=catboost_loss_functions,
        catboost_eval_metrics=catboost_eval_metrics,
        db_url=db_url,
        db_properties=db_properties,
        db_table="catboost_eval_results"
    )

    return all_models