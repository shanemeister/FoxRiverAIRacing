import io
import json
import os
import logging
from datetime import datetime
import pandas as pd
import numpy as np
import optuna
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col
from catboost import CatBoostRanker, Pool, CatBoostError
from src.data_preprocessing.data_prep1.data_utils import save_parquet
from src.train_and_predict.final_predictions import main_inference
from sklearn.metrics import mean_squared_error, mean_absolute_error, ndcg_score
import scipy.stats as stats


###############################################################################
# Helper function to get a timestamp for filenames
###############################################################################



def get_timestamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")
###############################################################################
# Helper function: The Optuna objective for ranking
###############################################################################
def objective(trial, catboost_loss_functions, eval_metric, y_valid, train_pool, valid_pool):
    
    # Log the chosen parameters for this trial.
    logging.info(f"Trial {trial.number} parameters: loss_function={catboost_loss_functions}, eval_metric={eval_metric}, "
                 f"iterations={trial.suggest_int('iterations', 1000, 5000, step=100)}")

    params = {
        "loss_function": catboost_loss_functions,
        "eval_metric": eval_metric,
        "task_type": "GPU",
        "devices": "0,1",
        
        # Number of boosting iterations. Allow a large range.
        "iterations": trial.suggest_int("iterations", 1000, 5000, step=100),
        
        # Depth of trees – try a broader range.
        "depth": trial.suggest_int("depth", 4, 12),
        
        # Learning rate with a logarithmic scale.
        "learning_rate": trial.suggest_float("learning_rate", 1e-4, 0.3, log=True),
        
        # L2 leaf regularization.
        "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 0.1, 20.0, log=True),
        
        # Grow policy: Depthwise, Lossguide, or SymmetricTree.
        "grow_policy": trial.suggest_categorical("grow_policy", ["Depthwise", "Lossguide", "SymmetricTree"]),
        
        # Random strength, which can help with overfitting.
        "random_strength": trial.suggest_float("random_strength", 0.1, 10.0, log=True),
        
        # Minimum number of data points in a leaf.
        "min_data_in_leaf": trial.suggest_int("min_data_in_leaf", 1, 30),
        
        # Bagging temperature can affect randomness in sampling.
        "bagging_temperature": trial.suggest_float("bagging_temperature", 0.0, 1.0, step=0.1),
        
        # Border count controls the discretization of continuous features.
        "border_count": trial.suggest_int("border_count", 1, 255),
        
        # Overfitting detector type and waiting rounds.
        "od_type": trial.suggest_categorical("od_type", ["IncToDec", "Iter"]),
        
        "random_seed": 42,
        "verbose": 50,
        "early_stopping_rounds": trial.suggest_int("early_stopping_rounds", 50, 200, step=10),
        "allow_writing_files": False
    }
    
    from catboost import CatBoostRanker
    model = CatBoostRanker(**params)
    model.fit(train_pool, eval_set=valid_pool, early_stopping_rounds=params["early_stopping_rounds"])
    
    # Get best score from validation. We expect our eval_metric (NDCG) to be maximized.
    score_dict = model.get_best_score()
    val_preds = model.predict(valid_pool)
    
    if "validation" in score_dict:
        metric_key = f"{eval_metric};type=Base"
        valid_ndcg = score_dict["validation"].get(metric_key, 0.0)
    else:
        # Fallback: compute NDCG manually (requires at least 2 documents per group).
        from sklearn.metrics import ndcg_score
        true_vals = y_valid.reshape(1, -1)
        pred_vals = val_preds.reshape(1, -1)
        valid_ndcg = ndcg_score(true_vals, pred_vals, k=4)
    
    return valid_ndcg

###############################################################################
# The run_optuna function
###############################################################################
def run_optuna(catboost_loss_functions, eval_metric, y_valid, train_pool, valid_pool, n_trials=100):
    """Creates an Optuna study and runs the objective to maximize NDCG (higher is better)."""
    import optuna
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
        print("Table read successfully. Row count:", spark_df.count())
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
    
    # 6) Compute metrics
    # Initialize lists to collect per-race metrics.
    top4_accuracy_list = []
    top1_accuracy_list = []
    top2_accuracy_list = []
    perfect_order_count = 0
    reciprocal_ranks = []
    prediction_std_devs = []
    all_true_vals = []
    all_pred_vals = []
    all_true_labels = []  # For computing per-race NDCG.
    all_predictions = []  # For computing per-race NDCG.
    total_groups = 0

        # Assume holdout_merged is your Pandas DataFrame with columns:
        # "group_id", "true_label", "prediction", "horse_id"
    for gid, group in holdout_merged.groupby("group_id"):
        # We need at least 2 horses per race for meaningful ranking.
        if len(group) < 2:
            continue
        total_groups += 1
        
        # Sort predictions in descending order (higher score = better rank)
        group_sorted_pred = group.sort_values("prediction", ascending=False).reset_index(drop=True)
        # Sort true labels in ascending order if lower finishing position means better finish.
        # (Adjust if your convention is different.)
        group_sorted_true = group.sort_values("true_label", ascending=True).reset_index(drop=True)
        
        # Top-4 Accuracy: Compare the top 4 predicted true_label values to the official top 4.
        top4_predicted = group_sorted_pred.head(4)["true_label"].values
        top4_actual = group_sorted_true.head(4)["true_label"].values
        correct_top4 = len(np.intersect1d(top4_predicted, top4_actual))
        top4_accuracy_list.append(correct_top4 / 4.0)
        
        # Perfect Order: If the top-4 predicted (by true_label) exactly match the official top-4 order.
        # (Here, order is compared; if you want to ignore order, you can compare sets.)
        if np.array_equal(top4_predicted, top4_actual):
            perfect_order_count += 1

        # Top-1 Accuracy: Check if the top predicted horse (by score) is the official winner.
        top1_pred = group_sorted_pred.iloc[0]["horse_id"]
        top1_true = group_sorted_true.iloc[0]["horse_id"]
        top1_accuracy_list.append(1 if top1_pred == top1_true else 0)
        
        # Top-2 Accuracy: Check if the official winner appears in the top 2 predictions.
        top2_pred_ids = group_sorted_pred.head(2)["horse_id"].values
        top2_accuracy_list.append(1 if top1_true in top2_pred_ids else 0)
        
        # MRR: Find the rank position (starting at 1) where the official winner appears.
        try:
            rank = group_sorted_pred.index[group_sorted_pred["horse_id"] == top1_true][0] + 1
        except IndexError:
            rank = None
        reciprocal_ranks.append(1 / rank if rank is not None else 0)
        
        # Prediction variance: Standard deviation of predictions within the race.
        prediction_std_devs.append(group_sorted_pred["prediction"].std())
        
        # Collect all true and predicted values (for global error metrics).
        all_true_vals.extend(group["true_label"].values)
        all_pred_vals.extend(group["prediction"].values)
        
        # Also collect per-race arrays for NDCG calculation.
        all_true_labels.append(group_sorted_true["true_label"].values)
        all_predictions.append(group_sorted_pred["prediction"].values)

    # Compute aggregate metrics.
    avg_top4_accuracy = float(np.mean(top4_accuracy_list))
    avg_top1_accuracy = float(np.mean(top1_accuracy_list))
    avg_top2_accuracy = float(np.mean(top2_accuracy_list))
    perfect_order_percentage = float(perfect_order_count / total_groups)  # As a fraction (multiply by 100 if percentage desired)
    MRR = float(np.mean(reciprocal_ranks))
    prediction_variance = float(np.mean(prediction_std_devs))
    RMSE = float(np.sqrt(mean_squared_error(np.array(all_true_vals), np.array(all_pred_vals))))
    MAE = float(mean_absolute_error(np.array(all_true_vals), np.array(all_pred_vals)))
    spearman_corr = float(stats.spearmanr(all_true_vals, all_pred_vals)[0])
    ndcg_values = [
        ndcg_score([t], [p], k=3)
        for t, p in zip(all_true_labels, all_predictions)
    ]
    avg_ndcg_3 = float(np.mean(ndcg_values))

    # Build the metrics dictionary.
    metrics = {
        "model_key": model_key,  # assuming model_key is defined elsewhere
        "avg_top_4_accuracy": avg_top4_accuracy,
        "avg_top_1_accuracy": avg_top1_accuracy,
        "avg_top_2_accuracy": avg_top2_accuracy,
        "perfect_order_percentage": perfect_order_percentage,
        "avg_ndcg_3": avg_ndcg_3,
        "MRR": MRR,
        "spearman_corr": spearman_corr,
        "RMSE": RMSE,
        "MAE": MAE,
        "prediction_variance": prediction_variance,
        "total_groups_evaluated": total_groups
    }

    print(metrics)
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
        print("Table read successfully. Row count:", spark_df.count())
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
                n_trials=100
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
    
def split_data_and_train(
    df, 
    label_col, 
    cat_cols,  # if you're excluding cats entirely, you can skip these
    embed_cols,
    excluded_cols,
    jdbc_url, 
    jdbc_properties, 
    spark
):
    """
    Splits the data into train/valid/holdout, 
    builds a minimal CatBoost Pools, and calls main_script(...).
    """

    # 1) Do NOT drop race_date, horse_id, group_id yet—split on the original df
    train_end_date = pd.to_datetime("2023-12-31")
    valid_data_end_date = pd.to_datetime("2024-06-30")
    holdout_start = pd.to_datetime("2024-07-01")

    train_data = df[df["race_date"] <= train_end_date].copy()
    valid_data = df[(df["race_date"] > train_end_date) & (df["race_date"] <= valid_data_end_date)].copy()
    holdout_data = df[df["race_date"] >= holdout_start].copy()

    # 2) Uniqueness check
    def check_combination_uniqueness(data, data_name):
        combination_unique = data[["horse_id", "group_id"]].drop_duplicates().shape[0] == data.shape[0]
        logging.info(f"{data_name} - horse_id and group_id combination unique: {combination_unique}")
        if not combination_unique:
            logging.info(f"Duplicate horse_id+group_id in {data_name}:")
            logging.info(data[data.duplicated(["horse_id", "group_id"], keep=False)])

    check_combination_uniqueness(train_data, "train_data")
    check_combination_uniqueness(valid_data, "valid_data")
    check_combination_uniqueness(holdout_data, "holdout_data")

    # 3) Hardcode or load your numeric+embedding final features
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


    # 4) Save them to JSON if needed
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/training/final_feature_cols_{now_str}.json"
    with open(filename, "w") as f:
        json.dump(final_feature_cols, f)

    # 5) Build X,y for each split, referencing final_feature_cols
    X_train = train_data[final_feature_cols].copy()
    y_train = train_data[label_col].values
    train_group_id = train_data["group_id"].values

    X_valid = valid_data[final_feature_cols].copy()
    y_valid = valid_data[label_col].values
    valid_group_id = valid_data["group_id"].values

    X_holdout = holdout_data[final_feature_cols].copy()
    y_holdout = holdout_data[label_col].values
    holdout_group_id = holdout_data["group_id"].values

    # 6) Build Pools
    # If no cat features, cat_features=[] or omit
    train_pool = Pool(X_train, label=y_train, group_id=train_group_id)
    valid_pool = Pool(X_valid, label=y_valid, group_id=valid_group_id)
    holdout_pool = Pool(X_holdout, label=y_holdout, group_id=holdout_group_id)

    # 7) minimal catboost training
    catboost_loss_functions = [
        # "YetiRank:top=1",
        # "YetiRank:top=2",
        # "YetiRank:top=3",
        "YetiRank:top=4",
        "QueryRMSE"
    ]
    catboost_eval_metrics = ["NDCG:top=1", "NDCG:top=2", "NDCG:top=3", "NDCG:top=4"]

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

    # 3) Save final_feature_cols with a timestamp to avoid overwriting
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    outpath = f"/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/training/final_feature_cols_{now_str}.json"
    with open(outpath, "w") as fp:
        json.dump(final_feature_cols, fp)
    logging.info(f"Saved final_feature_cols to {outpath}")

    # # Save
    # save_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/all_models.json"
    # with open(save_path, "w") as fp:
    #     json.dump(all_models, fp, indent=2)
    # print(f"Saved all_models to {save_path}")

    print("Done building minimal CatBoost model.")
    return all_models

###############################################################################
# Build catboost model - single table approach
###############################################################################
def build_catboost_model(spark, horse_embedding, jdbc_url, jdbc_properties, action):
    
    print("DEBUG: datetime is =>", datetime)
        
    # Then use Pandas boolean indexing
    historical_pdf = horse_embedding[horse_embedding["data_flag"] != "future"].copy()
    future_pdf = horse_embedding[horse_embedding["data_flag"] == "future"].copy()
    
    # Transform the Spark DataFrame to Pandas.
    if action == "train":
        hist_pdf, cat_cols, excluded_cols = transform_horse_df_to_pandas(historical_pdf, drop_label=False)
        logging.info(f"Shape of historical Pandas DF: {hist_pdf.shape}")
        
        hist_pdf = assign_piecewise_log_labels(hist_pdf)
        logging.info(f"Shape of historical Pandas DF: {hist_pdf.shape}")
        
        # Update your embed_cols list to use the new combined features.
        embed_cols = [f"combined_{i}" for i in range(5)]
        
        split_data_and_train(
            hist_pdf,
            label_col="relevance",
            cat_cols=cat_cols,
            embed_cols=embed_cols,
            excluded_cols=excluded_cols,
            jdbc_url=jdbc_url,
            jdbc_properties=jdbc_properties,
            spark=spark
        )

    elif action == "predict":
        fut_pdf, cat_cols, excluded_cols = transform_horse_df_to_pandas(future_pdf, drop_label=True)
        logging.info(f"Shape of future Pandas DF: {fut_pdf.shape}")
        
        # Typically no official_fin => can’t create relevance
        embed_cols = [f"combined_{i}" for i in range(5)]
        
        # Then call your inference function:
        main_inference(
            spark=spark,
            cat_cols=cat_cols,
            excluded_cols=excluded_cols,
            fut_pdf=fut_pdf,
            jdbc_url=jdbc_url,
            jdbc_properties=jdbc_properties
        )
    else:
        logging.info("Invalid action. Please select 'train' or 'predict'.")

    return horse_embedding

def transform_horse_df_to_pandas(pdf_df, drop_label=False):
    """
    1) Convert df_spark to Pandas.
    2) 'official_fin' Transform the Label to “Bigger = Better” log-based or exponential-based formula to positions ≥ 5 to make them much less relevant
    3) Create race_id, group_id, convert date columns, etc.
    4) Return the transformed Pandas DataFrame.
    """

    # Check the dtypes in Pandas (should show datetime64[ns] for the above columns)
    # Now you can safely convert to Pandas
    pdf = pdf_df.copy()
    print(pdf.dtypes)  # Check that your date columns now show as datetime64[ns]

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

    # # Make certain columns categorical
    # cat_cols = ["course_cd", "trk_cond", "sex", "equip", "surface", "med",
    #             "race_type", "stk_clm_md", "turf_mud_mark", "layoff_cat","previous_surface"]
    cat_cols = []
    
    excluded_cols = [
        "axciskey",         # Raw identifier
        "official_fin",     # Target column
        "relevance", 
        "horse_id",         # Original horse ID (we use the mapped horse_idx instead)
        "horse_name",       # Not used for prediction
        "race_date",        # Raw date; if not engineered further
        "race_number",      # Race identifier, not a predictor
        "saddle_cloth_number",  # Race-specific identifier
        "race_id",          # Unique race identifier
        "date_of_birth",
        "time_behind",
        "track_name",       # Metadata
        "data_flag",        # Used for filtering only
        "group_id",         # Grouping information, not a feature
        "global_speed_score",  # Processed separately (e.g. via embeddings
    ]

    # Convert to categorical data type
    # for c in cat_cols:
    #     if c in pdf.columns:
    #         pdf[c] = pdf[c].astype("category")      
    # # Return the transformed Pandas DataFrame, along with cat_cols and excluded_cols
    return pdf, cat_cols, excluded_cols

def assign_piecewise_log_labels(df):
    """
    If official_fin <= 4, assign them to a high relevance band (with small differences).
    Otherwise, use a log-based formula that drops more sharply.
    """
    def _relevance(fin):
        if fin == 1:
            return 40  # Could be 40 for 1st
        elif fin == 2:
            return 38  # Slightly lower than 1st, but still high
        elif fin == 3:
            return 36
        elif fin == 4:
            return 34
        else:
            # For 5th place or worse, drop off fast with a log formula:
            # e.g. alpha=30, beta=4 => 5th => 30 / log(4+5)=30/log(9)= ~9
            alpha = 30.0
            beta  = 4.0
            return alpha / np.log(beta + fin)
    
    df["relevance"] = df["official_fin"].apply(_relevance)
    return df