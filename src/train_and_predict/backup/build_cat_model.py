import io
import json
import os
import re
import sys
import logging
import datetime
from datetime import datetime, date
import pandas as pd
import numpy as np
import optuna
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col
from catboost import CatBoostRanker, Pool, CatBoostError
from src.data_preprocessing.data_prep1.data_utils import save_parquet
from src.train_and_predict.final_predictions import main_inference
from sklearn.metrics import root_mean_squared_error, mean_squared_error, mean_absolute_error, ndcg_score
import scipy.stats as stats
from sklearn.model_selection import TimeSeriesSplit

###############################################################################
# Create a class to enable calculation of NDCG from the objective function 
###############################################################################

class DataSplits:
    def __init__(self, X_train, y_train, train_group_id, X_valid, y_valid, valid_group_id, X_holdout, y_holdout, holdout_group_id,
        train_data, valid_data, holdout_data, cat_cols):
        # build X, y, group_id for each split
        self.X_train = X_train
        self.y_train = y_train
        self.train_group_id = train_group_id
        self.X_valid = X_valid
        self.y_valid = y_valid
        self.valid_group_id = valid_group_id
        self.X_holdout= X_holdout
        self.y_holdout = y_holdout
        self.holdout_group_id = holdout_group_id
        self.train_data = train_data
        self.valid_data = valid_data
        self.holdout_data = holdout_data
        self.cat_cols = cat_cols
        # Create CatBoost Pools
        
        self.train_pool = Pool(
            data=self.X_train,
            label=self.y_train,
            group_id=self.train_group_id,
            cat_features=self.cat_cols
        )

        self.valid_pool = Pool(
            data=self.X_valid,
            label=self.y_valid,
            group_id=self.valid_group_id,
            cat_features=self.cat_cols
        )

        self.holdout_pool = Pool(
            data=self.X_holdout,
            label=self.y_holdout,
            group_id=self.holdout_group_id,
            cat_features=self.cat_cols
        )    
            
    def compute_ndcg(self, model, k=4):
        import numpy as np
        from sklearn.metrics import ndcg_score

        # 1) Generate predictions on the validation features
        val_preds = model.predict(self.X_valid)  # 'X_valid' matches your __init__ attribute

        # 2) For each group in self.valid_group_id
        all_ndcgs = []
        for gid in np.unique(self.valid_group_id):
            mask = (self.valid_group_id == gid)
            # skip if single doc
            if np.sum(mask) < 2:
                continue

            group_true = self.y_valid[mask].reshape(1, -1)   # must use self.y_valid
            group_preds = val_preds[mask].reshape(1, -1)
            group_ndcg = ndcg_score(group_true, group_preds, k=k)
            all_ndcgs.append(group_ndcg)

        return float(np.mean(all_ndcgs)) if all_ndcgs else 0.0  

###############################################################################
# Last Occurrence Carried Forward (LOCF) for future columns
###############################################################################   
def locf_across_hist_future(hist_df, fut_df, columns_to_locf, date_col="race_date"):
    """
    1) Concatenate hist_df + fut_df
    2) Sort by (horse_id, race_date)
    3) Group by horse_id and ffill the specified columns
    4) Slice out the 'future' rows again.

    Returns a new `fut_df_filled` with columns_to_locf carried forward 
    from historical values.
    """
    import pandas as pd

    # 1) Concatenate
    combined = pd.concat([hist_df, fut_df], ignore_index=True)

    # 2) Sort by horse_id + race_date
    combined.sort_values(by=["horse_id", date_col], inplace=True)

    # 3) Forward fill within each horse_id
    combined[columns_to_locf] = (
        combined.groupby("horse_id", group_keys=False)[columns_to_locf]
                .ffill()
    )

    # 4) Re-split out the future subset
    fut_df_filled = combined[combined["data_flag"] == "future"].copy()
    return fut_df_filled
###############################################################################
# Helper function to get a timestamp for filenames
###############################################################################
def get_timestamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")
###############################################################################
# Helper function: The Optuna objective for ranking
###############################################################################
def objective(trial, catboost_loss_functions, eval_metric, data_splits):
    logging.info(f"Trial {trial.number} with {catboost_loss_functions}, {eval_metric}")

    params = {
        "loss_function": catboost_loss_functions,
        "eval_metric": eval_metric,
        "task_type": "GPU",
        "devices": "0,1",
        "thread_count": 96,
        "iterations": trial.suggest_int("iterations", 1000, 5000, step=500),
        "depth": trial.suggest_int("depth", 4, 12),
        "learning_rate": trial.suggest_float("learning_rate", 1e-4, 0.3, log=True),
        "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1.0, 100.0, log=True),  # Increased range
        "grow_policy": trial.suggest_categorical("grow_policy", ["Depthwise", "Lossguide", "SymmetricTree"]),
        "random_strength": trial.suggest_float("random_strength", 1.0, 20.0, log=True),  # Increased range
        "min_data_in_leaf": trial.suggest_int("min_data_in_leaf", 1, 50),  # Increased range
        "bagging_temperature": trial.suggest_float("bagging_temperature", 0.0, 1.0, step=0.1),
        "border_count": trial.suggest_int("border_count", 1, 255),
        "od_type": trial.suggest_categorical("od_type", ["IncToDec", "Iter"]),
        "random_seed": 42,
        "verbose": 50,
        "early_stopping_rounds": trial.suggest_int("early_stopping_rounds", 50, 200, step=10),
        "allow_writing_files": False
    }
    
    
    model = CatBoostRanker(**params)
    model.fit(
        data_splits.train_pool, 
        eval_set=data_splits.valid_pool, 
        use_best_model=True, 
        early_stopping_rounds=params["early_stopping_rounds"]
    )
    
    # Compute NDCG@4 via your class method
    valid_ndcg = data_splits.compute_ndcg(model, k=4)
    return valid_ndcg

###############################################################################
# The run_optuna function
###############################################################################
def run_optuna(catboost_loss_functions, eval_metric, n_trials=20, data_splits=None):
    """Creates an Optuna study with SQLite storage and runs the objective to maximize NDCG (higher is better)."""
    import optuna

    # Define the storage URL and a study name.
    storage = "sqlite:///optuna_study.db"
    study_name = "catboost_optuna_top4_2"  # "catboost_optuna_study_rmse_min" #"catboost_optuna_study"

    # Create the study, loading existing trials if the study already exists.
    study = optuna.create_study(
        study_name=study_name,
        storage=storage,
        load_if_exists=True,
        direction="maximize"
    )
    
    def _objective(trial):
        return objective(trial, catboost_loss_functions, eval_metric, data_splits)
    
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
    enriched_train["relevance"] = train_labels

    # Enrich validation data
    enriched_valid = valid_data.copy()
    enriched_valid["model_key"] = model_key
    enriched_valid["prediction"] = valid_preds
    enriched_valid["relevance"] = valid_labels

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

import numpy as np
from sklearn.metrics import ndcg_score, mean_absolute_error
from scipy import stats

def compute_metrics(holdout_merged, model_key):
    """
    Computes ranking metrics for each race (group_id).
    Assumes:
      - 'group_id' identifies each race.
      - 'horse_id' uniquely identifies each horse in that race.
      - 'relevance' is higher for better-finishing horses (descending = best).
      - 'prediction' is the model's predicted score, higher = better rank.
    """
    
    top4_accuracy_list = []
    top1_accuracy_list = []
    top2_accuracy_list = []
    perfect_order_count = 0
    
    reciprocal_ranks = []
    prediction_std_devs = []
    
    all_true_vals = []
    all_pred_vals = []
    
    all_relevance = []     # For per-race true relevance arrays
    all_predictions = []   # For per-race predicted score arrays
    
    total_groups = 0

    # Group by race
    for gid, group in holdout_merged.groupby("group_id"):
        # If the race has <2 horses, some metrics don't make sense (top2, MRR, etc.)
        if len(group) < 2:
            continue
        
        total_groups += 1
        
        # Sort descending by prediction (higher score = better rank)
        group_sorted_pred = group.sort_values("prediction", ascending=False).reset_index(drop=True)
        # Sort descending by "relevance" (assuming bigger = better)
        group_sorted_true = group.sort_values("relevance", ascending=False).reset_index(drop=True)
        
        # ------------------
        # 1) Top-4 Accuracy
        # ------------------
        # Only compute if the race has at least 4 horses
        if len(group) >= 4:
            top4_pred_ids = group_sorted_pred.head(4)["horse_id"].values
            top4_true_ids = group_sorted_true.head(4)["horse_id"].values
            
            # Convert to sets for ignoring order
            pred_set_4 = set(top4_pred_ids)
            true_set_4 = set(top4_true_ids)
            
            correct_top4 = len(pred_set_4.intersection(true_set_4))  # how many of the correct top4 are identified
            top4_accuracy_list.append(correct_top4 / 4.0)
            
            # Check perfect order (exact sequence match)
            # np.array_equal is fine for comparing arrays element-wise
            if np.array_equal(top4_pred_ids, top4_true_ids):
                perfect_order_count += 1
        
        # ----------------
        # 2) Top-1 Accuracy
        # ----------------
        # Compare predicted winner vs. actual winner
        top1_pred = group_sorted_pred.iloc[0]["horse_id"]
        top1_true = group_sorted_true.iloc[0]["horse_id"]
        top1_accuracy_list.append(1 if top1_pred == top1_true else 0)
        
        # ----------------
        # 3) Top-2 Accuracy
        # ----------------
        # If you want the fraction of top2 that match ignoring order:
        # Convert to sets, measure intersection
        if len(group) >= 2:
            pred_ids_top2 = set(group_sorted_pred.head(2)["horse_id"].values)
            true_ids_top2 = set(group_sorted_true.head(2)["horse_id"].values)
            correct_top2 = len(pred_ids_top2 & true_ids_top2)
            top2_accuracy_list.append(correct_top2 / 2.0)
        
        # ---------------
        # 4) MRR (Winner)
        # ---------------
        # MRR on the winner only: find rank of actual winner in predicted order
        try:
            rank = group_sorted_pred.index[group_sorted_pred["horse_id"] == top1_true][0] + 1
        except IndexError:
            rank = None
        reciprocal_ranks.append(1 / rank if rank is not None else 0)
        
        # --------------------
        # 5) Prediction StdDev
        # --------------------
        stddev = group_sorted_pred["prediction"].std()
        if stddev is not None:
            prediction_std_devs.append(stddev)
        
        # -------------------------
        # 6) Collect for RMSE/MAE
        # -------------------------
        # All true/pred for this race (no sorting needed, but either is fine)
        all_true_vals.extend(group["relevance"].values)
        all_pred_vals.extend(group["prediction"].values)
        
        # -------------------------
        # 7) NDCG Calculation
        # -------------------------
        # We'll do NDCG@3 as an example (like your code):
        # group_sorted_true["relevance"] are the 'true' labels in descending rank
        # group_sorted_pred["prediction"] are predicted scores in descending rank
        all_relevance.append(group_sorted_true["relevance"].values)
        all_predictions.append(group_sorted_pred["prediction"].values)
    
    # End group loop
    
    # ====================
    # Compute Aggregate Metrics
    # ====================

    # Ensure we don't divide by zero if total_groups==0
    if total_groups == 0:
        logging.warning("No groups had 2 or more horses! Metrics not computed.")
        return {}
    
    avg_top4_accuracy = float(np.mean(top4_accuracy_list)) if top4_accuracy_list else 0.0
    avg_top1_accuracy = float(np.mean(top1_accuracy_list)) if top1_accuracy_list else 0.0
    avg_top2_accuracy = float(np.mean(top2_accuracy_list)) if top2_accuracy_list else 0.0
    
    perfect_order_percentage = float(perfect_order_count / (total_groups))  # fraction of total races
    MRR = float(np.mean(reciprocal_ranks)) if reciprocal_ranks else 0.0
    prediction_variance = float(np.mean(prediction_std_devs)) if prediction_std_devs else 0.0
    
    # RMSE
    # (If you have a custom root_mean_squared_error, use that)
    RMSE = float(np.sqrt(np.mean((np.array(all_true_vals) - np.array(all_pred_vals))**2))) if all_true_vals else 0.0
    # Or if you prefer scikit-learn's mean_squared_error:
    # from sklearn.metrics import mean_squared_error
    # RMSE = float(np.sqrt(mean_squared_error(all_true_vals, all_pred_vals)))
    
    # MAE
    MAE = float(mean_absolute_error(all_true_vals, all_pred_vals)) if len(all_true_vals) > 1 else 0.0
    
    # Spearman correlation
    spearman_corr = 0.0
    if len(all_true_vals) > 1:
        spearman_corr = float(stats.spearmanr(all_true_vals, all_pred_vals)[0])
    
    # NDCG@3 for each group
    ndcg_values = []
    for t, p in zip(all_relevance, all_predictions):
        # SciKit's ndcg_score expects a 2D array: [ [relevance], [predictions] ]
        ndcg_values.append(ndcg_score([t], [p], k=3))
    avg_ndcg_3 = float(np.mean(ndcg_values)) if ndcg_values else 0.0
    
    metrics = {
        "model_key": model_key,
        "avg_top_4_accuracy": avg_top4_accuracy,
        "avg_top_1_accuracy": avg_top1_accuracy,
        "avg_top_2_accuracy": avg_top2_accuracy,
        "perfect_order_percentage": perfect_order_percentage,  # top-4 in the correct order
        "avg_ndcg_3": avg_ndcg_3,
        "MRR": MRR,
        "spearman_corr": spearman_corr,
        "RMSE": RMSE,
        "MAE": MAE,
        "prediction_variance": prediction_variance,
        "total_groups_evaluated": total_groups
    }
    
    return metrics

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
        "relevance": holdout_labels,
        "horse_id": holdout_df["horse_id"].values  # Ensure horse_id is included for merging
    })

    holdout_merged = holdout_predictions.merge(
        holdout_df,
        on=["group_id", "horse_id"],  # Merge on both group_id and horse_id
        how="left",
        suffixes=("", "_df")
    )
    
    if "relevance_df" in holdout_merged.columns:
        holdout_merged.drop(columns=["relevance_df"], inplace=True)
    if "prediction_df" in holdout_merged.columns:
        holdout_merged.drop(columns=["prediction_df"], inplace=True)
    # print("[DEBUG] holdout_merged columns (after merge):", holdout_merged.columns.tolist())
    
    logging.info(f"Final holdout_merged dataset size: {holdout_merged.shape}")

    # Convert race_date to str for Spark
    # holdout_merged["race_date"] = holdout_merged["race_date"].astype(str)

    # Sort by prediction descending, compute rank
    holdout_merged = holdout_merged.sort_values(by=["group_id", "prediction"], ascending=[True, False])
    holdout_merged["rank"] = holdout_merged.groupby("group_id").cumcount() + 1
    
    metrics = compute_metrics(holdout_merged, model_key)

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
    db_table,  # single final table: catboost_enriched_results
    data_splits
):
    """
    1) For each combination of catboost_loss_functions & catboost_eval_metrics:
       a) Run cross-validation
       b) Run Optuna
       c) Train model -> catboost_enriched_results (train+valid)
       d) Evaluate holdout -> catboost_enriched_results
    2) Returns a dictionary of all trained models & best params.
    """
    all_models = {}
    for loss_func in catboost_loss_functions:
        for eval_met in catboost_eval_metrics:
            logging.info(f"=== Starting cross-validation for {loss_func} with {eval_met} ===")
            
            # 1) Run cross-validation
            avg_ndcg_score = cross_validate_model(
                catboost_loss_functions=loss_func,
                eval_metric=eval_met,
                data_splits=data_splits,
                n_splits=5
            )
            logging.info(f"Cross-validation NDCG@4 score: {avg_ndcg_score}")

            # 2) Run Optuna
            study = run_optuna(
                catboost_loss_functions=loss_func,
                eval_metric=eval_met,
                n_trials=20,
                data_splits=data_splits
            )

            best_score = study.best_value
            best_params = study.best_params
            logging.info(f"Best score: {best_score}, Best params: {best_params}")

            # 3) Train final model + save train+valid predictions -> catboost_enriched_results
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

            # 4) Evaluate on holdout -> catboost_enriched_results
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
    final_feature_cols,
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

    def check_combination_uniqueness(data, data_name):
        combination_unique = data[["horse_id", "group_id"]].drop_duplicates().shape[0] == data.shape[0]
        logging.info(f"{data_name} - horse_id and group_id combination unique: {combination_unique}")
        if not combination_unique:
            logging.info(f"Duplicate horse_id+group_id in {data_name}:")
            logging.info(data[data.duplicated(["horse_id", "group_id"], keep=False)])

    check_combination_uniqueness(train_data, "train_data")
    check_combination_uniqueness(valid_data, "valid_data")
    check_combination_uniqueness(holdout_data, "holdout_data")

    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/training/final_feature_cols_{now_str}.json"
    with open(filename, "w") as f:
        json.dump(final_feature_cols, f)

    # 5) Build X,y for each split, referencing final_feature_cols
    
    all_feature_cols = final_feature_cols + embed_cols + cat_cols

    X_train = train_data[all_feature_cols].copy()
    y_train = train_data[label_col].values
    train_group_id = train_data["group_id"].values

    X_valid = valid_data[all_feature_cols].copy()
    y_valid = valid_data[label_col].values
    valid_group_id = valid_data["group_id"].values

    X_holdout = holdout_data[all_feature_cols].copy()
    y_holdout = holdout_data[label_col].values
    holdout_group_id = holdout_data["group_id"].values

    train_pool = Pool(X_train, label=y_train, group_id=train_group_id, cat_features=cat_cols)
    valid_pool = Pool(X_valid, label=y_valid, group_id=valid_group_id, cat_features=cat_cols)
    holdout_pool = Pool(X_holdout, label=y_holdout, group_id=holdout_group_id, cat_features=cat_cols)

    data_splits = DataSplits(
        X_train, y_train, train_group_id,
        X_valid, y_valid, valid_group_id,
        X_holdout, y_holdout, holdout_group_id,
        train_data, valid_data, holdout_data, cat_cols)
    
    # 7) minimal catboost training
    catboost_loss_functions = [
        # "YetiRank:top=1",
        # "YetiRank:top=2",
        # "YetiRank:top=3",
        "YetiRank:top=4"
    ]
    catboost_eval_metrics = ["NDCG:top=2"] #["NDCG:top=1", "NDCG:top=2", "NDCG:top=3", "NDCG:top=4"]

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
        db_table=db_table,
        data_splits=data_splits
    )

    # 3) Save final_feature_cols with a timestamp to avoid overwriting
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    outpath = f"/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/training/final_feature_cols_{now_str}.json"
    with open(outpath, "w") as fp:
        json.dump(final_feature_cols, fp)
    logging.info(f"Saved final_feature_cols to {outpath}")

    # # Save
    save_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/all_models.json"
    with open(save_path, "w") as fp:
        json.dump(all_models, fp, indent=2)
    print(f"Saved all_models to {save_path}")

    print("Done building minimal CatBoost model.")
    return all_models

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
                
        # Print columns of fut_df
        print("Columns in fut_df:")
        print(pdf.columns)

        # Check if 'group_id' is in the columns
        if 'group_id' not in pdf.columns:
            print("The 'group_id' column is NOT present in fut_df. Exiting the program.")
            sys.exit(1)
        
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
    1) Gather final_feature_cols + cat_cols + embed_cols to form 'all_feature_cols'.
    2) Load each .cbm model in models_dir, run inference, store in a new column.
    3) Save final fut_df to a DB table, returning a Spark DataFrame of the results.
    """

    logging.info("=== Starting Multi-Model Future Inference ===")

    # [1] Combine columns for inference
    all_feature_cols = final_feature_cols + embed_cols + cat_cols

    # [2] Make a copy to avoid SettingWithCopyWarning
    fut_df = fut_df.copy()

    # [3] Convert categorical columns to category dtype
    for c in cat_cols:
        if c in fut_df.columns:
            fut_df.loc[:, c] = fut_df[c].astype("category")
    
    # [4] Check for missing columns
    missing_cols = set(all_feature_cols) - set(fut_df.columns)
    if missing_cols:
        logging.warning(f"Future DF is missing columns: {missing_cols}")

    # [5] Gather .cbm files from the models directory
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

    # [6] Inference for each model
    for file in model_files:
        model_path = os.path.join(models_dir, file)
        logging.info(f"=== Making predictions with model: {file} ===")
        # Make sure group_id is included

        # Build a copy for inference
        inference_df = fut_df[all_feature_cols].copy()
        
        if "group_id" in fut_df.columns:
            inference_df["group_id"] = fut_df["group_id"]
    
        # Optional: Sort by group_id if you are using a ranker
        if "group_id" in fut_df.columns:
            inference_df = inference_df.sort_values("group_id")

        # Log some debug info safely
        try:
            logging.info("Checking null counts in inference_df:\n%s", inference_df.isnull().sum())
        except Exception as e:
            logging.error(f"Error logging inference_df null counts: {e}", exc_info=True)

        # 6A) Run predictions
        scored_df = make_future_predictions(
            pdf=inference_df, 
            all_feature_cols=all_feature_cols,
            cat_cols=cat_cols, 
            model_path=model_path, 
            model_type="ranker"  # or "regressor" if needed
        )

        # 6B) Create a safe column name from the model filename
        # Example: "catboost_YetiRank_top2_20250310_213012.cbm" -> "YetiRank_top2"
        model_col = file
        model_col = re.sub(r'^catboost_', '', model_col)     # remove "catboost_" prefix
        model_col = re.sub(r'\.cbm$', '', model_col)          # remove ".cbm"
        model_col = re.sub(r'_\d{8}_\d{6}$', '', model_col)    # remove timestamp
        model_col = re.sub(r'[^a-zA-Z0-9_]', '_', model_col)   # replace special chars with '_'

        # 6C) Insert scores back into fut_df
        #     Because 'inference_df' is a subset, we match by index:
        fut_df.loc[inference_df.index, model_col] = scored_df["model_score"].values

    # [7] Write final predictions to DB
    # Build a dynamic table name
    today = date.today()
    today_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name = f"predictions_{today_str}_1"
    
    logging.info(f"Writing predictions to DB table: {table_name}")
    scored_sdf = spark.createDataFrame(fut_df)
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

###############################################################################
# Build catboost model - single table approach
###############################################################################
def build_catboost_model(spark, horse_embedding, jdbc_url, jdbc_properties, action):
    
    print("DEBUG: datetime is =>", datetime)
        
    # Then use Pandas boolean indexing
    historical_pdf = horse_embedding[horse_embedding["data_flag"] != "future"].copy()
    future_pdf = horse_embedding[horse_embedding["data_flag"] == "future"].copy()
    # cols_to_locf = ["global_speed_score_iq_prev"]
    # future_pdf = locf_across_hist_future(historical_pdf, future_pdf, cols_to_locf, date_col="race_date")
        
    # Transform the Spark DataFrame to Pandas.
    hist_pdf, fut_pdf, cat_cols, excluded_cols = transform_horse_df_to_pandas(historical_pdf, future_pdf, drop_label=False)
    logging.info(f"Shape of historical Pandas DF: {hist_pdf.shape}")
    logging.info(f"Shape of future Pandas DF: {fut_pdf.shape}")

    hist_pdf = assign_labels(hist_pdf, alpha=0.8)
    logging.info(f"Shape of Historical Pandas DF: {hist_pdf.shape}")
        
    fut_pdf = assign_labels(fut_pdf, alpha=0.8)
    logging.info(f"Shape of Future Pandas DF: {fut_pdf.shape}")

    hist_embed_cols, fut_embed_cols = build_embed_cols(hist_pdf)  # This is now your dynamic list of combined columns.
    
        # 1) Prepare columns to match training
    # final_feature_cols = [
    #     "global_speed_score_iq_prev", "Spar", "class_rating", "previous_distance", 
    #     "off_finish_last_race", "prev_speed_rating", "purse", "claimprice", "power", 
    #     "avgspd", "avg_spd_sd","ave_cl_sd", "hi_spd_sd", "pstyerl", "horse_itm_percentage",
    #     "total_races_5", "avg_dist_bk_gate1_5", "avg_dist_bk_gate2_5", "avg_dist_bk_gate3_5", 
    #     "avg_dist_bk_gate4_5","avg_speed_fullrace_5","avg_stride_length_5","avg_strfreq_q1_5",
    #     "avg_speed_5","avg_fin_5","best_speed","avg_beaten_len_5","prev_speed",
    #     "avg_strfreq_q2_5", "avg_strfreq_q3_5", "avg_strfreq_q4_5", "speed_improvement", 
    #     "age_at_race_day", "count_workouts_3","avg_workout_rank_3","weight","days_off", 
    #     "starts", "race_count","has_gps", "cond_starts","cond_win","cond_place","cond_show",
    #     "cond_fourth","cond_earnings","all_starts", "all_win", "all_place", "all_show", 
    #     "all_fourth","all_earnings","morn_odds","net_sentiment", "distance_meters", 
    #     "jt_itm_percent","jt_win_percent","trainer_win_track","trainer_itm_track",
    #     "trainer_win_percent","trainer_itm_percent","jock_win_track","jock_win_percent",
    #     "jock_itm_track","jt_win_track","jt_itm_track","jock_itm_percent",
    #     "sire_itm_percentage","sire_roi","dam_itm_percentage","dam_roi"
    # ]
        
    final_feature_cols = ["global_speed_score_iq","horse_mean_rps","horse_std_rps","power","base_speed",
        "speed_improvement",'avgspd','starts','avg_spd_sd','ave_cl_sd',
        'hi_spd_sd','pstyerl','sire_itm_percentage','sire_roi','dam_itm_percentage','dam_roi',
        'all_starts','all_win','all_place','all_show','all_fourth','all_earnings','horse_itm_percentage',
        'best_speed', 'weight','jock_win_percent', 'jock_itm_percent','trainer_win_percent','trainer_itm_percent',
        'jt_win_percent','jt_itm_percent','jock_win_track','jock_itm_track','trainer_win_track',
        'trainer_itm_track','jt_win_track','jt_itm_track','cond_starts','cond_win','cond_place','cond_show',
        'cond_fourth','cond_earnings', 'par_time','running_time','total_distance_ran','previous_distance',
        'distance_meters','avgtime_gate1','avgtime_gate2','avgtime_gate3','avgtime_gate4',
        'dist_bk_gate1','dist_bk_gate2','dist_bk_gate3','dist_bk_gate4',
        'speed_q1','speed_q2','speed_q3','speed_q4','speed_var','avg_speed_fullrace',
        'accel_q1','accel_q2','accel_q3','accel_q4','avg_acceleration','max_acceleration',
        'jerk_q1','jerk_q2','jerk_q3','jerk_q4','avg_jerk','max_jerk',
        'dist_q1','dist_q2','dist_q3','dist_q4','total_dist_covered',
        'strfreq_q1','strfreq_q2','strfreq_q3','strfreq_q4','avg_stride_length','net_progress_gain',
        'prev_speed_rating','previous_class', 'purse','class_rating','morn_odds',
        'net_sentiment','avg_fin_5','avg_speed_5','avg_beaten_len_5','avg_dist_bk_gate1_5','avg_dist_bk_gate2_5','avg_dist_bk_gate3_5',
        'avg_dist_bk_gate4_5','avg_speed_fullrace_5','avg_stride_length_5','avg_strfreq_q1_5','avg_strfreq_q2_5',
        'avg_strfreq_q3_5','avg_strfreq_q4_5','prev_speed','days_off','avg_workout_rank_3',
        'has_gps','age_at_race_day', 'race_avg_speed_agg','race_std_speed_agg','race_avg_relevance_agg','race_std_relevance_agg',
        'race_class_avg_speed_agg','race_class_min_speed_agg','race_class_max_speed_agg', 'claimprice']
    
    # 6) Train the model(s) using historical data
    all_models = split_data_and_train(
        df=hist_pdf,
        label_col="relevance",
        cat_cols=cat_cols,
        embed_cols=hist_embed_cols,   # could also just pass fut_embed_cols if identical
        final_feature_cols=final_feature_cols,
        jdbc_url=jdbc_url,
        jdbc_properties=jdbc_properties,
        spark=spark
    )

    # 7) **Infer on the future data** (Placeholder function call)
    # After training is done, you have fut_pdf with the same columns/transform as hist_pdf
    # Check if 'group_id' is in the columns
    if 'group_id' not in fut_pdf.columns:
        print("The 'group_id' column is NOT present in fut_df. Exiting the program.")
        sys.exit(1)
    
    scored_sdf = do_future_inference_multi(
        spark=spark,
        fut_df=fut_pdf,
        cat_cols=cat_cols,
        embed_cols=fut_embed_cols,       # or hist_embed_cols if they match
        final_feature_cols=final_feature_cols,
        db_url=jdbc_url,
        db_properties=jdbc_properties,
        models_dir="./data/models/catboost",  # or your actual path
        output_dir="./data/predictions"
    )

    return scored_sdf

def transform_horse_df_to_pandas(hist_df, fut_df, drop_label=False):
    """
    1) Convert df_spark to Pandas.
    2) 'official_fin' Transform the Label to “Bigger = Better” log-based or exponential-based formula to positions ≥ 5 to make them much less relevant
    3) Create race_id, group_id, convert date columns, etc.
    4) Return the transformed Pandas DataFrame.
    """

    # Check the dtypes in Pandas (should show datetime64[ns] for the above columns)
    # Now you can safely convert to Pandas
    hist_pdf = hist_df.copy()
    fut_pdf = fut_df.copy()
    
    # Historical: Create race_id + group_id
    hist_pdf["race_id"] = (
        hist_pdf["course_cd"].astype(str)
        + "_"
        + hist_pdf["race_date"].astype(str)
        + "_"
        + hist_pdf["race_number"].astype(str)
    )
    
    # Future: Create race_id + group_id
    fut_pdf["race_id"] = (
        fut_pdf["course_cd"].astype(str)
        + "_"
        + fut_pdf["race_date"].astype(str)
        + "_"
        + fut_pdf["race_number"].astype(str)
    )
    # Historical: Create group_id and sort ascending for historical data
    hist_pdf["group_id"] = hist_pdf["race_id"].astype("category").cat.codes
    hist_pdf = hist_pdf.sort_values("group_id", ascending=True).reset_index(drop=True)
    
    # Future: Create group_id and sort ascending for future data
    fut_pdf["group_id"] = fut_pdf["race_id"].astype("category").cat.codes
    fut_pdf = fut_pdf.sort_values("group_id", ascending=True).reset_index(drop=True)
    
    # Historical: Convert selected datetime columns to numeric
    datetime_columns = ["first_race_date_5", "most_recent_race_5", "prev_race_date"]
    new_numeric_cols = {}
    for col in datetime_columns:
        hist_pdf[col] = pd.to_datetime(hist_pdf[col])
        new_numeric_cols[col + "_numeric"] = (hist_pdf[col] - pd.Timestamp("1970-01-01")).dt.days
    # Historical: Drop the original datetime columns
    hist_pdf.drop(columns=datetime_columns, inplace=True, errors="ignore")
    hist_pdf = pd.concat([hist_pdf, pd.DataFrame(new_numeric_cols, index=hist_pdf.index)], axis=1)

    # Future: Convert selected datetime columns to numeric
    datetime_columns = ["first_race_date_5", "most_recent_race_5", "prev_race_date"]
    new_numeric_cols = {}
    for col in datetime_columns:
        fut_pdf[col] = pd.to_datetime(fut_pdf[col])
        new_numeric_cols[col + "_numeric"] = (fut_pdf[col] - pd.Timestamp("1970-01-01")).dt.days
    # Future: Drop the original datetime columns
    fut_pdf.drop(columns=datetime_columns, inplace=True, errors="ignore")
    fut_pdf = pd.concat([fut_pdf, pd.DataFrame(new_numeric_cols, index=fut_pdf.index)], axis=1)

    # Historical: Convert main race_date to datetime
    hist_pdf["race_date"] = pd.to_datetime(hist_pdf["race_date"])

    # Future: Convert main race_date to datetime
    fut_pdf["race_date"] = pd.to_datetime(fut_pdf["race_date"])
    
    # # Make certain columns categorical
    cat_cols = ["course_cd", "trk_cond", "sex", "equip", "surface", "med",
                 "race_type", "stk_clm_md", "turf_mud_mark", "layoff_cat","previous_surface"]
    # cat_cols = []
    
    excluded_cols = [
        "axciskey",         # Raw identifier
        "official_fin",     # Target column
        "relevance", 
        "top4_label",
        "post_time",
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
    ]

    # Historical: Convert to categorical data type
    for c in cat_cols:
        if c in hist_pdf.columns:
            hist_pdf[c] = hist_pdf[c].astype("category")
            
    
    # Future: Convert to categorical data type
    for c in cat_cols:
        if c in fut_pdf.columns:
            fut_pdf[c] = fut_pdf[c].astype("category")
                          
    # Return the transformed Pandas DataFrame, along with cat_cols and excluded_cols
    return hist_pdf, fut_pdf, cat_cols, excluded_cols

def assign_labels(df, alpha=0.8):
    """
    1) Exponential label in [0,1] for ranking,
    2) Binary label for top-4 or not.
    """
    def _exp_label(fin):
        return alpha ** (fin - 1)

    df["relevance"] = df["official_fin"].apply(_exp_label)
    df["top4_label"] = (df["official_fin"] <= 4).astype(int)
    return df

def build_embed_cols(hist):
    """
    1) Create a list of combined columns for embedding.
    2) Return the list of combined columns.
    """
    all_combined_cols = [c for c in hist.columns if c.startswith("combined_")]
    
    all_combined_cols_sorted = sorted(all_combined_cols, key=lambda x: int(x.split("_")[1]))
    # Historical:
    hist_embed_cols = all_combined_cols_sorted  # This is now your dynamic list of combined columns.
    
    fut_embed_cols = all_combined_cols_sorted  # This is now your dynamic list of combined columns.
    return hist_embed_cols, fut_embed_cols

# Retired function
# def assign_piecewise_log_labels(df):
#     """
#     If official_fin <= 4, assign them to a high relevance band (with small differences).
#     Otherwise, use a log-based formula that drops more sharply.
#     """
#     def _relevance(fin):
#         if fin == 1:
#             return 40  # Could be 40 for 1st
#         elif fin == 2:
#             return 38  # Slightly lower than 1st, but still high
#         elif fin == 3:
#             return 36
#         elif fin == 4:
#             return 34
#         else:
#             # For 5th place or worse, drop off fast with a log formula:
#             # e.g. alpha=30, beta=4 => 5th => 30 / log(4+5)=30/log(9)= ~9
#             alpha = 30.0
#             beta  = 4.0
#             return alpha / np.log(beta + fin)
    
#     df["relevance"] = df["official_fin"].apply(_relevance)
#     return df

def cross_validate_model(catboost_loss_functions, eval_metric, data_splits, n_splits=5):
    tscv = TimeSeriesSplit(n_splits=n_splits)
    ndcg_scores = []

    for train_index, valid_index in tscv.split(data_splits.X_train):
        logging.info(f"Train indices: {train_index[:10]}... Valid indices: {valid_index[:10]}...")

        # Ensure indices are properly reset
        X_train_fold = data_splits.X_train.iloc[train_index].reset_index(drop=True)
        X_valid_fold = data_splits.X_train.iloc[valid_index].reset_index(drop=True)
        y_train_fold = data_splits.y_train[train_index]  # NumPy array, no need for iloc
        y_valid_fold = data_splits.y_train[valid_index]  # NumPy array, no need for iloc
        train_group_id_fold = data_splits.train_group_id[train_index]  # NumPy array, no need for iloc
        valid_group_id_fold = data_splits.train_group_id[valid_index]  # NumPy array, no need for iloc

        logging.info(f"X_train_fold shape: {X_train_fold.shape}, X_valid_fold shape: {X_valid_fold.shape}")
        logging.info(f"y_train_fold shape: {y_train_fold.shape}, y_valid_fold shape: {y_valid_fold.shape}")

        train_pool = Pool(X_train_fold, label=y_train_fold, group_id=train_group_id_fold, cat_features=data_splits.cat_cols)
        valid_pool = Pool(X_valid_fold, label=y_valid_fold, group_id=valid_group_id_fold, cat_features=data_splits.cat_cols)

        model = CatBoostRanker(
            loss_function=catboost_loss_functions,
            eval_metric=eval_metric,
            task_type="GPU",
            devices="0,1",
            random_seed=42,
            verbose=50
        )
        model.fit(train_pool, eval_set=valid_pool, use_best_model=True, early_stopping_rounds=100)

        ndcg_score = data_splits.compute_ndcg(model, k=4)
        ndcg_scores.append(ndcg_score)

    avg_ndcg_score = np.mean(ndcg_scores)
    return avg_ndcg_score