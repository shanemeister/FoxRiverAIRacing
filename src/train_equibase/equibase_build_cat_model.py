import io
import json
import os
import time
import re
import sys
import logging
import datetime
import optuna
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
from scipy import stats
from scipy.stats import spearmanr
from src.train_and_predict.horse_embed_query import horse_embedding_queries

###############################################################################
# Create a class to enable calculation of NDCG from the objective function 
###############################################################################

class DataSplits:
    def __init__(self, X_train, y_train, train_group_id, X_valid, y_valid, valid_group_id, X_holdout, y_holdout, holdout_group_id,
        train_data, valid_data, holdout_data, cat_cols, all_feature_cols):
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
        self.all_feature_cols = all_feature_cols
        self.train_data = train_data
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

import pandas as pd

def top3_in_top3_fraction(
    model,
    df: pd.DataFrame,
    group_col: str = "group_id",
    horse_col: str = "horse_id",
    label_col: str = "relevance",
    feature_cols = None
) -> float:
    """
    Computes the fraction of races (with at least 6 horses) where
    the model's predicted top-3 exactly matches the actual top-3.
    """
    if feature_cols is not None:
        df = df.copy()
        df["prediction"] = model.predict(df[feature_cols])
    elif "prediction" not in df.columns:
        raise ValueError(
            "No `feature_cols` provided and `df` has no 'prediction' column. "
            "Cannot compute predictions."
        )

    total_groups = 0
    strict_top3_hits = 0
    perfect_order_count = 0
    
    for gid, group in df.groupby(group_col):
        if len(group) < 4:
            continue

        total_groups += 1
        try:
            # Sort descending by predicted score
            group_sorted_pred = group.sort_values("prediction", ascending=False)
            # Sort ascending by true relevance
            group_sorted_true = group.sort_values(label_col, ascending=False)
            
            # top3_true_ids = group_sorted_true.head(3)[horse_col].values
            # top3_pred_ids = group_sorted_pred.head(3)[horse_col].values

            pred_top3 = set(group_sorted_pred.head(3)[horse_col])
            true_top3 = set(group_sorted_true.head(3)[horse_col])
            # Just require the top-3 to match exactly but not necessarily in order
            if pred_top3 == true_top3:
                strict_top3_hits += 1
            
            # if np.array_equal(top3_pred_ids, top3_true_ids):
            #     # The first horse is exactly the same, the second is exactly the same, and the third is exactly the same, in order
            #     perfect_order_count += 1
        except Exception as e:
            print(f"Error in group {gid}: {e}")
            
    if total_groups == 0:
        return 0.0

    return strict_top3_hits / total_groups

def metric_top4_hit(
    model,
    df: pd.DataFrame,
    group_col: str = "group_id",
    horse_col: str = "horse_id",
    label_col: str = "relevance",
    feature_cols=None
) -> float:
    """
    For each race (group):
      1) We sort horses by predicted scores descending to get predicted_top4.
      2) We sort horses by label descending (meaning 1st place has highest label,
         or if you're using 'relevance' with bigger = better) to get actual_top4.
      3) If the predicted_top4 horses are all in actual_top4, we count that race as a 'hit'.

    Return the fraction of races (with >=4 horses) that meet this condition.
    """

    # If we need to predict within this function:
    if feature_cols is not None:
        df = df.copy()
        df["prediction"] = model.predict(df[feature_cols])
    elif "prediction" not in df.columns:
        raise ValueError(
            "No `feature_cols` provided and `df` has no 'prediction' column. "
            "Cannot compute predictions."
        )

    total_groups = 0
    hits = 0

    for gid, group in df.groupby(group_col):
        # skip very small races if desired
        if len(group) < 4:
            continue

        total_groups += 1
        try:
            # Sort by predicted score descending
            group_sorted_pred = group.sort_values("prediction", ascending=False)
            predicted_top4 = group_sorted_pred.head(4)[horse_col].values

            # Sort by actual label descending
            #  - If "relevance" is bigger-is-better (i.e. 1st place has the highest label).
            #  - If your label is smaller-is-better (like finishing position), you might
            #    need ascending=True. Adjust as needed.
            group_sorted_true = group.sort_values(label_col, ascending=False)
            actual_top4 = group_sorted_true.head(4)[horse_col].values

            # Check set membership ignoring order:
            set_predicted = set(predicted_top4)
            set_actual = set(actual_top4)

            # If the predicted top-4 is a subset of actual top-4:
            if set_predicted.issubset(set_actual):
                hits += 1

        except Exception as e:
            print(f"Error in group {gid}: {e}")

    if total_groups == 0:
        return 0.0

    return hits / total_groups

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

    # If user picks "YetiRankPairwise", force CPU (GPU not supported).
    if catboost_loss_functions == "YetiRankPairwise":
        chosen_task_type = "CPU"
    else:
        chosen_task_type = "GPU"

    params = {
        "loss_function": catboost_loss_functions,
        "eval_metric": eval_metric,
        "task_type": chosen_task_type,
        "thread_count": 96,
        # Only Bernoulli for now
        "bootstrap_type": trial.suggest_categorical("bootstrap_type", ["Bernoulli", "Bayesian"]),
        # Narrowed iterations range centered around 2450
        "iterations": trial.suggest_int("iterations", 2400, 5000, step=50),
        # Narrowed depth near best (adjust as needed if GPU pairwise requirements apply)
        "depth": trial.suggest_int("depth", 8, 9),
        # Narrower learning_rate range
        "learning_rate": trial.suggest_float("learning_rate", 0.0005, 0.005, log=True),
        # Narrowed l2_leaf_reg range
        "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 7.0, 10.0, log=True),
        # Fixed grow_policy remains constant
        "grow_policy": "SymmetricTree",
        # Narrowed random_strength range
        "random_strength": trial.suggest_float("random_strength", 2.5, 3.5),
        # Narrowed min_data_in_leaf range
        "min_data_in_leaf": trial.suggest_int("min_data_in_leaf", 5, 15, step=5),
        # Fixed border_count
        "border_count": 128,
        "od_type": "Iter",
        "early_stopping_rounds": 100,
        "random_seed": 42,
        "verbose": 100,
        "allow_writing_files": False
    }    
    # If you want multi-GPU usage (e.g., devices 0 and 1)
    if chosen_task_type == "GPU":
        params["devices"] = "0,1"


    model = CatBoostRanker(**params)
    model.fit(
        data_splits.train_pool, 
        eval_set=data_splits.valid_pool, 
        use_best_model=True,
        early_stopping_rounds=params["early_stopping_rounds"]
    )
    
    fraction = metric_top4_hit(
        model=model,
        df=data_splits.valid_data,
        group_col="race_id",
        horse_col="horse_id",
        label_col="relevance",
        feature_cols=data_splits.all_feature_cols
    )
    print(f"metric_top4_hit = {fraction:.4f}")
    
    return fraction


def run_optuna(catboost_loss_functions, eval_metric, n_trials=20, data_splits=None):

    storage = "sqlite:///optuna_study.db"
    study_name = f"catboost_{catboost_loss_functions}_{eval_metric}_v1"

    study = optuna.create_study(
        study_name=study_name,
        storage=storage,
        load_if_exists=True,
        direction="maximize"
    )
    
    study.optimize(
        lambda trial: objective(trial, catboost_loss_functions, eval_metric, data_splits),
        n_trials=n_trials
    )
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

    # Start with a copy of best_params from the study. Remove keys that are only used for early stopping
    # and GPU-specific settings, ensuring the parameters match what was used in the objective.
    recognized_params = dict(best_params)
    recognized_params.pop("early_stopping_rounds", None)
    recognized_params.pop("devices", None)  # Remove GPU-specific parameter if it exists

    # Update/ensure critical parameters are set as in the objective.
    recognized_params["loss_function"] = catboost_loss_functions
    recognized_params["eval_metric"] = eval_metric
    recognized_params["random_seed"] = 42
    # Do not override task_type here; use what's in best_params (which may be CPU if needed).

    final_model = CatBoostRanker(**recognized_params)
    final_model.fit(
        train_pool,
        eval_set=valid_pool,
        use_best_model=True,
        early_stopping_rounds=best_params.get("early_stopping_rounds", 100),
        verbose=100
    )
    
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
    embed_data = pd.concat([enriched_train, enriched_valid], ignore_index=True)
    # Sort + rank per group_id
    embed_data = embed_data.sort_values(by=["group_id", "prediction"], ascending=[True, False])
    embed_data["rank"] = embed_data.groupby("group_id").cumcount() + 1

    # Check for duplicates in embed_data
    def check_duplicates(data, subset_cols):
        duplicates = data[data.duplicated(subset=subset_cols, keep=False)]
        if not duplicates.empty:
            print(f"Found duplicates in embed_data based on columns {subset_cols}:")
            print(duplicates)
        else:
            print(f"No duplicates found in embed_data based on columns {subset_cols}.")

    check_duplicates(embed_data, ["horse_id", "group_id"])

    # Write to DB (append mode)
    try:
        spark_df = spark.createDataFrame(embed_data)
        (
            spark_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", db_table)
            .option("user", jdbc_properties["user"])
            .option("driver", jdbc_properties["driver"])
            .mode("append")
            .save()
        )
        print("Table read successfully. Row count:", spark_df.count())
        print(f"Appended {len(embed_data)} rows to '{db_table}' (train+valid).")
    except Exception as e:
        print(f"Error saving train+valid predictions: {e}")
        raise

    return final_model, model_path, model_key, embed_data

def compute_metrics_with_trifecta(
    holdout_merged, 
    model_key,
    holdout_preds, 
    holdout_labels,
    holdout_group_id
):    
    """
    Computes ranking metrics for each race (group_id),
    including trifecta-related metrics:

      - Top-3 in the predicted top-3 (partial credit: fraction correct among 3).
      - A boolean “top3_in_top4” for whether all actual top-3 finishers 
        are contained in the predicted top-4 (for trifecta “box 4”).

    Parameters
    ----------
    holdout_merged : pd.DataFrame
        Must have columns:
          ['group_id', 'horse_id', 'relevance', 'prediction', 'official_fin']
    model_key : str
        The model name or key to store in metrics.

    Returns
    -------
    metrics : dict
        A dictionary of computed metrics.
    """

    top4_accuracy = 0
    top1_accuracy = 0
    perfect_order_count = 0

    # TRIFECTA-RELATED
    # partial credit for top-3 in predicted top-3
    top3_in_top3 = 0
    # boolean: did we get *all 3* actual top-3 horses in predicted top-4?
    top3_in_top4_hits = 0
    reciprocal_ranks = []
    prediction_std_devs = []
    
    all_true_vals = []
    all_pred_vals = []

    total_groups = 0

    # Group by race
    for gid, group in holdout_merged.groupby("group_id"):
        # If the race has <5 horses, skip or continue
        if len(group) < 4:
            continue
        
        total_groups += 1
        
        # Sort descending by predicted score
        group_sorted_pred = group.sort_values("prediction", ascending=False).reset_index(drop=False)
        # Sort ascending by true relevance (bigger = better finish)
        group_sorted_true = group.sort_values("relevance", ascending=False).reset_index(drop=False)

        # =============== 
        # 1) Top-4 Accuracy
        # ===============
        if len(group) >= 5:
            top4_pred_ids = group_sorted_pred.head(4)["horse_id"].values
            top4_true_ids = group_sorted_true.head(4)["horse_id"].values
            
            # sets for ignoring order
            pred_set_4 = set(top4_pred_ids)
            true_set_4 = set(top4_true_ids)
            correct_top4 = len(pred_set_4.intersection(true_set_4))
            if correct_top4 == 4:
                top4_accuracy += 1
            
            # perfect order (only if the first 4 exactly match in sequence)
            if np.array_equal(top4_pred_ids, top4_true_ids):
                perfect_order_count += 1

        # =============== 
        # 2) Top-1 Accuracy
        # ===============
        top1_pred = group_sorted_pred.iloc[0]["horse_id"]
        top1_true = group_sorted_true.iloc[0]["horse_id"]
        if top1_pred == top1_true:
            top1_accuracy += 1
        # =============== 
        # 3) TRIFECTA METRICS
        # ===============
        if len(group) >= 5:
            # Actual top-3 finishers
            top3_true_ids = group_sorted_true.head(3)["horse_id"].values
            # Predicted top-3
            top3_pred_ids = group_sorted_pred.head(3)["horse_id"].values
            # Also predicted top-4 for "boxing" scenario
            top4_pred_ids = group_sorted_pred.head(4)["horse_id"].values

            # partial fraction: how many of the true top-3 appear in predicted top-3?
            pred_set_3 = set(top3_pred_ids)
            true_set_3 = set(top3_true_ids)
            intersect_3 = len(pred_set_3.intersection(true_set_3))
            if intersect_3 == 3:
                top3_in_top3 += 1
                
            # did we get *all* actual top-3 in predicted top-4? -> yes or no
            pred_set_4 = set(top4_pred_ids)
            if true_set_3.issubset(pred_set_4):
                top3_in_top4_hits += 1

        # =============== 
        # 4) MRR for the winner
        # ===============
        try:
            rank = group_sorted_pred.index[group_sorted_pred["horse_id"] == top1_true][0] + 1
        except IndexError:
            rank = None
        reciprocal_ranks.append(1/rank if rank is not None else 0)

        # =============== 
        # 5) Prediction std dev
        # ===============
        stddev = group_sorted_pred["prediction"].std()
        if stddev is not None:
            prediction_std_devs.append(stddev)
        
        # =============== 
        # 6) For RMSE & MAE
        # ===============
        all_true_vals.extend(group["relevance"].values)
        all_pred_vals.extend(group["prediction"].values)

    # end for loop

    if total_groups == 0:
        logging.warning("No races with >=2 horses. Metrics not computed.")
        return {}

    # ---------------------
    # Compute Aggregates
    # ---------------------
    avg_top4_accuracy = float(top4_accuracy / total_groups)
    avg_top1_accuracy = float(top1_accuracy / total_groups)

    perfect_order_percentage = float(perfect_order_count / total_groups)

    # top3_in_top3: average fraction
    avg_top3_in_top3 = float(top3_in_top3 / total_groups)
    # top3_in_top4: fraction of races that have all top-3 actual in predicted top-4
    top3_in_top4_fraction = float(top3_in_top4_hits) / float(total_groups)

    MRR = float(np.mean(reciprocal_ranks)) if reciprocal_ranks else 0.0
    prediction_variance = float(np.mean(prediction_std_devs)) if prediction_std_devs else 0.0

    if len(all_true_vals) < 4:
        RMSE = 0.0
        MAE = 0.0
        spearman_corr = 0.0
    else:
        # RMSE
        RMSE = float(np.sqrt(np.mean((np.array(all_true_vals)-np.array(all_pred_vals))**2)))
        # MAE
        MAE = float(mean_absolute_error(all_true_vals, all_pred_vals))
        # Spearman
        spearman_corr = float(stats.spearmanr(all_true_vals, all_pred_vals)[0])

    # No manual sorting of group here
    # y_true = group["relevance"].values
    # y_pred = group["prediction"].values

    # ndcg_3 = ndcg_score([y_true], [y_pred], k=3)
    # ndcg_values_3.append(ndcg_3)

    # NDCG@3
    # NDCG@4
    
    ndcg_values_3 = []
    ndcg_values_4 = []
    
    unique_gids = np.unique(holdout_group_id)
    for gid in unique_gids:
        mask = (holdout_group_id == gid)
        # skip single-horse groups, etc.
        if np.sum(mask) < 4:
            continue
        
        # Grab the "ground truth" and "predictions" in
        # the original, unsorted order
        group_true = holdout_labels[mask].reshape(1, -1)
        group_pred = holdout_preds[mask].reshape(1, -1)
        
        ndcg_val = ndcg_score(group_true, group_pred, k=3)
        ndcg_values_3.append(ndcg_val)

        ndcg_val4 = ndcg_score(group_true, group_pred, k=4)
        ndcg_values_4.append(ndcg_val4)

    if ndcg_values_3:
        avg_ndcg_3 = float(np.mean(ndcg_values_3))
    else:
        avg_ndcg_3 = 0.0

    if ndcg_values_4:
        avg_ndcg_4 = float(np.mean(ndcg_values_4))
    else:
        avg_ndcg_4 = 0.0

    metrics = {
        "model_key": model_key,
        "total_groups_evaluated": total_groups,

        "avg_top_1_accuracy": avg_top1_accuracy,
        "avg_top_4_accuracy": avg_top4_accuracy,
        "perfect_order_percentage": perfect_order_percentage, # top4 exact sequence
        "avg_top3_in_top3": avg_top3_in_top3,  # partial fraction correct
        "top3_in_top4_fraction": top3_in_top4_fraction,  # all 3 in predicted top4?

        "MRR": MRR,
        "avg_ndcg_3": avg_ndcg_3,
        "avg_ndcg_4": avg_ndcg_4, 

        "spearman_corr": spearman_corr,
        "RMSE": RMSE,
        "MAE": MAE,
        "prediction_variance": prediction_variance
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
    
    metrics = compute_metrics_with_trifecta(holdout_merged, model_key, holdout_preds, holdout_labels, holdout_group_id)

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
            top3_in_top3 = cross_validate_model(
                catboost_loss_functions=loss_func,
                eval_metric=eval_met,
                data_splits=data_splits,
                n_splits=5
            )
            logging.info(f"Cross-validation top3-in-top3 fraction score: {top3_in_top3}")            
            
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
    
    all_feature_cols = final_feature_cols + cat_cols

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
        train_data, valid_data, holdout_data, cat_cols, all_feature_cols)
    
    # 7) minimal catboost training
    catboost_loss_functions = [
        # "YetiRankPairwise"
        "YetiRank",
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

###############################################################################
# Build catboost model - single table approach
###############################################################################
def build_catboost_model(spark, train_df, jdbc_url, jdbc_properties):
    
    # horse_embedding = None
    # queries = horse_embedding_queries()
    # for name, query in queries.items():
    #     if name == "horse_embedding":
    #         logging.info("Query training_data located and loading from PostgreSQL...")
    #         start_time = time.time()
    #         horse_embedding = spark.read.jdbc(
    #             url=jdbc_url,
    #             table=f"({query}) AS subquery",
    #             properties=jdbc_properties
    #         )
    #         logging.info(f"Data loaded from PostgreSQL in {time.time() - start_time:.2f} seconds.")

    # if horse_embedding is None:
    #     logging.error("No training_data query found; train_df is not defined.")
    #     # Handle the error or exit
    # else:
    #     horse_embedding.cache()
    #     rows_horse_embedding = horse_embedding.count()
    #     logging.info(f"Data loaded from PostgreSQL. Count: {rows_horse_embedding}")
    
    # horse_embedding.printSchema()
    # horse_embedding = horse_embedding.toPandas()
    # row_count = horse_embedding.count()
    # logging.info(f"Row count: {row_count}")
    # logging.info(f"Count operation completed in {time.time() - start_time:.2f} seconds.")
    
    print("DEBUG: datetime is =>", datetime)
        
    # Then use Pandas boolean indexing
    historical_pdf = train_df
        
    # Transform the Spark DataFrame to Pandas.
    hist_pdf, cat_cols, excluded_cols = transform_horse_df_to_pandas(historical_pdf, drop_label=False)
    logging.info(f"Shape of historical Pandas DF: {hist_pdf.shape}")


    # Instead of assign_piecewise_log_labels(...)
    hist_pdf = assign_piecewise_log_labels(hist_pdf)
    logging.info(f"Shape of Historical Pandas DF: {hist_pdf.shape}")
        
        # 1) Prepare columns to match training
    final_feature_cols = ["class_rating", "par_time", "prev_speed_rating", "previous_class", "weight",
                          "claimprice", "previous_distance", "prev_official_fin",
                          "power","avgspd", "starts", "avg_spd_sd", "ave_cl_sd", "hi_spd_sd", "pstyerl",
                          "purse", "distance_meters", "morn_odds", "jock_win_percent",
                          "jock_itm_percent", "trainer_win_percent", "trainer_itm_percent", "jt_win_percent",
                          "jt_itm_percent", "jock_win_track", "jock_itm_track", "trainer_win_track", "trainer_itm_track",
                          "jt_win_track", "jt_itm_track", "sire_itm_percentage", "sire_roi", "dam_itm_percentage",
                          "dam_roi", "all_starts", "all_win", "all_place", "all_show", "all_fourth", "all_earnings",
                          "horse_itm_percentage", "cond_starts", "cond_win", "cond_place", "cond_show", "cond_fourth",
                          "cond_earnings", "net_sentiment", "total_races_5", "avg_fin_5", "best_speed",
                          "prev_speed", "speed_improvement", "days_off", "avg_workout_rank_3",
                          "count_workouts_3", "age_at_race_day"] 
    
    # "class_offset", "class_multiplier",
    # "official_distance", "base_speed", "dist_penalty", "horse_mean_rps", "horse_std_rps",
    # "global_speed_score_iq", "race_count_agg", "race_avg_speed_agg", "race_std_speed_agg",
    # "race_avg_relevance_agg", "race_std_relevance_agg", "race_class_count_agg", "race_class_avg_speed_agg",
    # "race_class_min_speed_agg", "race_class_max_speed_agg", "post_position", "avg_purse_val"]

    # 6) Train the model(s) using historical data
    all_models = split_data_and_train(
        df=hist_pdf,
        label_col="relevance",
        cat_cols=cat_cols,
        final_feature_cols=final_feature_cols,
        jdbc_url=jdbc_url,
        jdbc_properties=jdbc_properties,
        spark=spark
    )

    return all_models

def transform_horse_df_to_pandas(hist_df, drop_label=False):
    """
    1) Convert df_spark to Pandas.
    2) 'official_fin' Transform the Label to “Bigger = Better” log-based or exponential-based formula to positions ≥ 5 to make them much less relevant
    3) Create race_id, group_id, convert date columns, etc.
    4) Return the transformed Pandas DataFrame.
    """

    # Check the dtypes in Pandas (should show datetime64[ns] for the above columns)
    # Now you can safely convert to Pandas
    hist_pdf = hist_df.copy()

    # Historical: Create group_id and sort ascending for historical data
    hist_pdf["group_id"] = hist_pdf["race_id"].astype("category").cat.codes
    hist_pdf = hist_pdf.sort_values("group_id", ascending=True).reset_index(drop=True)
    
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

    # Historical: Convert main race_date to datetime
    hist_pdf["race_date"] = pd.to_datetime(hist_pdf["race_date"])
    
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
        "group_id",         # Grouping information, not a feature
    ]

    # Historical: Convert to categorical data type
    for c in cat_cols:
        if c in hist_pdf.columns:
            hist_pdf[c] = hist_pdf[c].astype("category")

    # Return the transformed Pandas DataFrame, along with cat_cols and excluded_cols
    return hist_pdf, cat_cols, excluded_cols

# Retired function
def assign_piecewise_log_labels(df):
    """
    If official_fin <= 4, assign them to a high relevance band (with small differences).
    Otherwise, use a log-based formula that drops more sharply.
    """
    def _relevance(fin):
        if fin == 1:
            return 150 #40  # Could be 40 for 1st
        elif fin == 2:
            return 125 # 38  # Slightly lower than 1st, but still high
        elif fin == 3:
                    return 100 # 38  # Slightly lower than 1st, but still high
        elif fin == 4:
                    return 75 # 38  # Slightly lower than 1st, but still high
        else:
            alpha = 75.0
            beta  = 5.0
            return alpha / np.log(beta + fin)
    
    df["relevance"] = df["official_fin"].apply(_relevance)
    return df

def cross_validate_model(catboost_loss_functions, eval_metric, data_splits, n_splits=5):
    tscv = TimeSeriesSplit(n_splits=n_splits)
    ndcg_scores = []
    top3_hits_list = []

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
            iterations=1500,                # Fewer iterations
            depth=6,                        # Not too deep to keep training faster
            learning_rate=0.03,
            max_bin=96,                     # Coarser binning to speed up
            bootstrap_type='Bernoulli',
            random_strength=0.5,
            od_type='Iter',                 # Early stopping
            od_wait=100,
            random_seed=42,
            verbose=50
        )
        model.fit(train_pool, eval_set=valid_pool, use_best_model=True, early_stopping_rounds=100)

        # ndcg_score = data_splits.compute_ndcg(model, k=4)
        # ndcg_scores.append(ndcg_score)

    # avg_ndcg_score = np.mean(ndcg_scores)
    # return avg_ndcg_score
        X_valid_fold["prediction"] = model.predict(X_valid_fold[data_splits.all_feature_cols])
            
        # 2) Now group by valid_group_id_fold for the fold
        fold_df = X_valid_fold.copy()
        fold_df["relevance"] = y_valid_fold  # or rename if needed
        fold_df["race_id"] = valid_group_id_fold  # or group_col
        fold_df["horse_id"] = data_splits.train_data["horse_id"].iloc[valid_index].values

        fraction = metric_top4_hit(
            model=model,
            df=fold_df,
            group_col="race_id",
            horse_col="horse_id",
            label_col="relevance",
            feature_cols=data_splits.all_feature_cols
        )
        logging.info(f"metric_top4_hit = {fraction:.4f}")
        top3_hits_list.append(fraction)

        # Return average across folds
    return float(np.mean(top3_hits_list))
