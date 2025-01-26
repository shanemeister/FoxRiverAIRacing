import io
import json
import os
import logging
import datetime
import pandas as pd
import optuna
from optuna.pruners import MedianPruner
from catboost import CatBoostRanker, Pool
import numpy as np
from sklearn.metrics import ndcg_score

from src.data_preprocessing.data_prep1.data_utils import save_parquet

def build_catboost_model(spark): # (spark, parquet_dir, model_filename):
    """
    1) Reads a Parquet from Spark,
    2) Converts to Pandas,
    3) Splits data chronologically into train/valid/holdout,
    4) Defines numeric/cat columns,
    5) Creates CatBoost Pools,
    6) Runs Optuna to find best hyperparams,
    7) Trains final model & saves it.

    :param spark: SparkSession
    :param parquet_dir: Directory containing the parquet file
    :param model_filename: The base file name of the parquet to load
    :return: Path to final trained CatBoost model, or info as needed
    """

    ###############################################################################
    # 0) Read Spark DF, Convert to Pandas
    ###############################################################################
    full_path = f"/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/horse_embedding_data-2025-01-25-2309.parquet" #os.path.join(parquet_dir, model_filename)

    horse_embedding_sdf = spark.read.parquet(full_path)
    logging.info("Schema of horse_embedding_sdf:")
    horse_embedding_sdf.printSchema()

    logging.info(f"horse_embedding count: {horse_embedding_sdf.count()}")

    # Convert to Pandas
    df = horse_embedding_sdf.toPandas()

    # Show basic info
    rows, cols = df.shape
    logging.info(f"Rows: {rows}, Columns: {cols}")
    logging.info(f"Columns: {df.columns.tolist()}")
    logging.info(f"Dtypes:\n{df.dtypes}")

    ###############################################################################
    # 1) Handle certain datetime columns
    ###############################################################################
    datetime_columns = ["first_race_date_5", "most_recent_race_5", "prev_race_date"]
    # Convert each datetime column, but store the "numeric" versions in a dict of new columns
    new_numeric_cols = {}
    for col in datetime_columns:
        df[col] = pd.to_datetime(df[col])
        new_numeric_cols[col + "_numeric"] = (df[col] - pd.Timestamp("1970-01-01")).dt.days

    # Drop all original datetime columns at once:
    df.drop(columns=datetime_columns, inplace=True, errors="ignore")

    # Now concat all new numeric columns in a single step
    df = pd.concat([df, pd.DataFrame(new_numeric_cols, index=df.index)], axis=1)

    ###############################################################################
    # 2) Split Data Chronologically (Train up to 2023-12-31, etc.)
    ###############################################################################
    df["race_date"] = pd.to_datetime(df["race_date"])

    train_end_date = pd.to_datetime("2023-12-31")
    valid_data_end_date = pd.to_datetime("2024-06-30")
    holdout_start = pd.to_datetime("2024-07-01")

    # Note: check your date logic carefully. 
    train_data = df[df["race_date"] <= train_end_date].copy()
    valid_data = df[(df["race_date"] > train_end_date) & (df["race_date"] <= valid_data_end_date)].copy()
    holdout_data = df[df["race_date"] >= holdout_start].copy()

    print(f"Train shape: {train_data.shape}")
    print(f"Valid shape: {valid_data.shape}")
    print(f"Holdout shape: {holdout_data.shape}")

    logging.info(f"Train shape: {train_data.shape}")
    logging.info(f"Valid shape: {valid_data.shape}")
    logging.info(f"Holdout shape: {holdout_data.shape}")

    ###############################################################################
    # 3) Identify Categorical and Numeric Columns
    ###############################################################################
    cat_cols = [
            "course_cd", "trk_cond", "sex", "equip", "surface", "med",
            "race_type", "stk_clm_md", "turf_mud_mark", "layoff_cat",
            "previous_surface"
        ]
        # Removed duplicate "surface" from that list if it was repeated.

    label_col = "perf_target"

    # We'll add the embed_0..embed_63 columns if they exist
    embed_cols = [f"embed_{i}" for i in range(64)]

    # numeric_cols: all float64/int64 except label_col
    # We'll define them for each sub-data because they differ after merges/drops
    # For now, let's define them from train_data
    def get_numeric_cols(df_):
        # Potential numeric
        cand = [c for c in df_.columns if df_[c].dtype in [np.float64, np.int64]]
        # Exclude the label col
        cand = [c for c in cand if c != label_col]
        return cand

    numeric_cols_train = get_numeric_cols(train_data)
    # add embed columns if they're not already included 
    for ec in embed_cols:
        if ec not in numeric_cols_train and ec in train_data.columns:
            numeric_cols_train.append(ec)

    # We'll do the same or similar for valid_data, holdout_data if needed

    logging.info(f"Numeric Cols (train): {numeric_cols_train}")

    # We'll do the same or similar for valid_data, holdout_data if needed

    ###############################################################################
    # 4) Sort by group_id if it exists, then create X,y,group
    ###############################################################################
    if "group_id" in train_data.columns:
        train_data.sort_values("group_id", ascending=True, inplace=True)
    if "group_id" in valid_data.columns:
        valid_data.sort_values("group_id", ascending=True, inplace=True)
    if "group_id" in holdout_data.columns:
        holdout_data.sort_values("group_id", ascending=True, inplace=True)
        
    all_training_cols = numeric_cols_train + cat_cols
    # remove duplicates
    all_training_cols = list(set(all_training_cols))

    # Build X,y for train
    X_train = train_data.drop(columns=[label_col], errors="ignore")
    X_train = train_data[all_training_cols].copy()
    y_train = train_data[label_col]
    if "group_id" in train_data.columns:
        train_group_id = train_data["group_id"]
    else:
        train_group_id = pd.Series(np.zeros(len(train_data)), index=train_data.index)

    # Build X,y for valid
    X_valid = valid_data.drop(columns=[label_col], errors="ignore")
    X_valid = valid_data[all_training_cols].copy()
    y_valid = valid_data[label_col]
    if "group_id" in valid_data.columns:
        valid_group_id = valid_data["group_id"]
    else:
        valid_group_id = pd.Series(np.zeros(len(valid_data)), index=valid_data.index)

    # Build X,y for holdout
    X_holdout = holdout_data.drop(columns=[label_col], errors="ignore")
    X_holdout = holdout_data[all_training_cols].copy()
    y_holdout = holdout_data[label_col]
    if "group_id" in holdout_data.columns:
        holdout_group_id = holdout_data["group_id"]
    else:
        holdout_group_id = pd.Series(np.zeros(len(holdout_data)), index=holdout_data.index)
    ###############################################################################
    # 5) Create CatBoost Pools
    ###############################################################################
    # cat_features_idx: indices in X_train that match cat_cols
    # We only do it for the columns that truly exist in X_train
    train_col_list = X_train.columns.tolist()
    cat_features_idx = [train_col_list.index(c) for c in cat_cols if c in train_col_list]

    train_pool = Pool(
        data=X_train,
        label=y_train,
        group_id=train_group_id,
        cat_features=cat_features_idx
    )

    valid_pool = Pool(
        data=X_valid,
        label=y_valid,
        group_id=valid_group_id,
        cat_features=cat_features_idx
    )

    holdout_pool = Pool(
        data=X_holdout,
        label=y_holdout,
        group_id=holdout_group_id,
        cat_features=cat_features_idx
    )

    print(f"X_train shape: {X_train.shape}, y_train length: {len(y_train)}")
    print(f"X_valid shape: {X_valid.shape}, y_valid length: {len(y_valid)}")
    print(f"X_holdout shape: {X_holdout.shape}, y_holdout length: {len(y_holdout)}")

    logging.info(f"X_train shape: {X_train.shape}, y_train length: {len(y_train)}")
    logging.info(f"X_valid shape: {X_valid.shape}, y_valid length: {len(y_valid)}")
    logging.info(f"X_holdout shape: {X_holdout.shape}, y_holdout length: {len(y_holdout)}")

    ###############################################################################
    # 6) Define Utility for get_timestamp
    ###############################################################################
    def get_timestamp():
        return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    ###############################################################################
    # 7) Define the objective, run_optuna, final training
    ###############################################################################
    def objective(trial):
        """Objective function for Optuna hyperparam search on CatBoost YetiRank:top=3."""
        params = {
            "loss_function": "YetiRank:top=3",
            # "eval_metric": "NDCG:top=3",
            "task_type": "GPU",
            "iterations": trial.suggest_int("iterations", 500, 3000, step=500),
            "depth": trial.suggest_int("depth", 4, 12),
            "learning_rate": trial.suggest_float("learning_rate", 1e-3, 0.3, log=True),
            "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1.0, 20.0),
            "random_seed": 42,
            # Set a train_dir so CatBoost logs go to our data/catboost_info folder
            "train_dir": "./data/catboost_info",
            "verbose": 100,
        }

        # Grow policy
        grow_policy = trial.suggest_categorical("grow_policy", ["SymmetricTree", "Depthwise", "Lossguide"])
        params["grow_policy"] = grow_policy
        if grow_policy == "Lossguide":
            params["max_leaves"] = trial.suggest_int("max_leaves", 32, 256, step=32)

        # Bootstrap type
        bootstrap_type = trial.suggest_categorical("bootstrap_type", ["Bayesian", "Bernoulli", "Poisson", "MVS"])
        params["bootstrap_type"] = bootstrap_type

        if bootstrap_type == "Bernoulli":
            params["subsample"] = trial.suggest_float("subsample", 0.5, 1.0)
        elif bootstrap_type == "Bayesian":
            params["bagging_temperature"] = trial.suggest_float("bagging_temperature", 0.0, 2.0)

        # random_strength
        params["random_strength"] = trial.suggest_float("random_strength", 0.0, 20.0)

        # border_count
        params["border_count"] = trial.suggest_int("border_count", 32, 512, step=32)

        # early stopping
        params["od_type"] = "Iter"
        params["od_wait"] = trial.suggest_int("od_wait", 20, 100, step=10)

        # Build and train the model
        model = CatBoostRanker(**params)
        model.fit(train_pool, eval_set=valid_pool)

        val_preds = model.predict(valid_pool)
        true_labels = y_valid.values.reshape(1, -1)
        predicted_scores = val_preds.reshape(1, -1)
        valid_ndcg = ndcg_score(true_labels, predicted_scores, k=3)
        return valid_ndcg

    def run_optuna_search(n_trials=300):
        """
        Create an Optuna study with a persistent SQLite storage so we can
        stop and resume the hyperparameter search as needed.
        """
        study_db = "./data/training/optuna_yeti_top3.db"  # store in data/training
        study_name = "catboost_yeti_top3_study"

        # This ensures we can stop & resume
        study = optuna.create_study(
            study_name=study_name,
            storage=f"sqlite:///{study_db}",
            load_if_exists=True,                # resume if DB exists
            direction="maximize",
            pruner=MedianPruner(n_warmup_steps=3)
        )
        study.optimize(objective, n_trials=n_trials)
        return study

    def train_and_save_model(best_params, model_save_dir):
        """Train final model with best_params, then save to .cbm."""
        recognized_params = dict(best_params)
        recognized_params["loss_function"] = "YetiRank:top=3"
        recognized_params["eval_metric"]   = "NDCG:top=3"
        recognized_params["random_seed"]   = 42
        recognized_params["task_type"]     = "GPU"
        recognized_params["train_dir"]     = "./data/catboost_info"  # store logs

        final_model = CatBoostRanker(**recognized_params)
        final_model.fit(
            train_pool,
            eval_set=valid_pool,
            early_stopping_rounds=100,
            verbose=100
        )

        os.makedirs(model_save_dir, exist_ok=True)
        timestamp = get_timestamp()
        model_filename = f"catboost_YetiRank_top3_{timestamp}.cbm"
        model_path = os.path.join(model_save_dir, model_filename)
        final_model.save_model(model_path)
        logging.info(f"Final model saved to: {model_path}")
        return final_model, model_path

    ###############################################################################
    # 8) Run the hyperparameter search
    ###############################################################################
    logging.info("=== Starting Optuna search for YetiRank:top=3 / NDCG:top=3 ===")
    study = run_optuna_search(n_trials=300)  # or 500 if you want bigger search

    best_score = study.best_value
    best_params = study.best_params
    logging.info(f"Best NDCG@3: {best_score}, Best params: {best_params}")

    # Save best params to data/training
    os.makedirs("./data/training", exist_ok=True)
    with open("./data/training/best_params.json", "w") as f:
        json.dump(best_params, f, indent=2)
    
    ###############################################################################
    # 9) Train final model with best params
    ###############################################################################
    final_model, model_path = train_and_save_model(best_params, "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/catboost")
    # Train final model with best params
    logging.info(f"Saved final model to: {model_path}")

    # Evaluate on holdout
    holdout_preds  = final_model.predict(holdout_pool)
    holdout_true   = y_holdout.values.reshape(1, -1)
    holdout_scores = holdout_preds.reshape(1, -1)
    holdout_ndcg   = ndcg_score(holdout_true, holdout_scores, k=3)
    logging.info(f"Holdout NDCG@3: {holdout_ndcg:.4f}")

    logging.info("=== Done training CatBoost model ===")