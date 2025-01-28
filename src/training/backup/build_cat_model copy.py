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

def build_catboost_model(spark, horse_embedding):
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
    # Step 4: Use model_filename to load the saved Parquet file
    #parquet_path = os.path.join(parquet_dir, f"{model_filename}.parquet")
    #logging.info(f"Loading Parquet file from: {parquet_path}")

    # Reload the Parquet file into a Spark DataFrame
    # horse_embedding_df = spark.read.parquet(parquet_path)

    logging.info(f"Schema of horse_embedding_df: {horse_embedding.printSchema()}")

    logging.info(f"horse_embedding count: {horse_embedding.count()}")

    # Convert to Pandas
    df = horse_embedding.toPandas()

    df.drop(columns=["official_fin"], inplace=True, errors="ignore")
    # Show basic info
    rows, cols = df.shape
    logging.info(f"Rows: {rows}, Columns: {cols}")
    logging.info(f"Columns: {df.columns.tolist()}")
    logging.info(f"Dtypes:\n{df.dtypes}")

    ###############################################################################
    # 1) Reset grouping
    ###############################################################################
    
    df["race_id"] = (
        df["course_cd"].astype(str) + "_" +
        df["race_date"].astype(str) + "_" +
        df["race_number"].astype(str)
    )
    
    df["group_id"] = df["race_id"].astype("category").cat.codes
    df = df.sort_values("group_id", ascending=True).reset_index(drop=True)
    
    ###############################################################################
    # 1a) Handle certain datetime columns
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

    # ###############################################################################
    # # 6) Define Utility for get_timestamp
    # ###############################################################################
    def get_timestamp():
        return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # ###############################################################################
    # # 7) Define the objective, run_optuna, final training
    # ###############################################################################
    # Already built:
    #   train_pool, valid_pool, holdout_pool
    #   with 'cat_features_idx' from X_train, X_valid, etc.
    # and that cat_cols, X_train, X_valid, X_holdout exist in the outer scope as well.

    def run_optuna(n_trials=50):
        """
        Creates an Optuna study with direction='maximize' for a manual/CPU-based NDCG,
        uses a persistent SQLite DB so we can stop/resume,
        and references train_pool, valid_pool from the outer scope.
        """
        # We'll store Optuna results in data/training so we can pick up if we stop
        study_db = "./data/training/optuna_yetirank_top1_4.db"
        study_name = "catboost_YetiRank_top3_20250127_2130_study"

        # Create or load an existing study from the DB
        study = optuna.create_study(
            study_name=study_name,
            storage=f"sqlite:///{study_db}",
            load_if_exists=True,          # resume if DB exists
            direction="maximize",
            pruner=MedianPruner(n_warmup_steps=3)
        )

        def objective_inner(trial):
            """
            Expanded hyperparameter search for YetiRank:top=3 with optimized memory usage.
            """
            params = {
                "loss_function": "YetiRank:top=3",
                "task_type": "GPU",
                "devices": "0:0.8,1:0.8",  # Restrict GPU memory usage to 80%
                "iterations": trial.suggest_int("iterations", 500, 1500, step=500),

                # Depth
                "depth": trial.suggest_categorical("depth", [2, 4, 6, 8]),

                # Learning rate (log scale)
                "learning_rate": trial.suggest_float("learning_rate", 1e-3, 0.5, log=True),

                # L2 regularization
                "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1.0, 20.0),

                # No bootstrap
                "bootstrap_type": "No",

                # Fixed values
                "random_seed": 42,
                "verbose": 100,

                # Grow policy
                "grow_policy": trial.suggest_categorical(
                    "grow_policy", ["SymmetricTree", "Depthwise", "Lossguide"]
                ),

                # Parameters affecting memory usage
                "border_count": trial.suggest_int("border_count", 32, 128, step=32),
                "random_strength": trial.suggest_float("random_strength", 0.0, 10.0),

                # Early stopping
                "od_type": "Iter",
                "od_wait": trial.suggest_int("od_wait", 20, 50, step=10),
            }

            # Conditional logic for grow_policy
            if params["grow_policy"] == "Lossguide":
                # Tune max_leaves only for Lossguide
                params["max_leaves"] = trial.suggest_int("max_leaves", 32, 128, step=32)

            # Build and train the model
            try:
                model = CatBoostRanker(**params)
                model.fit(
                    train_pool,
                    eval_set=valid_pool,
                    early_stopping_rounds=50
                )
            except Exception as e:
                if "NCudaLib" in str(e):
                    logging.error(f"GPU memory error during training: {e}")
                    return float("inf")  # Return a high loss to skip the trial
                else:
                    raise  # Re-raise other exceptions

                logging.error(f"Out of memory during trial {trial.number}: {e}")
                return float("inf")  # Return a high loss to skip this trial

            # Compute NDCG manually if not in score_dict
            val_preds = model.predict(valid_pool)
            manual_y = valid_pool.get_label()
            manual_ndcg = ndcg_score([manual_y], [val_preds], k=3)

            return manual_ndcg
        study.optimize(objective_inner, n_trials=n_trials)
        return study

    def train_and_save_model(best_params, save_dir="./data/models/catboost"):
        """
        Train final CatBoostRanker using best_params for YetiRank:top=3, then save model to disk.
        Also references train_pool from the outer scope.
        """
        recognized_params = dict(best_params)
        recognized_params.pop("early_stopping_rounds", None)

        # Force YetiRank:top=3, no bootstrap
        recognized_params["loss_function"] = "YetiRank:top=3"
        recognized_params["bootstrap_type"] = "No"
        recognized_params["random_seed"] = 42
        recognized_params["task_type"]   = "GPU"

        # Build the final model
        final_model = CatBoostRanker(**recognized_params)
        final_model.fit(train_pool, verbose=100)  # references outer scope train_pool

        os.makedirs(save_dir, exist_ok=True)
        timestamp = get_timestamp()
        model_filename = f"catboost_YetiRank_top3_noBootstrap_{timestamp}.cbm"
        model_path = os.path.join(save_dir, model_filename)
        final_model.save_model(model_path)

        print(f"Final model saved to: {model_path}")
        return final_model, model_path

    study = run_optuna(n_trials=50)

    best_score  = study.best_value
    best_params = study.best_params
    print("Best NDCG@3:", best_score)
    print("Best Params:", best_params)

    # Save best params
    os.makedirs("./data/training", exist_ok=True)
    params_path = "./data/training/best_params.json"
    with open(params_path, "w") as fp:
        json.dump(best_params, fp, indent=2)
    print(f"Saved best params to: {params_path}")

    # 3) train final model with best params
    final_model, model_path = train_and_save_model(best_params, "./data/models/catboost")
    logging.info(f"Saved final model to: {model_path}")

    model_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/catboost/catboost_YetiRank_top3_noBootstrap_20250127_153507.cbm"
