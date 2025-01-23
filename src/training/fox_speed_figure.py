import logging
import os
import sys
import configparser
import pandas as pd
import numpy as np
import optuna
from catboost import CatBoostRegressor, Pool
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.metrics import roc_auc_score, accuracy_score
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from src.data_preprocessing.data_prep1.data_utils import save_parquet, initialize_environment
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql
from src.training.training_sql_queries import sql_queries

def create_custom_speed_figure(df):
    """
    Build a custom speed figure using a CatBoostRegressor.
    """
    # If df is a Spark DataFrame, convert to Pandas first:
    # If it's already Pandas, comment out or remove the line below
    df = df.toPandas()

    # Create race_id for grouping
    df["race_number"] = df["race_number"].astype(float)
    
    df["race_id"] = (
        df["course_cd"].astype(str) + "_" +
        df["race_date"].astype(str) + "_" +
        df["race_number"].astype(str)
    )
    
    df["group_id"] = df["race_id"].astype("category").cat.codes
    df = df.sort_values("group_id", ascending=True).reset_index(drop=True)
    # Convert decimal columns
    decimal_cols = [
        'distance_meters', 'class_rating', 'previous_class', 'power',
        'horse_itm_percentage', 'starts', 'official_fin',
        'time_behind', 'pace_delta_time'
    ]
    for col_name in decimal_cols:
        df[col_name] = df[col_name].astype(float)
    # Fill NaNs
    df[decimal_cols] = df[decimal_cols].fillna(0)
    
    logging.info("Decimal columns converted to float and filled NaN with 0.")
    logging.info(f"Columns: {df.dtypes}")

    # Map finishing position to a performance target
    rank_map = {
        1: 20,
        2: 19,
        3: 18,
        4: 17,
        5: 16,
        6: 15,
        7: 14,
        8: 13,
        9: 12,
        10: 11,
        11: 10,
        12: 9,
        13: 8,
        14: 7,
        15: 0  # Default for ranks 15 and below
    }

    df["perf_target"] = df["official_fin"].map(rank_map).fillna(0).astype(int)

    # Features for CatBoost
    numeric_features = [
        "distance_meters",
        "time_behind",
        "pace_delta_time",
        "speed_rating",
        "class_rating",
        "previous_class",
        "power",
        "starts",
        "horse_itm_percentage",
    ]

    X = df[numeric_features]
    y = df["perf_target"]

    # ------------------------
    # Train/Validation split
    # ------------------------
    X_train, X_val, y_train, y_val = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42
    )

    train_pool = Pool(X_train, label=y_train)
    val_pool   = Pool(X_val,   label=y_val)

    # ------------------------
    # Define Optuna objective
    # ------------------------
    def objective(trial):
        params = {
            'iterations': trial.suggest_int('iterations', 200, 2000),
            'depth': trial.suggest_int('depth', 3, 8),
            'learning_rate': trial.suggest_float('learning_rate', 1e-3, 0.3, log=True),
            'l2_leaf_reg': trial.suggest_float('l2_leaf_reg', 1e-3, 10.0, log=True),
            'random_seed': 42,
            'loss_function': 'RMSE',
            'verbose': 0
        }
        
        model = CatBoostRegressor(**params)
        
        model.fit(
            train_pool,
            eval_set=val_pool,
            early_stopping_rounds=50,
            use_best_model=True
        )

        y_pred = model.predict(val_pool)
        y_true = val_pool.get_label()

        # If scikit-learn is older, it doesn't support squared=False, so do a manual sqrt:
        mse = mean_squared_error(y_true, y_pred)
        rmse = np.sqrt(mse)
        return rmse

    # -----------------------------
    # Run Optuna study
    # -----------------------------
    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=30)
    best_params = study.best_params
    logging.info(f"Best Hyperparameters: {best_params}")
    logging.info(f"Best RMSE: {study.best_value}")

    # ----------------------------------------
    # Retrain final model with best params
    # Option A: Train on the full dataset
    # Option B: Train on only train_pool
    # ----------------------------------------
    final_model = CatBoostRegressor(
        **best_params,
        loss_function='RMSE',
        random_seed=42,
        verbose=50
    )
    # Here we show training on the full X,y for a final model:
    full_pool = Pool(X, label=y)
    final_model.fit(full_pool)

    # Create the custom_speed_figure from the final model
    df["custom_speed_figure"] = final_model.predict(full_pool)
      
    y_pred = final_model.predict(X_val)
    mse = mean_squared_error(y_val, y_pred)
    rmse = np.sqrt(mse)
    mae = mean_absolute_error(y_val, y_pred)

    print("RMSE:", rmse)
    print("MAE:", mae)

    # Check correlations
    for col in numeric_features:
        corr = df[col].corr(df["custom_speed_figure"])
        print(f"Correlation between {col} and custom_speed_figure: {corr}")

    final_model.save_model("data/models/speed_figure_model/speed_figure_regressor_model_2025-01-21.cbm")
    
    return df
