import time
import os
import logging
import datetime
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
import optuna
import tensorflow as tf
from optuna.integration import TFKerasPruningCallback
from tensorflow import keras
import io
from tensorflow.keras import layers
import joblib # Used for encoding horse_id
from sklearn.model_selection import KFold
from sklearn.utils import shuffle
import matplotlib.pyplot as plt
import optuna
import optuna.visualization as viz
from catboost import CatBoostRanker, CatBoostRegressor, CatBoostClassifier, Pool
import numpy as np
import itertools
import pyspark.sql.functions as F
from pyspark.sql.functions import (col, count, row_number, abs, unix_timestamp, mean, 
                                   when, lit, min as F_min, max as F_max , upper, trim,
                                   row_number, mean as F_mean, countDistinct, last, first, when)
from src.data_preprocessing.data_prep1.data_utils import save_parquet


def embed_and_train(spark, parquet_dir):
    # 1. Load Data
    speed_figure = spark.read.parquet("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/speed_figure.parquet")
    
    speed_figure = speed_figure.toPandas()
    
    # 2. Set target_metric as Rank
    unique_horses = speed_figure["horse_id"].unique()
    horse_id_to_idx = {h: i for i, h in enumerate(unique_horses)}
    horse_idx = speed_figure["horse_id"].map(horse_id_to_idx)

    # Use pd.concat to avoid fragmentation
    speed_figure = pd.concat([speed_figure, horse_idx.rename("horse_idx")], axis=1)

    # 3) Select numeric columns for embedding input  - 
                
    embedding_features = [
        "custom_speed_figure","off_finish_last_race","time_behind", "pace_delta_time",
        "all_starts","all_win","all_place","all_show","all_fourth", "horse_itm_percentage",
        "sire_itm_percentage", "sire_roi", "dam_itm_percentage", "dam_roi", "age_at_race_day",
        "power", "speed_rating", "prev_speed_rating", "previous_class", "class_rating", 
        "speed_improvement"  
        # Possibly other horse-level stats
    ]
    
    # Identify target to predict (e.g., finishing position or next speed rating)
    target_col = "perf_target"
    
    # 4) Create X and y arrays
    X_numerical = speed_figure[embedding_features].astype(float).values  # shape: [num_samples, num_numeric_feats]
    X_horse_idx = speed_figure["horse_idx"].values  # shape: [num_samples]
    y = speed_figure[target_col].values  # shape: [num_samples]
    
    # 5) Simple train/val split (use time-based if possible!)
    X_num_train, X_num_val, X_horse_train, X_horse_val, y_train, y_val = train_test_split(
        X_numerical, X_horse_idx, y, test_size=0.2, random_state=42
    )

    # -----------------------------------------------------------------------------
    # Define the dict inputs that Keras expects
    #    train_inputs and val_inputs must exist BEFORE objective() is called
    # -----------------------------------------------------------------------------
    train_inputs = {
        "numeric_input": X_num_train,
        "horse_id_input": X_horse_train
    }
    val_inputs = {
        "numeric_input": X_num_val,
        "horse_id_input": X_horse_val
    }
    
    # 6) Building a Keras Model with an Embedding Layer
    
    num_horses = len(unique_horses)
    embedding_dim = 8  # hyperparameter you can tune
    num_numeric_feats = len(embedding_features)
    logging.info(f"Number of horses: {num_horses}")
    
    # 7) Define horse_id input
                    
    horse_id_input = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
    # Embedding layer for horse IDs
    horse_embedding_layer = layers.Embedding(
        input_dim=num_horses, 
        output_dim=embedding_dim, 
        name="horse_embedding"
    )
    horse_embedded = horse_embedding_layer(horse_id_input)  # shape: (batch, embedding_dim)

    # The embedding output will be 2D [batch_size, embedding_dim].
    # Optionally, you can Flatten() if you want a 1D vector
    horse_embedded = layers.Flatten()(horse_embedded)

    # 8) Define numeric input
    numeric_input = tf.keras.Input(shape=(num_numeric_feats,), name="numeric_input")
    x_numeric = layers.Dense(16, activation="relu")(numeric_input)
    x_numeric = layers.Dense(16, activation="relu")(x_numeric)
    
    # 9) Concatenate the numeric output and the embedding
    combined = layers.Concatenate()([x_numeric, horse_embedded])

    # 10) Final output layer for regression
    output = layers.Dense(1, activation="linear", name="output")(combined)

    # 5) Build the model
    model = tf.keras.Model(
        inputs=[numeric_input, horse_id_input],
        outputs=output
    )
    
    # 6) Compile the model with MSE or MAE
    model.compile(
        optimizer=keras.optimizers.Adam(learning_rate=0.001),
        loss="mse",  # for regression
        metrics=["mae"] 
    )

    model.summary()
    
    # Let's assume you have these global or pass them in
    # X_num_train, X_horse_train, y_train
    # X_num_val,   X_horse_val,   y_val
    # train_inputs, val_inputs for your dictionary inputs to the model
    # num_horses = number of distinct horse IDs


    # Suppose we have these global data structures available:
    #   X_num_train, X_horse_train, y_train
    #   X_num_val,   X_horse_val,   y_val
    #   train_inputs = {"numeric_input": X_num_train, "horse_id_input": X_horse_train}
    #   val_inputs   = {"numeric_input": X_num_val,   "horse_id_input": X_horse_val}
    #   num_horses, horse_id_to_idx, etc.

    def objective(trial):
        # -----------------------------
        # 1) Hyperparameter Search Space
        # -----------------------------
        embedding_dim = trial.suggest_categorical("embedding_dim", [2, 4, 8, 16, 32, 64])
        n_hidden_layers = trial.suggest_int("n_hidden_layers", 1, 5)
        units = trial.suggest_int("units_per_layer", 16, 512, step=16)
        activation = trial.suggest_categorical("activation", ["relu", "selu", "tanh", "gelu", "softplus"])
        learning_rate = trial.suggest_float("learning_rate", 1e-5, 1e-1, log=True)
        batch_size = trial.suggest_categorical("batch_size", [128, 256, 512, 1024])
        epochs = trial.suggest_int("epochs", 5, 100, step=5)  # up to 100

        # Dropout-related:
        use_dropout = trial.suggest_categorical("use_dropout", [False, True])
        dropout_rate = 0.0
        if use_dropout:
            dropout_rate = trial.suggest_float("dropout_rate", 0.1, 0.5, step=0.1)

        # -----------------------------
        # 2) Build the Keras Model
        # -----------------------------
        # Horse ID input
        horse_id_input = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)

        # Numeric input
        numeric_input = keras.Input(shape=(X_num_train.shape[1],), name="numeric_input")

        # Embedding layer for horse_id
        horse_embedding_layer = layers.Embedding(
            input_dim=num_horses,
            output_dim=embedding_dim,
            name="horse_embedding"
        )
        horse_embedded = horse_embedding_layer(horse_id_input)  
        horse_embedded = layers.Flatten()(horse_embedded)

        # Dense layers for numeric features
        x = numeric_input
        for _ in range(n_hidden_layers):
            x = layers.Dense(units, activation=activation)(x)
            if use_dropout:
                x = layers.Dropout(dropout_rate)(x)

        # Concatenate embedding + numeric branch
        combined = layers.Concatenate()([x, horse_embedded])

        # Final output (regression)
        output = layers.Dense(1, activation="linear")(combined)

        model = keras.Model([numeric_input, horse_id_input], outputs=output)

        # Compile
        model.compile(
            optimizer=keras.optimizers.Adam(learning_rate=learning_rate),
            loss="mse",   # or 'mae' or custom
            metrics=["mae"]
        )

        # -----------------------------
        # 3) Keras Callbacks
        # -----------------------------
        early_stopping = keras.callbacks.EarlyStopping(
            monitor="val_loss",
            patience=5,
            restore_best_weights=True
        )

        pruning_callback = TFKerasPruningCallback(trial, "val_loss")

        # Create a folder for each trial's checkpoints
        trial_base_dir = "./data/trials"
        trial_dir = os.path.join(trial_base_dir, f"trial_{trial.number}")
        os.makedirs(trial_dir, exist_ok=True)

        checkpoint_filepath = os.path.join(trial_dir, "best.weights.h5")
        model_checkpoint = keras.callbacks.ModelCheckpoint(
            filepath=checkpoint_filepath,
            save_weights_only=True,
            monitor="val_loss",
            mode="min",
            save_best_only=True
        )

        # -----------------------------
        # 4) Train / Fit
        # -----------------------------
        history = model.fit(
            train_inputs,
            y_train,
            validation_data=(val_inputs, y_val),
            epochs=epochs,
            batch_size=batch_size,
            callbacks=[early_stopping, pruning_callback, model_checkpoint],
            verbose=2
        )

        # Evaluate on validation set
        val_loss, val_mae = model.evaluate(val_inputs, y_val, verbose=0)

        # Load the best weights from checkpoint (just to be safe)
        model.load_weights(checkpoint_filepath)

        # Return the validation MSE as the objective
        return val_loss

    def run_optuna_study():
        # Use TPE Sampler (common choice) and a median pruner
        sampler = optuna.samplers.TPESampler(seed=42)
        pruner = optuna.pruners.MedianPruner(n_warmup_steps=3)

        # Create a brand-new study with 500 trials (adjust as you like)
        study = optuna.create_study(
            study_name="horse_embedding_search",
            storage="sqlite:///my_optuna_study.db",
            load_if_exists=False,  # Force a fresh study, if you want a new run each time
            direction="minimize",
            sampler=sampler,
            pruner=pruner
        )

        study.optimize(objective, n_trials=500, timeout=None)

        print("Number of finished trials: ", len(study.trials))
        print("Best trial:")
        trial = study.best_trial
        print("  Value (Val MSE):", trial.value)
        for k, v in trial.params.items():
            print(f"    {k}: {v}")

        # Save all results to CSV for record
        df = study.trials_dataframe()
        df.to_csv("optuna_study_results.csv", index=False)

        return study

    # ---------------
    # Running it:
    study = run_optuna_study()

    # 5) Train Final Model With Best Hyperparams
    best_params = study.best_params
    print("Best Params:\n", best_params)

    embedding_dim   = best_params["embedding_dim"]
    n_hidden_layers = best_params["n_hidden_layers"]
    units           = best_params["units_per_layer"]
    activation      = best_params["activation"]
    learning_rate   = best_params["learning_rate"]
    batch_size      = best_params["batch_size"]
    epochs          = best_params["epochs"]
    use_dropout     = best_params["use_dropout"]
    dropout_rate    = 0.0
    if use_dropout:
        dropout_rate = best_params["dropout_rate"]

    # Rebuild final model architecture
    horse_id_input = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
    numeric_input  = keras.Input(shape=(X_num_train.shape[1],), name="numeric_input")

    horse_embedding_layer = layers.Embedding(input_dim=num_horses, output_dim=embedding_dim)
    horse_embedded = layers.Flatten()(horse_embedding_layer(horse_id_input))

    x = numeric_input
    for _ in range(n_hidden_layers):
        x = layers.Dense(units, activation=activation)(x)
        if use_dropout:
            x = layers.Dropout(dropout_rate)(x)

    combined = layers.Concatenate()([x, horse_embedded])
    output = layers.Dense(1, activation="linear")(combined)
    final_model = keras.Model([numeric_input, horse_id_input], outputs=output)

    final_model.compile(
        optimizer=keras.optimizers.Adam(learning_rate=learning_rate),
        loss="mse",
        metrics=["mae"]
    )

    # Optionally use early stopping in final training, with train+val or just train
    early_stopping = keras.callbacks.EarlyStopping(
        monitor="val_loss",
        patience=5,
        restore_best_weights=True
    )

    final_model.fit(
        train_inputs, y_train,
        validation_data=(val_inputs, y_val),
        epochs=epochs,
        batch_size=batch_size,
        callbacks=[early_stopping],
        verbose=1
    )

    val_loss, val_mae = final_model.evaluate(val_inputs, y_val, verbose=0)
    print(f"Final Model - Val MSE: {val_loss:.4f}, Val MAE: {val_mae:.4f}")

    # 6) Extract the Embeddings
    embedding_weights = horse_embedding_layer.get_weights()[0]  # shape: (num_horses, embedding_dim)

    # Suppose we have a horse_id_to_idx = {horse_id: index}
    idx_to_horse_id = {v: k for k, v in horse_id_to_idx.items()}

    rows = []
    for i in range(num_horses):
        horse_id = idx_to_horse_id[i]
        emb_vec = embedding_weights[i].tolist()
        rows.append([horse_id] + emb_vec)

    embed_cols = ["horse_id"] + [f"embed_{k}" for k in range(embedding_dim)]
    embed_df = pd.DataFrame(rows, columns=embed_cols)
    print("Sample of final embeddings:\n", embed_df.head())

    # Merging Embeddings Back into Your Main Data

    # If your main data is still in df, merge on horse_id:
    df_final = pd.merge(
        speed_figure,       # original DataFrame with race-level rows
        embed_df, # the embedding vectors
        on="horse_id",
        how="left"
    )

    horse_embedding = spark.createDataFrame(df_final)
    
    horse_embedding.printSchema()
    
        # Generate dynamic filename
    current_time = datetime.datetime.now().strftime("%Y-%m-%d-%H%M")
    model_filename = f"horse_embedding_data-{current_time}"
    
    save_parquet(spark, horse_embedding, model_filename, parquet_dir)    
    
    return model_filename