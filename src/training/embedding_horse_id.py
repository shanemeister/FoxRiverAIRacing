import logging
import datetime
import os
import optuna
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from tensorflow import keras
from tensorflow.keras import layers
from optuna.integration import TFKerasPruningCallback
import tensorflow as tf
from src.data_preprocessing.data_prep1.data_utils import save_parquet

def embed_and_train(spark, jdbc_url, parquet_dir, jdbc_properties, conn, speed_figure):
    # ---------------------------------------------------
    # 1) LOAD AND PREPARE DATA
    # ---------------------------------------------------
    speed_figure_spark = spark.read.parquet("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/speed_figure.parquet")
    speed_figure = speed_figure_spark.toPandas()

    # Map each horse_id to a unique integer index (horse_idx)
    unique_horses = speed_figure["horse_id"].unique()
    horse_id_to_idx = {h: i for i, h in enumerate(unique_horses)}
    horse_idx_series = speed_figure["horse_id"].map(horse_id_to_idx)

    # Concat horse_idx into DataFrame
    speed_figure = pd.concat([speed_figure, horse_idx_series.rename("horse_idx")], axis=1)

    # Numeric features + target
    embedding_features = [
        "custom_speed_figure","off_finish_last_race","time_behind","pace_delta_time",
        "all_starts","all_win","all_place","all_show","all_fourth","horse_itm_percentage",
        "sire_itm_percentage","sire_roi","dam_itm_percentage","dam_roi","age_at_race_day",
        "power","speed_rating","prev_speed_rating","previous_class","class_rating", 
        "speed_improvement","avg_dist_bk_gate1_5","avg_dist_bk_gate2_5","avg_dist_bk_gate3_5",
        "avg_dist_bk_gate4_5","avg_speed_fullrace_5","avg_stride_length_5","avg_strfreq_q1_5",
        "avg_strfreq_q2_5","avg_strfreq_q3_5","avg_strfreq_q4_5"
    ]
    target_col = "perf_target"

    X_numerical = speed_figure[embedding_features].astype(float).values
    X_horse_idx = speed_figure["horse_idx"].values
    y = speed_figure[target_col].values

    # Simple train/val split
    X_num_train, X_num_val, X_horse_train, X_horse_val, y_train, y_val = train_test_split(
        X_numerical, X_horse_idx, y, test_size=0.2, random_state=42
    )

    train_inputs = {
        "numeric_input": X_num_train,
        "horse_id_input": X_horse_train
    }
    val_inputs = {
        "numeric_input": X_num_val,
        "horse_id_input": X_horse_val
    }

    num_horses = len(unique_horses)
    num_numeric_feats = len(embedding_features)

    logging.info(f"Number of horses: {num_horses}")
    logging.info(f"Number of numeric features: {num_numeric_feats}")

    # ---------------------------------------------------
    # 2) DEFINE OPTUNA OBJECTIVE
    # ---------------------------------------------------
    def objective(trial):
        """
        Builds + trains a Keras model w/ an embedding layer for horse_id.
        Returns validation MSE (val_loss) for Optuna to minimize.
        """
        # Hyperparams
        embedding_dim = trial.suggest_categorical("embedding_dim", [2, 4, 8, 16, 32, 64])
        n_hidden_layers = trial.suggest_int("n_hidden_layers", 1, 5)
        units = trial.suggest_int("units_per_layer", 16, 512, step=16)
        activation = trial.suggest_categorical("activation", ["relu","selu","tanh","gelu","softplus"])
        learning_rate = trial.suggest_float("learning_rate", 1e-4, 1e-2, log=True)
        batch_size = trial.suggest_categorical("batch_size", [128, 256, 512, 1024])
        epochs = trial.suggest_int("epochs", 10, 100, step=10)
        use_dropout = trial.suggest_categorical("use_dropout", [False, True])
        dropout_rate = 0.0
        if use_dropout:
            dropout_rate = trial.suggest_float("dropout_rate", 0.1, 0.5, step=0.1)

        # Keras model
        horse_id_input = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
        numeric_input = keras.Input(shape=(X_num_train.shape[1],), name="numeric_input")

        horse_embedding_layer = layers.Embedding(
            input_dim=num_horses,
            output_dim=embedding_dim,
            name="horse_embedding"
        )
        horse_embedded = horse_embedding_layer(horse_id_input)
        horse_embedded = layers.Flatten()(horse_embedded)

        x = numeric_input
        for _ in range(n_hidden_layers):
            x = layers.Dense(units, activation=activation)(x)
            if use_dropout:
                x = layers.Dropout(dropout_rate)(x)

        combined = layers.Concatenate()([x, horse_embedded])
        output = layers.Dense(1, activation="linear")(combined)

        model = keras.Model([numeric_input, horse_id_input], outputs=output)
        model.compile(
            optimizer=keras.optimizers.Adam(learning_rate=learning_rate),
            loss="mse",
            metrics=["mae"]
        )

        # Callbacks
        early_stopping = keras.callbacks.EarlyStopping(
            monitor="val_loss",
            patience=10,  # More patience for stable training
            restore_best_weights=True
        )
        pruning_callback = TFKerasPruningCallback(trial, "val_loss")

        # Checkpoint folder for each trial
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

        # Train
        model.fit(
            train_inputs, y_train,
            validation_data=(val_inputs, y_val),
            epochs=epochs,
            batch_size=batch_size,
            callbacks=[early_stopping, pruning_callback, model_checkpoint],
            verbose=1  # Show some logs to confirm training progress
        )

        # Evaluate
        val_loss, val_mae = model.evaluate(val_inputs, y_val, verbose=0)
        model.load_weights(checkpoint_filepath)  # ensure best weights loaded
        return val_loss

    # ---------------------------------------------------
    # 3) RUN OPTUNA STUDY
    # ---------------------------------------------------
    def run_optuna_study():
        sampler = optuna.samplers.TPESampler(seed=42)
        # Increase n_warmup_steps => Wait more epochs before pruning
        pruner = optuna.pruners.MedianPruner(n_warmup_steps=10)

        study_name = "horse_embedding_search"
        storage_url = "sqlite:///my_optuna_study.db"

        try:
            study = optuna.create_study(
                study_name=study_name,
                storage=storage_url,
                load_if_exists=False,
                direction="minimize",
                sampler=sampler,
                pruner=pruner
            )
            print(f"Created a new study: {study_name}")
        except optuna.exceptions.DuplicatedStudyError:
            print(f"A study named '{study_name}' already exists.")
            study = optuna.load_study(
                study_name=study_name,
                storage=storage_url
            )
            print(f"Loaded existing study: {study_name}")

        # Actually run the search
        study.optimize(objective, n_trials=100, timeout=None)

        print("Number of completed trials:", len(study.trials))
        best_trial = study.best_trial
        print(f"Best trial Value (Val MSE): {best_trial.value}")
        for k, v in best_trial.params.items():
            print(f"  {k}: {v}")

        # Save all trials to CSV
        df_trials = study.trials_dataframe()
        df_trials.to_csv("optuna_study_results.csv", index=False)
        print("Saved study results to 'optuna_study_results.csv'.")
        return study

    study = run_optuna_study()
    best_params = study.best_params
    print("Best Params:\n", best_params)

    # Extract best hyperparameters
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

    # ---------------------------------------------------
    # Build final model with best hyperparams
    # ---------------------------------------------------
    horse_id_input = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
    numeric_input  = keras.Input(shape=(num_numeric_feats,), name="numeric_input")

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

    # A more relaxed EarlyStopping for final training
    early_stopping_final = keras.callbacks.EarlyStopping(
        monitor="val_loss",
        patience=10,
        restore_best_weights=True
    )

    print("=== Training final model with best hyperparameters ===")
    final_model.fit(
        train_inputs, y_train,
        validation_data=(val_inputs, y_val),
        epochs=epochs,
        batch_size=batch_size,
        callbacks=[early_stopping_final],
        verbose=1
    )

    val_loss, val_mae = final_model.evaluate(val_inputs, y_val, verbose=0)
    print(f"Final Model - Val MSE: {val_loss:.4f}, Val MAE: {val_mae:.4f}")

    # ---------------------------------------------------
    # Extract & Save Embeddings
    # ---------------------------------------------------
    embedding_weights = horse_embedding_layer.get_weights()[0]  # (num_horses, embedding_dim)
    idx_to_horse_id = {v: k for k, v in horse_id_to_idx.items()}

    rows = []
    for i in range(num_horses):
        horse_id = idx_to_horse_id[i]
        emb_vec = embedding_weights[i].tolist()
        rows.append([i, horse_id] + emb_vec)

    embed_cols = ["horse_idx", "horse_id"] + [f"embed_{k}" for k in range(embedding_dim)]
    embed_df = pd.DataFrame(rows, columns=embed_cols)
    print("Sample of final embeddings:\n", embed_df.head())

    # Convert to Spark DataFrame
    horse_embedding_spark = spark.createDataFrame(embed_df)

    # Write to DB table
    staging_table = "horse_embedding"
    logging.info(f"Writing horse embeddings to table: {staging_table}")
    (
        horse_embedding_spark.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", staging_table)
        .option("user", jdbc_properties["user"])
        .option("driver", jdbc_properties["driver"])
        .mode("overwrite")
        .save()
    )
    logging.info("Embeddings saved to DB table 'horse_embedding'.")

    # Merge embeddings into speed_figure if desired
    df_final = pd.merge(speed_figure, embed_df, on="horse_id", how="left")
    df_final_spark = spark.createDataFrame(df_final)

    # Save final DataFrame as Parquet
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    model_filename = f"horse_embedding_data-{current_time}"
    save_parquet(spark, df_final_spark, model_filename, parquet_dir)
    logging.info(f"Final merged DataFrame saved as Parquet: {model_filename}")

    logging.info("*** Horse embedding job completed successfully ***")
    return model_filename