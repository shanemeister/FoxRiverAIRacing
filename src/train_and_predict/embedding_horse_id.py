import logging
import datetime
import os
import optuna
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint, ReduceLROnPlateau, Callback
from tensorflow.keras.regularizers import l2
from optuna.integration import TFKerasPruningCallback
from sklearn.model_selection import train_test_split
import scipy.stats as stats
from src.data_preprocessing.data_prep1.data_utils import save_parquet
from pyspark.sql import functions as F

def embed_and_train(spark, jdbc_url, parquet_dir, jdbc_properties, conn, global_speed_score):
    # ---------------------------------------------------
    # 1) LOAD AND PREPARE DATA
    # ---------------------------------------------------
    # Assume global_speed_score is a Pandas DataFrame or can be converted to one.
    speed_figure = global_speed_score.copy()
    
    print("speed_figure shape:", speed_figure.shape)
    print("unique horse IDs in speed_figure:", speed_figure["horse_id"].nunique())
    
    # Map each horse_id to a unique integer index (horse_idx)
    unique_horses = speed_figure["horse_id"].unique()
    horse_id_to_idx = {h: i for i, h in enumerate(unique_horses)}
    horse_idx_series = speed_figure["horse_id"].map(horse_id_to_idx)
    speed_figure = pd.concat([speed_figure, horse_idx_series.rename("horse_idx")], axis=1)

    # Define numeric features (embedding_features) and target.
    embedding_features = [
        "off_finish_last_race", "time_behind", "pace_delta_time",
        "all_starts", "all_win", "all_place", "all_show", "all_fourth", "horse_itm_percentage",
        "sire_itm_percentage", "sire_roi", "dam_itm_percentage", "dam_roi", "age_at_race_day",
        "power", "speed_rating", "prev_speed_rating", "previous_class", "class_rating", 
        "speed_improvement", "avg_dist_bk_gate1_5", "avg_dist_bk_gate2_5", "avg_dist_bk_gate3_5",
        "avg_dist_bk_gate4_5", "avg_speed_fullrace_5", "avg_stride_length_5", "avg_strfreq_q1_5",
        "avg_strfreq_q2_5", "avg_strfreq_q3_5", "avg_strfreq_q4_5", "global_speed_score", "running_time",
        "base_speed", "wide_factor", "par_diff_ratio", "class_multiplier", "official_distance"
    ]
    
    X_numerical = speed_figure[embedding_features].astype(float).values
    X_horse_idx = speed_figure["horse_idx"].values
    y = speed_figure["official_fin"].values

    # Check for NaNs and infinities.
    df_check = speed_figure[embedding_features].copy()
    print("NaN counts in each feature:")
    print(df_check.isna().sum()[df_check.isna().sum() > 0])
    print("Infinity counts in each feature:")
    print(df_check.apply(lambda col: np.isinf(col).sum())[df_check.apply(lambda col: np.isinf(col).sum()) > 0])
    print("official_fin unique values:", speed_figure["official_fin"].unique())
    print("Min, Max official_fin:", speed_figure["official_fin"].min(), speed_figure["official_fin"].max())
    
    # Train/validation split.
    all_indices = np.arange(len(speed_figure))
    X_train_indices, X_val_indices, _, _ = train_test_split(all_indices, y, test_size=0.2, random_state=42)
    X_num_train = speed_figure.iloc[X_train_indices][embedding_features].astype(float).values
    X_num_val   = speed_figure.iloc[X_val_indices][embedding_features].astype(float).values
    course_cd_train = speed_figure.iloc[X_train_indices]["course_cd"].values
    course_cd_val   = speed_figure.iloc[X_val_indices]["course_cd"].values
    trk_cond_train  = speed_figure.iloc[X_train_indices]["trk_cond"].values
    trk_cond_val    = speed_figure.iloc[X_val_indices]["trk_cond"].values
    X_horse_train = speed_figure.iloc[X_train_indices]["horse_idx"].values
    X_horse_val   = speed_figure.iloc[X_val_indices]["horse_idx"].values
    y_train = speed_figure.iloc[X_train_indices]["official_fin"].values
    y_val   = speed_figure.iloc[X_val_indices]["official_fin"].values

    train_inputs = {
        "numeric_input": X_num_train,
        "horse_id_input": X_horse_train,
        "course_cd_input": course_cd_train,
        "trk_cond_input": trk_cond_train,
    }
    val_inputs = {
        "numeric_input": X_num_val,
        "horse_id_input": X_horse_val,
        "course_cd_input": course_cd_val,
        "trk_cond_input": trk_cond_val,
    }
    
    num_horses = len(unique_horses)
    num_numeric_feats = len(embedding_features)
    logging.info(f"Number of horses: {num_horses}")
    logging.info(f"Number of numeric features: {num_numeric_feats}")

    # ---------------------------
    # Custom Ranking Metric Callback.
    # ---------------------------
    class RankingMetricCallback(Callback):
        def __init__(self, val_data):
            """
            val_data should be a tuple of:
                (val_numeric, val_horse, val_course_cd, val_trk_cond, val_y)
            """
            super().__init__()
            self.val_numeric, self.val_horse, self.val_course_cd, self.val_trk_cond, self.val_y = val_data

        def on_epoch_end(self, epoch, logs=None):
            preds = self.model.predict([self.val_numeric, self.val_horse, self.val_course_cd, self.val_trk_cond])
            corr, _ = stats.spearmanr(self.val_y, preds.flatten())
            logs["val_spearman"] = corr
            print(f" - val_spearman: {corr:.4f}")

    # ---------------------------
    # Define the Optuna objective function.
    # ---------------------------
    def objective(trial):
        # Choose feature subset as a hyperparameter.
        feature_set_choice = trial.suggest_categorical("feature_set", ["all_features", "core_speed", "race_context"])
        if feature_set_choice == "all_features":
            features = embedding_features
        elif feature_set_choice == "core_speed":
            features = ["custom_speed_figure", "global_speed_score", "base_speed", "wide_factor"]
        elif feature_set_choice == "race_context":
            features = ["off_finish_last_race", "time_behind", "pace_delta_time"]

        X_num_train_subset = speed_figure.iloc[X_train_indices][features].astype(float).values
        X_num_val_subset   = speed_figure.iloc[X_val_indices][features].astype(float).values

        train_inputs_subset = {
            "numeric_input": X_num_train_subset,
            "horse_id_input": X_horse_train,
            "course_cd_input": course_cd_train,
            "trk_cond_input": trk_cond_train,
        }
        val_inputs_subset = {
            "numeric_input": X_num_val_subset,
            "horse_id_input": X_horse_val,
            "course_cd_input": course_cd_val,
            "trk_cond_input": trk_cond_val,
        }

        # Hyperparameters for the model.
        embedding_dim_trial = trial.suggest_categorical("embedding_dim", [2, 4, 8, 16, 32, 64])
        n_hidden_layers_trial = trial.suggest_int("n_hidden_layers", 1, 5)
        units_trial = trial.suggest_int("units_per_layer", 16, 512, step=16)
        activation_trial = trial.suggest_categorical("activation", ["relu", "selu", "tanh", "gelu", "softplus"])
        learning_rate_trial = trial.suggest_float("learning_rate", 1e-4, 1e-2, log=True)
        batch_size_trial = trial.suggest_categorical("batch_size", [128, 256, 512, 1024])
        epochs_trial = trial.suggest_int("epochs", 10, 100, step=10)
        use_dropout_trial = trial.suggest_categorical("use_dropout", [False, True])
        dropout_rate_trial = 0.0
        if use_dropout_trial:
            dropout_rate_trial = trial.suggest_float("dropout_rate", 0.1, 0.5, step=0.1)

        categorical_cols_trial = ["course_cd", "trk_cond"]
        cat_inputs_trial = {}
        cat_embeddings_trial = []
        embedding_dims_trial_dict = {"course_cd": 4, "trk_cond": 4}
        for col in categorical_cols_trial:
            lookup = tf.keras.layers.StringLookup(output_mode='int')
            vocab = speed_figure[col].unique().tolist()
            lookup.adapt(vocab)
            input_layer = keras.Input(shape=(), name=f"{col}_input", dtype=tf.string)
            cat_inputs_trial[col] = input_layer
            indices = lookup(input_layer)
            embed_layer = layers.Embedding(input_dim=len(vocab) + 1, output_dim=embedding_dims_trial_dict[col], name=f"{col}_embed")
            embedded = layers.Flatten()(embed_layer(indices))
            cat_embeddings_trial.append(embedded)

        horse_id_input_trial = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
        numeric_input_trial = keras.Input(shape=(X_num_train_subset.shape[1],), name="numeric_input")
        horse_embedding_layer_trial = layers.Embedding(input_dim=num_horses, output_dim=embedding_dim_trial, name="horse_embedding")
        horse_embedded_trial = layers.Flatten()(horse_embedding_layer_trial(horse_id_input_trial))

        def dense_block(x, units, activation, dropout_rate, l2_reg):
            x = layers.Dense(units, activation=activation, kernel_regularizer=l2(l2_reg))(x)
            if dropout_rate > 0:
                x = layers.Dropout(dropout_rate)(x)
            return x

        x_trial = numeric_input_trial
        l2_reg = 1e-4
        for _ in range(n_hidden_layers_trial):
            x_trial = dense_block(x_trial, units_trial, activation_trial, dropout_rate_trial, l2_reg)

        combined_trial = layers.Concatenate()([x_trial, horse_embedded_trial] + cat_embeddings_trial)
        output_trial = layers.Dense(1, activation="linear")(combined_trial)

        model_trial = keras.Model(inputs=[numeric_input_trial, horse_id_input_trial] + list(cat_inputs_trial.values()), outputs=output_trial)
        model_trial.compile(
            optimizer=keras.optimizers.Adam(learning_rate=learning_rate_trial),
            loss="mse",
            metrics=["mae"]
        )
        early_stopping = EarlyStopping(monitor="val_loss", patience=10, restore_best_weights=True)
        pruning_callback = TFKerasPruningCallback(trial, "val_loss")
        trial_base_dir = "./data/trials"
        trial_dir = os.path.join(trial_base_dir, f"trial_{trial.number}")
        os.makedirs(trial_dir, exist_ok=True)
        checkpoint_filepath = os.path.join(trial_dir, "best.weights.h5")
        model_checkpoint = ModelCheckpoint(
            filepath=checkpoint_filepath,
            save_weights_only=True,
            monitor="val_loss",
            mode="min",
            save_best_only=True
        )
        lr_callback_trial = ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=5, min_lr=1e-6, verbose=1)
        ranking_metric_callback_trial = RankingMetricCallback((X_num_val_subset, X_horse_val, course_cd_val, trk_cond_val, y_val))
        
        model_trial.fit(
            train_inputs_subset, y_train,
            validation_data=(val_inputs_subset, y_val),
            epochs=epochs_trial,
            batch_size=batch_size_trial,
            callbacks=[early_stopping, pruning_callback, model_checkpoint, lr_callback_trial, ranking_metric_callback_trial],
            verbose=1
        )
        
        val_loss, val_mae = model_trial.evaluate(val_inputs_subset, y_val, verbose=0)
        model_trial.load_weights(checkpoint_filepath)
        return val_loss

    def run_optuna_study():
        sampler = optuna.samplers.TPESampler(seed=42)
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
            study = optuna.load_study(study_name=study_name, storage=storage_url)
            print(f"Loaded existing study: {study_name}")
        
        study.optimize(objective, n_trials=100, timeout=None)
        print("Number of completed trials:", len(study.trials))
        best_trial = study.best_trial
        print(f"Best trial Value (Val MSE): {best_trial.value}")
        for k, v in best_trial.params.items():
            print(f"  {k}: {v}")
        
        df_trials = study.trials_dataframe()
        df_trials.to_csv("optuna_study_results.csv", index=False)
        print("Saved study results to 'optuna_study_results.csv'.")
        return study

    # Run Optuna study.
    study = run_optuna_study()
    best_params = study.best_params
    print("Best Params:\n", best_params)

    # ---------------------------
    # Build final model with best hyperparameters.
    # ---------------------------
    # Hard-coded best parameters from the Optuna study:
    best_params = {
        "activation": "softplus",
        "batch_size": 128,
        "embedding_dim": 4,
        "epochs": 90,
        "learning_rate": 0.0021337077624273464,
        "n_hidden_layers": 4,
        "units_per_layer": 400,
        "use_dropout": False
    }

    embedding_dim   = best_params["embedding_dim"]
    n_hidden_layers = best_params["n_hidden_layers"]
    units           = best_params["units_per_layer"]
    activation      = best_params["activation"]
    learning_rate   = best_params["learning_rate"]
    batch_size      = best_params["batch_size"]
    epochs          = best_params["epochs"]
    use_dropout     = best_params["use_dropout"]
    dropout_rate    = best_params.get("dropout_rate", 0.0) if use_dropout else 0.0

    # Final model inputs: numeric, horse_id, and additional categorical inputs.
    horse_id_input = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
    numeric_input  = keras.Input(shape=(num_numeric_feats,), name="numeric_input")

    categorical_cols = ["course_cd", "trk_cond"]
    embedding_dims = {"course_cd": 4, "trk_cond": 4}

    cat_inputs = {}
    cat_embeddings = []
    for col in categorical_cols:
        lookup = tf.keras.layers.StringLookup(output_mode='int')
        vocab = speed_figure[col].unique().tolist()
        lookup.adapt(vocab)
        input_layer = keras.Input(shape=(), name=f"{col}_input", dtype=tf.string)
        cat_inputs[col] = input_layer
        indices = lookup(input_layer)
        embed_layer = layers.Embedding(input_dim=len(vocab) + 1, output_dim=embedding_dims[col], name=f"{col}_embed")
        embedded = layers.Flatten()(embed_layer(indices))
        cat_embeddings.append(embedded)

    horse_embedding_layer = layers.Embedding(input_dim=num_horses, output_dim=embedding_dim, name="horse_embedding")
    horse_embedded = layers.Flatten()(horse_embedding_layer(horse_id_input))

    x = numeric_input
    for _ in range(n_hidden_layers):
        x = layers.Dense(units, activation=activation, kernel_regularizer=l2(1e-4))(x)
        if use_dropout:
            x = layers.Dropout(dropout_rate)(x)

    combined = layers.Concatenate()([x, horse_embedded] + cat_embeddings)
    output = layers.Dense(1, activation="linear")(combined)

    final_model = keras.Model(
        inputs=[numeric_input, horse_id_input] + list(cat_inputs.values()),
        outputs=output
    )

    final_model.compile(
        optimizer=keras.optimizers.Adam(learning_rate=learning_rate),
        loss="mse",
        metrics=["mae"]
    )

    early_stopping_final = keras.callbacks.EarlyStopping(monitor="val_loss", patience=10, restore_best_weights=True)
    lr_callback = keras.callbacks.ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=5, min_lr=1e-6, verbose=1)
    ranking_metric_callback = RankingMetricCallback((X_num_val, X_horse_val, course_cd_val, trk_cond_val, y_val))

    print("=== Training final model with best hyperparameters ===")
    final_model.fit(
        train_inputs, y_train,
        validation_data=(val_inputs, y_val),
        epochs=epochs,
        batch_size=batch_size,
        callbacks=[early_stopping_final, lr_callback, ranking_metric_callback],
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

    df_final = pd.merge(speed_figure, embed_df, on="horse_id", how="left")
    horse_embedding_spark = spark.createDataFrame(df_final)

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

    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    model_filename = f"horse_embedding_data-{current_time}"
    save_parquet(spark, horse_embedding_spark, model_filename, parquet_dir)
    logging.info(f"Final merged DataFrame saved as Parquet: {model_filename}")

    logging.info("*** Horse embedding job completed successfully ***")
    return model_filename