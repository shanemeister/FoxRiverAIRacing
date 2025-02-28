import logging
import datetime
import os
import optuna
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau, Callback
from tensorflow.keras.regularizers import l2
from optuna.integration import TFKerasPruningCallback
from sklearn.model_selection import train_test_split
from pyspark.sql.types import DoubleType, IntegerType
import scipy.stats as stats
from src.data_preprocessing.data_prep1.data_utils import save_parquet
from pyspark.sql import functions as F, Window

# ---------------------------
# Custom Callback for Ranking Metric
# ---------------------------
class RankingMetricCallback(Callback):
    def __init__(self, val_data_dict):
        """
        val_data_dict is a dictionary with keys:
          "horse_id_val", "horse_stats_val", "race_numeric_val",
          "course_cd_val", "trk_cond_val", "y_val"
        """
        super().__init__()
        self.val_data_dict = val_data_dict

    def on_epoch_end(self, epoch, logs=None):
        preds = self.model.predict({
            "horse_id_input":     self.val_data_dict["horse_id_val"],
            "horse_stats_input":  self.val_data_dict["horse_stats_val"],
            "race_numeric_input": self.val_data_dict["race_numeric_val"],
            "course_cd_input":    self.val_data_dict["course_cd_val"],
            "trk_cond_input":     self.val_data_dict["trk_cond_val"]
        })
        y_true = self.val_data_dict["y_val"]
        corr, _ = stats.spearmanr(y_true, preds.flatten())
        logs = logs or {}
        logs["val_spearman"] = corr
        print(f" - val_spearman: {corr:.4f}")

# ---------------------------
# Helper: check_nan_inf
# ---------------------------
def check_nan_inf(name, arr):
    if np.issubdtype(arr.dtype, np.floating) or np.issubdtype(arr.dtype, np.integer):
        nan_count = np.isnan(arr).sum()
        inf_count = np.isinf(arr).sum()
        print(f"[CHECK] {name}: nan={nan_count}, inf={inf_count}, shape={arr.shape}")
    else:
        print(f"[CHECK] {name}: (skipped - not numeric), dtype={arr.dtype}")

def assign_labels_spark(df, alpha=0.8):
    """
    Adds two columns to the Spark DataFrame:
      1) relevance: Exponential label computed as alpha^(official_fin - 1)
      2) top4_label: 1 if official_fin <= 4, else 0
    This update is applied only for rows where official_fin is not null.
    
    Parameters:
      df (DataFrame): A Spark DataFrame with an 'official_fin' column.
      alpha (float): Base of the exponential transformation.
    
    Returns:
      DataFrame: The input DataFrame with new columns 'relevance' and 'top4_label'.
    """
    df = df.withColumn(
        "relevance",
        F.when(
            F.col("official_fin").isNotNull(),
            F.pow(F.lit(alpha), F.col("official_fin") - 1)
        ).otherwise(F.lit(None).cast(DoubleType()))
    ).withColumn(
        "top4_label",
        F.when(
            F.col("official_fin").isNotNull(),
            F.when(F.col("official_fin") <= 4, F.lit(1)).otherwise(F.lit(0))
        ).otherwise(F.lit(None).cast(IntegerType()))
    )
    return df

# ---------------------------
# Helper: attach_recent_speed_figure
# ---------------------------
def attach_recent_speed_figure(df_final, enriched_df_spark):
    """
    We assume `enriched_df_spark` is the same DataFrame that
    actually has 'combined_4' (and the older race dates, etc.).
    """
    hist_subset = (
        enriched_df_spark
        .withColumnRenamed("race_date", "hist_race_date")
        .select("horse_id", "hist_race_date", "combined_4")
    )
    w = Window.partitionBy("f.horse_id", "f.race_date").orderBy(F.desc("h.hist_race_date"))
    joined = (
        df_final.alias("f")
        .join(
            hist_subset.alias("h"),
            on=( (F.col("f.horse_id") == F.col("h.horse_id"))
                 & (F.col("h.hist_race_date") <= F.col("f.race_date")) ),
            how="left"
        )
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
    )
    return joined.select("f.*", F.col("h.combined_4").alias("combined_4_most_recent")).drop("rn")

# ---------------------------
# Helper: add_combined_feature
# ---------------------------
def add_combined_feature(pdf):
    import pandas as pd
    # Get all columns starting with "embed_"
    embed_cols = sorted([c for c in pdf.columns if c.startswith("embed_")],
                        key=lambda x: int(x.split("_")[1]))
    print(f"[DEBUG] Found embedding columns: {embed_cols}")
    # List of other required columns
    other_cols = [
        "global_speed_score",
        "race_std_speed_agg", "race_avg_relevance_agg", "race_std_relevance_agg",
        "race_class_avg_speed_agg", "race_class_std_speed_agg",
        "race_class_min_speed_agg", "race_class_max_speed_agg",
        "horse_avg_speed_agg", "horse_std_speed_agg",
        "horse_min_speed_agg", "horse_max_speed_agg"
    ]
    for col in other_cols:
        if col not in pdf.columns:
            raise ValueError(f"Column {col} not found in DataFrame!")
    def combine_row(row):
        emb_vals = [row[c] for c in embed_cols]
        gss = [row["global_speed_score"]]
        race_feats = [row[c] for c in ["race_std_speed_agg", "race_avg_relevance_agg", "race_std_relevance_agg"]]
        race_class_feats = [row[c] for c in ["race_class_avg_speed_agg", "race_class_std_speed_agg",
                                             "race_class_min_speed_agg", "race_class_max_speed_agg"]]
        horse_feats = [row[c] for c in ["horse_avg_speed_agg", "horse_std_speed_agg",
                                        "horse_min_speed_agg", "horse_max_speed_agg"]]
        return emb_vals + gss + race_feats + race_class_feats + horse_feats
    combined = pdf.apply(combine_row, axis=1)
    n_features = len(combined.iloc[0])
    combined_df = pd.DataFrame(combined.tolist(), columns=[f"combined_{i}" for i in range(n_features)])
    pdf = pd.concat([pdf.reset_index(drop=True), combined_df], axis=1)
    pdf.drop(columns=embed_cols, inplace=True)
    return pdf

# ---------------------------
# Build Final Model Function
# ---------------------------
def build_final_model(horse_embedding_dim, horse_stats_dim, race_dim, cat_feature_info):
    # A) Inputs for horse sub-network
    horse_id_input = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
    horse_stats_input = keras.Input(shape=(horse_stats_dim,), name="horse_stats_input", dtype=tf.float32)
    
    # B) Inputs for race-level numeric features
    race_numeric_input = keras.Input(shape=(race_dim,), name="race_numeric_input", dtype=tf.float32)
    
    # C) Horse sub-network: combine horse_id embedding and horse_stats MLP
    horse_id_embedding = layers.Embedding(input_dim=75000 + 1, output_dim=8, name="horse_id_embedding")
    horse_id_emb = layers.Flatten()(horse_id_embedding(horse_id_input))
    
    x_horse = layers.Dense(64, activation="relu")(horse_stats_input)
    combined_horse = layers.Concatenate()([horse_id_emb, x_horse])
    horse_embedding_out = layers.Dense(horse_embedding_dim, activation="linear", name="horse_embedding_out")(combined_horse)
    
    # D) Race sub-network: simple MLP on race numeric features
    x_race = layers.Dense(32, activation="relu")(race_numeric_input)
    
    # E) Categorical branch using actual vocabulary from cat_feature_info
    cat_inputs = {}
    cat_embeddings = []
    for col, info in cat_feature_info.items():
        input_layer = keras.Input(shape=(1,), name=f"{col}_input", dtype=tf.string)  # Use shape (1,)
        cat_inputs[col] = input_layer
        vocab = info["vocab"]
        lookup = layers.StringLookup(vocabulary=vocab, output_mode="int", name=f"{col}_lookup")
        indices = lookup(input_layer)
        embed_layer = layers.Embedding(input_dim=len(vocab) + 1, output_dim=info["embed_dim"], name=f"{col}_embed")
        embedded = layers.Flatten()(embed_layer(indices))
        cat_embeddings.append(embedded)
    
    # F) Combine all branches
    combined_all = layers.Concatenate(name="final_concat")([horse_embedding_out, x_race] + cat_embeddings)
    output = layers.Dense(1, activation="linear", name="output")(combined_all)
    
    model = keras.Model(
        inputs=[horse_id_input, horse_stats_input, race_numeric_input] + list(cat_inputs.values()),
        outputs=output
    )
    return model

# ---------------------------
# Main Pipeline: embed_and_train
# ---------------------------
def embed_and_train(spark, jdbc_url, parquet_dir, jdbc_properties, global_speed_score, action="load"):
    
    global_speed_score = assign_labels_spark(global_speed_score, alpha=0.8)
    
    # Load historical and future data from Spark.
    historical_df_spark = global_speed_score.filter(F.col("data_flag") == "historical")
    future_df = global_speed_score.filter(F.col("data_flag") == "future")
    historical_pdf = historical_df_spark.toPandas()
    print("historical_pdf shape:", historical_pdf.shape)
    print("unique horse IDs in historical_pdf:", historical_pdf["horse_id"].nunique())
    
    # Build horse_id indexing.
    unique_horses = np.unique(historical_pdf["horse_id"])
    num_horses = len(unique_horses)
    horse_id_to_idx = {h: i for i, h in enumerate(unique_horses)}
    idx_to_horse_id = {v: k for k, v in horse_id_to_idx.items()}
    X_horse_id = np.array([horse_id_to_idx[h] for h in historical_pdf["horse_id"]])
    
    # Define column groups.
    horse_stats_cols = ["horse_avg_speed_agg", "horse_std_speed_agg", "horse_min_speed_agg", "horse_max_speed_agg"]
    race_numeric_cols = ["time_behind", "pace_delta_time", "race_std_speed_agg", 
                         "race_avg_relevance_agg", "race_std_relevance_agg", "race_class_avg_speed_agg", 
                         "race_class_std_speed_agg", "race_class_min_speed_agg", "race_class_max_speed_agg",
                         "class_rating", "official_distance"]
    cat_cols = ["course_cd", "trk_cond"]
    # Define categorical feature info with actual vocabularies.
    cat_feature_info = {
        "course_cd": {
            "vocab": ['CNL', 'SAR', 'PIM', 'TSA', 'BEL', 'MVR', 'TWO', 'CLS', 'KEE', 'TAM', 
                      'TTP', 'TKD', 'ELP', 'PEN', 'HOU', 'DMR', 'TLS', 'AQU', 'MTH', 'TGP',
                      'TGG', 'CBY', 'LRL', 'TED', 'IND', 'ASD', 'TCD', 'LAD', 'TOP'],
            "vocab_size": 29,  # if there are 29 unique items
            "embed_dim": 4
        },
        "trk_cond": {
            "vocab": ['FM', 'FT', 'FZ', 'GD', 'HD', 'HY', 'MY', 'SF', 'SL', 'SY', 'WF', 'YL'],
            "vocab_size": 12,
            "embed_dim": 3
        }
    }
    
    # Prepare input arrays.
    X_horse_stats = historical_pdf[horse_stats_cols].astype(float).values
    X_race_numeric = historical_pdf[race_numeric_cols].astype(float).values
    X_course_cd = historical_pdf["course_cd"].values
    X_trk_cond = historical_pdf["trk_cond"].values
    y = historical_pdf["relevance"].values  # Target
    
    # Split into train/validation sets.
    all_inds = np.arange(len(historical_pdf))
    train_inds, val_inds = train_test_split(all_inds, test_size=0.2, random_state=42)
    
    X_horse_id_train = X_horse_id[train_inds]
    X_horse_id_val = X_horse_id[val_inds]
    X_horse_stats_train = X_horse_stats[train_inds]
    X_horse_stats_val = X_horse_stats[val_inds]
    X_race_numeric_train = X_race_numeric[train_inds]
    X_race_numeric_val = X_race_numeric[val_inds]
    X_course_cd_train = X_course_cd[train_inds]
    X_course_cd_val = X_course_cd[val_inds]
    X_trk_cond_train = X_trk_cond[train_inds]
    X_trk_cond_val = X_trk_cond[val_inds]
    y_train = y[train_inds]
    y_val = y[val_inds]
    
    # Replace NaNs in X_race_numeric arrays with column means.
    for col_idx in range(X_race_numeric_train.shape[1]):
        col = X_race_numeric_train[:, col_idx]
        mean_val = np.nanmean(col)
        X_race_numeric_train[:, col_idx] = np.where(np.isnan(col), mean_val, col)
    for col_idx in range(X_race_numeric_val.shape[1]):
        col = X_race_numeric_val[:, col_idx]
        mean_val = np.nanmean(col)
        X_race_numeric_val[:, col_idx] = np.where(np.isnan(col), mean_val, col)
    
    # Build training and validation dictionaries.
    train_dict = {
        "horse_id_input": X_horse_id_train,
        "horse_stats_input": X_horse_stats_train,
        "race_numeric_input": X_race_numeric_train,
        "course_cd_input": X_course_cd_train,
        "trk_cond_input": X_trk_cond_train
    }
    val_dict = {
        "horse_id_input": X_horse_id_val,
        "horse_stats_input": X_horse_stats_val,
        "race_numeric_input": X_race_numeric_val,
        "course_cd_input": X_course_cd_val,
        "trk_cond_input": X_trk_cond_val
    }
    
    print(f"num_horses = {num_horses}")
    print(f"num_horse_stats = {X_horse_stats.shape[1]}")
    print(f"num_race_numeric = {X_race_numeric.shape[1]}")
    
    # Check for NaNs/Infs.
    check_nan_inf("X_horse_id_train", X_horse_id_train)
    check_nan_inf("X_horse_stats_train", X_horse_stats_train)
    check_nan_inf("X_race_numeric_train", X_race_numeric_train)
    check_nan_inf("X_course_cd_train", X_course_cd_train)
    check_nan_inf("X_trk_cond_train", X_trk_cond_train)
    check_nan_inf("y_train", y_train)
    
    # ---------------------------
    # Define objective function for Optuna
    # ---------------------------
    def objective(trial):
        # Hyperparameters for the horse sub-network.
        horse_embedding_dim = trial.suggest_int("horse_embedding_dim", 4, 8, step=4)
        horse_hid_layers = trial.suggest_int("horse_hid_layers", 1, 2)
        horse_units = trial.suggest_int("horse_units", 96, 160, step=16)
        # Hyperparameters for the race sub-network.
        race_hid_layers = trial.suggest_int("race_hid_layers", 1, 2)
        race_units = trial.suggest_int("race_units", 192, 256, step=32)
        activation = trial.suggest_categorical("activation", ["tanh", "relu", "softplus"])
        dropout_rate = trial.suggest_float("dropout_rate", 0.0, 0.2, step=0.05)
        learning_rate = trial.suggest_float("learning_rate", 5e-4, 2e-3, log=True)
        batch_size = trial.suggest_categorical("batch_size", [256])
        epochs = trial.suggest_int("epochs", 40, 100, step=10)
        l2_reg = 1e-4
        
        # (A) Horse sub-network.
        horse_id_inp = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
        horse_id_embedding = layers.Embedding(input_dim=num_horses+1,
                                              output_dim=horse_embedding_dim,
                                              name="horse_id_embedding")
        horse_id_emb = layers.Flatten()(horse_id_embedding(horse_id_inp))
        horse_stats_inp = keras.Input(shape=(X_horse_stats.shape[1],), name="horse_stats_input")
        x_horse = horse_stats_inp
        for _ in range(horse_hid_layers):
            x_horse = layers.Dense(horse_units, activation=activation, kernel_regularizer=l2(l2_reg))(x_horse)
            if dropout_rate > 0:
                x_horse = layers.Dropout(dropout_rate)(x_horse)
        combined_horse = layers.Concatenate()([horse_id_emb, x_horse])
        horse_embedding_out = layers.Dense(horse_embedding_dim, activation="linear", name="horse_embedding_out")(combined_horse)
        
        # (B) Race sub-network.
        race_numeric_inp = keras.Input(shape=(X_race_numeric.shape[1],), name="race_numeric_input")
        x_race = race_numeric_inp
        for _ in range(race_hid_layers):
            x_race = layers.Dense(race_units, activation=activation, kernel_regularizer=l2(l2_reg))(x_race)
            if dropout_rate > 0:
                x_race = layers.Dropout(dropout_rate)(x_race)
        
        # (C) Categorical branch.
        cat_inputs = {}
        cat_embeddings = []
        for cat_col in cat_cols:
            # Use input shape (1,) to ensure the output is a one-element vector per example.
            cat_in = keras.Input(shape=(1,), name=f"{cat_col}_input", dtype=tf.string)
            cat_inputs[cat_col] = cat_in
            vocab = np.unique(historical_pdf[cat_col].astype(str)).tolist()
            lookup = layers.StringLookup(output_mode="int", name=f"{cat_col}_lookup")
            lookup.adapt(vocab)
            cat_idx = lookup(cat_in)
            cat_embed_dim = 4
            cat_embed_layer = layers.Embedding(input_dim=len(vocab)+1, output_dim=cat_embed_dim, name=f"{cat_col}_embed")
            cat_emb = layers.Flatten()(cat_embed_layer(cat_idx))
            cat_embeddings.append(cat_emb)
        
        # (D) Combine all branches.
        # **FIX:** Use cat_embeddings (the numeric outputs) rather than cat_inputs.
        final_concat = layers.Concatenate()([horse_embedding_out, x_race] + cat_embeddings)
        output = layers.Dense(1, activation="linear", name="output")(final_concat)
        
        model = keras.Model(
            inputs=[horse_id_inp, horse_stats_inp, race_numeric_inp] + list(cat_inputs.values()),
            outputs=output
        )
        model.compile(
            optimizer=keras.optimizers.Adam(learning_rate=learning_rate),
            loss="mse",
            metrics=["mae"]
        )
        
        train_dict_local = {
            "horse_id_input": X_horse_id_train,
            "horse_stats_input": X_horse_stats_train,
            "race_numeric_input": X_race_numeric_train,
            "course_cd_input": X_course_cd_train,
            "trk_cond_input": X_trk_cond_train
        }
        val_dict_local = {
            "horse_id_input": X_horse_id_val,
            "horse_stats_input": X_horse_stats_val,
            "race_numeric_input": X_race_numeric_val,
            "course_cd_input": X_course_cd_val,
            "trk_cond_input": X_trk_cond_val
        }
        
        early_stopping = EarlyStopping(monitor="val_loss", patience=5, restore_best_weights=True)
        pruning_cb = TFKerasPruningCallback(trial, "val_loss")
        lr_cb = ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=3, min_lr=1e-6, verbose=1)
        ranking_cb = RankingMetricCallback({
            "horse_id_val": X_horse_id_val,
            "horse_stats_val": X_horse_stats_val,
            "race_numeric_val": X_race_numeric_val,
            "course_cd_val": X_course_cd_val,
            "trk_cond_val": X_trk_cond_val,
            "y_val": y_val
        })
        
        model.fit(
            train_dict_local, y_train,
            validation_data=(val_dict_local, y_val),
            epochs=epochs,
            batch_size=batch_size,
            callbacks=[early_stopping, pruning_cb, lr_cb, ranking_cb],
            verbose=1
        )
        val_loss, _ = model.evaluate(val_dict_local, y_val, verbose=0)

        return val_loss

    # ---------------------------
    # run_optuna_study function.
    # ---------------------------
    def run_optuna_study():
        sampler = optuna.samplers.RandomSampler(seed=42)  # using RandomSampler for now
        pruner = optuna.pruners.MedianPruner(n_warmup_steps=10)
        study_name = "horse_embedding_subnetwork_v4"
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
            print(f"Created new study: {study_name}")
        except optuna.exceptions.DuplicatedStudyError:
            print(f"Study {study_name} already exists. Loading it.")
            study = optuna.load_study(study_name=study_name, storage=storage_url)
        study.optimize(objective, n_trials=20)
        return study

    # ---------------------------
    # Run Optuna and get best hyperparameters.
    # ---------------------------
    def load_existing_study_best_params():
        """
        Load an existing Optuna study from DB, return best params
        (without running any new trials).
        """
        study_name = "horse_embedding_subnetwork_v4"
        storage_url = "sqlite:///my_optuna_study.db"
        study = optuna.load_study(
            study_name=study_name, 
            storage=storage_url
        )
        return study.best_trial.params
    
    # 4) Depending on action, either run the study or load best params
    if action == "train":
        # run the study => produce best params
        study = run_optuna_study()
        best_params = study.best_trial.params
        logging.info("Best parameters found by Optuna [train mode]:")
        logging.info(best_params)
    elif action == "load":
        # skip running => just load best params from existing DB
        best_params = load_existing_study_best_params()
        logging.info("Best parameters loaded from existing study [final mode]:")
        logging.info(best_params)
    else:
        raise ValueError(f"Unknown action={action}. Must be 'train' or 'final'.")
    
    horse_embedding_dim = best_params["horse_embedding_dim"]
    horse_hid_layers = best_params["horse_hid_layers"]
    horse_units = best_params["horse_units"]
    race_hid_layers = best_params["race_hid_layers"]
    race_units = best_params["race_units"]
    activation = best_params["activation"]
    dropout_rate = best_params["dropout_rate"]
    learning_rate = best_params["learning_rate"]
    batch_size = best_params["batch_size"]
    epochs = best_params["epochs"]
    l2_reg = 1e-4
    
    # ---------------------------
    # Build final model using build_final_model.
    # ---------------------------
    final_model = build_final_model(
        horse_embedding_dim=horse_embedding_dim,
        horse_stats_dim=len(horse_stats_cols),
        race_dim=len(race_numeric_cols),
        cat_feature_info=cat_feature_info
    )
    final_model.compile(
        optimizer=keras.optimizers.Adam(learning_rate=learning_rate),
        loss="mse",
        metrics=["mae"]
    )
    
    final_model.fit(
        train_dict, y_train,
        validation_data=(val_dict, y_val),
        epochs=epochs,
        batch_size=batch_size,
        callbacks=[
            EarlyStopping(monitor="val_loss", patience=10, restore_best_weights=True),
            ReduceLROnPlateau(monitor="val_loss", factor=0.5, patience=5, min_lr=1e-6, verbose=1),
            RankingMetricCallback({
                "horse_id_val": X_horse_id_val,
                "horse_stats_val": X_horse_stats_val,
                "race_numeric_val": X_race_numeric_val,
                "course_cd_val": X_course_cd_val,
                "trk_cond_val": X_trk_cond_val,
                "y_val": y_val
            })
        ],
        verbose=1
    )
    
    val_loss, val_mae = final_model.evaluate(val_dict, y_val, verbose=0)
    print(f"Final Model - Val MSE: {val_loss:.4f}, Val MAE: {val_mae:.4f}")
    
    # ---------------------------
    # Option A: Extract Raw horse_id_embedding Weights.
    # ---------------------------
    horse_id_embedding_layer = final_model.get_layer("horse_id_embedding")
    raw_embedding_weights = horse_id_embedding_layer.get_weights()[0]
    embedding_dim_actual = raw_embedding_weights.shape[1]
    
    rows_raw = []
    for i in range(num_horses):
        horse_id = idx_to_horse_id.get(i, None)
        if horse_id is not None:
            emb_vec = raw_embedding_weights[i].tolist()
            rows_raw.append([horse_id] + emb_vec)
    
    embed_cols_raw = ["horse_id"] + [f"embed_{i}" for i in range(embedding_dim_actual)]
    embed_df_raw = pd.DataFrame(rows_raw, columns=embed_cols_raw)
    print("\n[INFO] Raw horse_id_embedding weights (Option A):")
    print(embed_df_raw.head())
    
    raw_embed_sdf = spark.createDataFrame(embed_df_raw)
    raw_embed_sdf.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "horse_embedding_raw_weights") \
        .option("user", jdbc_properties["user"]) \
        .option("driver", jdbc_properties["driver"]) \
        .mode("overwrite") \
        .save()
    print("[INFO] Saved raw horse_id_embedding weights to DB table: horse_embedding_raw_weights.")
    
    # ---------------------------
    # Option B: Extract Row-Level Embeddings via Submodel.
    # ---------------------------
    try:
        submodel = keras.Model(
            inputs=[
                final_model.get_layer("horse_id_input").input,
                final_model.get_layer("horse_stats_input").input
            ],
            outputs=final_model.get_layer("horse_embedding_out").output
        )
    except Exception as e:
        print("[WARN] Could not build submodel for 'horse_embedding_out':", e)
        submodel = None
    
    if submodel is not None:
        row_embeddings = submodel.predict({
            "horse_id_input": historical_pdf["horse_id"].astype(int).values,
            "horse_stats_input": historical_pdf[horse_stats_cols].astype(float).values
        })
        print(f"[INFO] Row-level embeddings shape (Option B): {row_embeddings.shape}")
        embed_df_row = pd.DataFrame(row_embeddings, columns=[f"embrow_{i}" for i in range(row_embeddings.shape[1])])
        embed_df_row["row_idx"] = historical_pdf.index
        merged_df_row = pd.concat([historical_pdf.reset_index(drop=True), embed_df_row.reset_index(drop=True)], axis=1)
        row_embed_sdf = spark.createDataFrame(merged_df_row)
        row_embed_sdf.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "horse_embedding_out_rowlevel") \
            .option("user", jdbc_properties["user"]) \
            .option("driver", jdbc_properties["driver"]) \
            .mode("overwrite") \
            .save()
        print("[INFO] Saved row-level 'horse_embedding_out' to DB table: horse_embedding_out_rowlevel.")
    
    # ---------------------------
    # Merge embeddings with historical data using Option A.
    # ---------------------------
    merged_raw = pd.merge(historical_pdf, embed_df_raw, on="horse_id", how="left")
    merged_df = add_combined_feature(merged_raw)
    print("\n[INFO] Merged columns after add_combined_feature:")
    print(merged_df.columns.tolist())
    print("[INFO] Sample of merged DataFrame:")
    print(merged_df.head())
    
    historical_with_embed_sdf = spark.createDataFrame(merged_df)
    all_df = historical_with_embed_sdf.unionByName(future_df, allowMissingColumns=True)
    all_df = attach_recent_speed_figure(all_df, historical_with_embed_sdf)
    all_df.printSchema()
    print("Columns in final DF:", all_df.columns)
    
    staging_table = "horse_embedding_final"
    print(f"Writing final merged data to DB table: {staging_table}")
    all_df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", staging_table) \
        .option("user", jdbc_properties["user"]) \
        .option("driver", jdbc_properties["driver"]) \
        .mode("overwrite") \
        .save()
    print(f"[INFO] Final merged data saved to DB table: {staging_table}")
    
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    model_filename = f"horse_embedding_data-{current_time}"
    save_parquet(spark, all_df, model_filename, parquet_dir)
    print(f"[INFO] Final merged DataFrame saved as Parquet: {model_filename}")
    
    print("*** Horse embedding job completed successfully ***")
 
