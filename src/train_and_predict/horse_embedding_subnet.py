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
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint, ReduceLROnPlateau, Callback
from tensorflow.keras.regularizers import l2
from optuna.integration import TFKerasPruningCallback
from sklearn.model_selection import train_test_split
import scipy.stats as stats
from src.data_preprocessing.data_prep1.data_utils import save_parquet
from pyspark.sql.window import Window
from pyspark.sql import functions as F, Window
from scipy import stats

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
        self.val_data_dict = val_data_dict  # just store the dict

    def on_epoch_end(self, epoch, logs=None):
        # gather them
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
        
def check_nan_inf(name, arr):
    # Only do np.isnan/np.isinf if arr.dtype is float or int:
    if np.issubdtype(arr.dtype, np.floating) or np.issubdtype(arr.dtype, np.integer):
        nan_count = np.isnan(arr).sum()
        inf_count = np.isinf(arr).sum()
        print(f"[CHECK] {name}: nan={nan_count}, inf={inf_count}, shape={arr.shape}")
    else:
        print(f"[CHECK] {name}: (skipped - not numeric), dtype={arr.dtype}")

def attach_recent_speed_figure(df_final, historical_pdf):
    # We'll rename h.race_date to avoid collision in the join condition.
    hist_subset = (
        historical_pdf
        .withColumnRenamed("race_date", "hist_race_date")
        .select("horse_id", "hist_race_date", "combined_4")
    )
    w = Window.partitionBy("f.horse_id", "f.race_date").orderBy(F.desc("h.hist_race_date"))

    joined = (
        df_final.alias("f")
        .join(
            hist_subset.alias("h"),
            on=((F.col("f.horse_id") == F.col("h.horse_id")) & (F.col("h.hist_race_date") <= F.col("f.race_date"))),
            how="left"
        )
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
    )

    # Now select only the columns from f plus the combined_4 from h.
    # We do NOT select "h.hist_race_date".
    joined = joined.select(
        "f.*",
        F.col("h.combined_4").alias("combined_4_most_recent")
    )

    # (Optional) drop the temporary "rn" column
    joined = joined.drop("rn")

    return joined

def add_combined_feature(pdf):
    """
    Dynamically find all `embed_#` columns, plus the other known columns.
    Then build `combined_0..combined_{N-1}` from them.
    """
    import pandas as pd

    # 1) Collect the embedding columns actually present
    embed_cols = sorted(
        [c for c in pdf.columns if c.startswith("embed_")],
        key=lambda x: int(x.split("_")[1])  # ensure embed_0, embed_1, ...
    )
    embedding_dim = len(embed_cols)
    print(f"[DEBUG] Found {embedding_dim} embedding columns: {embed_cols}")

    # 2) List out the other columns you definitely want
    #    (Adjust as needed if you renamed or changed them.)
    other_cols = [
        "global_speed_score",
        "race_std_speed_agg", "race_avg_relevance_agg", "race_std_relevance_agg",
        "race_class_avg_speed_agg", "race_class_std_speed_agg",
        "race_class_min_speed_agg", "race_class_max_speed_agg",
        "horse_avg_speed_agg", "horse_std_speed_agg",
        "horse_min_speed_agg", "horse_max_speed_agg"
    ]

    # 3) Check that all the "other" columns exist
    for col in other_cols:
        if col not in pdf.columns:
            raise ValueError(f"Column {col} not found in DataFrame!")

    # 4) We'll define a function that concatenates these embedding + other columns
    def combine_row(row):
        # embedding columns:
        emb_values = [row[c] for c in embed_cols]  
        # other columns in a chosen order
        gss = [row["global_speed_score"]]
        race_feats = [row[c] for c in ["race_std_speed_agg", "race_avg_relevance_agg", "race_std_relevance_agg"]]
        race_class_feats = [row[c] for c in ["race_class_avg_speed_agg", "race_class_std_speed_agg",
                                             "race_class_min_speed_agg", "race_class_max_speed_agg"]]
        horse_feats = [row[c] for c in ["horse_avg_speed_agg", "horse_std_speed_agg",
                                        "horse_min_speed_agg", "horse_max_speed_agg"]]
        return emb_values + gss + race_feats + race_class_feats + horse_feats

    # 5) Apply across rows
    combined = pdf.apply(combine_row, axis=1)
    n_features = len(combined.iloc[0])  # total length of the combined vector

    # 6) Create combined_0..combined_{n_features-1}
    combined_df = pd.DataFrame(
        combined.tolist(),
        columns=[f"combined_{i}" for i in range(n_features)]
    )

    # 7) Concatenate back
    pdf = pd.concat([pdf.reset_index(drop=True), combined_df], axis=1)

    # 8) Optionally, drop the raw embed_# columns to reduce clutter
    pdf.drop(columns=embed_cols, inplace=True)

    return pdf

def build_final_model(horse_embedding_dim, horse_stats_dim, race_dim):
    
#     In this example:
# 	•	The horse sub-network (embedding + stats) merges into a horse_embedding_out.
# 	•	The final network merges that with race_numeric_input.
# 	•	We can create a submodel that takes [horse_id_input, horse_stats_input] and outputs horse_embedding_out.

# Important: We didn’t define “cat embeddings” or “top-level ranking.” You might do that in your actual code. The key is consistent naming.


    # A) Inputs
    horse_id_input = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
    horse_stats_input = keras.Input(shape=(horse_stats_dim,), name="horse_stats_input", dtype=tf.float32)

    race_numeric_input = keras.Input(shape=(race_dim,), name="race_numeric_input", dtype=tf.float32)
    # (You might also have cat features, etc.)

    # B) Horse sub-network
    #   e.g. embed horse_id + MLP on horse_stats, then combine
    horse_id_emb_layer = layers.Embedding(input_dim=75000+1, # e.g. 75k horses
                                          output_dim=8,
                                          name="horse_id_emb")
    horse_id_emb = layers.Flatten()(horse_id_emb_layer(horse_id_input))

    x_horse = layers.Dense(64, activation="relu")(horse_stats_input)
    # optionally more layers

    combined_horse = layers.Concatenate()([horse_id_emb, x_horse])

    # produce final "horse_embedding_out" (for extraction later)
    horse_embedding_out = layers.Dense(horse_embedding_dim,
                                       activation="linear",
                                       name="horse_embedding_out")(combined_horse)

    # C) Race sub-network
    x_race = layers.Dense(32, activation="relu")(race_numeric_input)
    # optionally more layers

    # D) Combine everything -> final output
    final_concat = layers.Concatenate(name="final_concat")([horse_embedding_out, x_race])
    output = layers.Dense(1, activation="linear", name="output")(final_concat)

    # E) Build the full model
    final_model = keras.Model(
        inputs=[horse_id_input, horse_stats_input, race_numeric_input],
        outputs=output
    )
    return final_model

# 2) Suppose we compile/train it:
model = build_final_model(horse_embedding_dim=4, horse_stats_dim=10, race_dim=5)
model.compile(optimizer="adam", loss="mse")

# 3) Now build submodel that outputs row-level "horse_embedding_out"
try:
    submodel = keras.Model(
        inputs=[
            model.get_layer("horse_id_input").input,
            model.get_layer("horse_stats_input").input
        ],
        outputs=model.get_layer("horse_embedding_out").output
    )
    print("Submodel for 'horse_embedding_out' created successfully.")
except:
    print("[WARN] Could not build submodel for 'horse_embedding_out' - check layer names.")
    submodel = None
    

def embed_and_train(spark, jdbc_url, parquet_dir, jdbc_properties, global_speed_score):
    # Assume global_speed_score is a Spark DataFrame.
    historical_df_spark = global_speed_score.filter(F.col("data_flag") == "historical")
    future_df = global_speed_score.filter(F.col("data_flag") == "future")

    historical_pdf = historical_df_spark.toPandas()
    print("historical_pdf shape:", historical_pdf.shape)
    print("unique horse IDs in historical_pdf:", historical_pdf["horse_id"].nunique())

    unique_horses = np.unique(historical_pdf["horse_id"])
    num_horses = len(unique_horses)

    # Build dictionary: horse_id -> 0..(num_horses-1)
    horse_id_to_idx = {h: i for i, h in enumerate(unique_horses)}
    # Invert it so we can map back idx -> horse_id
    idx_to_horse_id = {v: k for k, v in horse_id_to_idx.items()}

    # Then, when building X_horse_id:
    X_horse_id = np.array([horse_id_to_idx[h] for h in historical_pdf["horse_id"]])  # map each ID to the 0..N-1 index
    
    horse_stats_cols = ["horse_avg_speed_agg", "horse_std_speed_agg", "horse_min_speed_agg", "horse_max_speed_agg",
                        "avg_dist_bk_gate1_5", "avg_dist_bk_gate2_5", "avg_dist_bk_gate3_5",
                        "avg_dist_bk_gate4_5", "avg_speed_fullrace_5", "avg_stride_length_5", "avg_strfreq_q1_5",
                        "avg_strfreq_q2_5", "avg_strfreq_q3_5", "avg_strfreq_q4_5", "global_speed_score",
                        "all_starts", "all_win", "all_place", "all_show", "all_fourth", "horse_itm_percentage",
                        "cond_starts", "cond_win", "cond_place", "cond_show", "cond_fourth", "cond_earnings",
                        "sire_itm_percentage", "sire_roi", "dam_itm_percentage", "dam_roi", "age_at_race_day",
                        "speed_rating", "prev_speed_rating", "speed_improvement","power","previous_class",
                        "off_finish_last_race", "running_time"]
    
    race_numeric_cols = ["time_behind", "pace_delta_time","race_std_speed_agg", 
                         "race_avg_relevance_agg", "race_std_relevance_agg","race_class_avg_speed_agg", 
                         "race_class_std_speed_agg", "race_class_min_speed_agg", "race_class_max_speed_agg",
                         "class_rating","official_distance"]
    
    cat_cols = ["course_cd", "trk_cond"]

    # Prepare target.
    y = historical_pdf["relevance"].values
    
    # horse_id
    historical_pdf["horse_id"] = historical_pdf["horse_id"].astype(int)
    horse_ids = historical_pdf["horse_id"].values

    # Horse stats array
    X_horse_stats = historical_pdf[horse_stats_cols].astype(float).values
    # Race numeric array
    X_race_numeric = historical_pdf[race_numeric_cols].astype(float).values

    # course_cd, trk_cond arrays
    X_course_cd = historical_pdf["course_cd"].values
    X_trk_cond  = historical_pdf["trk_cond"].values

    # do a train/val split
    all_indices = np.arange(len(historical_pdf))
    from sklearn.model_selection import train_test_split
    train_inds, val_inds = train_test_split(all_indices, test_size=0.2, random_state=42)

    y_train = y[train_inds]
    y_val   = y[val_inds]

    # splitted arrays
    X_horse_id_train = X_horse_id[train_inds]
    X_horse_id_val   = X_horse_id[val_inds]
    print("X_horse_id_train.dtype =", X_horse_id_train.dtype)
    print("X_horse_id_val.dtype =", X_horse_id_val.dtype)
    input("Press Enter to continue...")


    X_horse_stats_train = X_horse_stats[train_inds]
    X_horse_stats_val   = X_horse_stats[val_inds]
    print("X_horse_stats_train.dtype =", X_horse_stats_train.dtype)
    print("X_horse_stats_val.dtype =", X_horse_stats_val.dtype)
    input("Press Enter to continue...")


    X_race_numeric_train = X_race_numeric[train_inds]
    X_race_numeric_val   = X_race_numeric[val_inds]
    
    for col_idx in range(X_race_numeric_train.shape[1]):
        col = X_race_numeric_train[:, col_idx]
        # Compute mean ignoring NaNs
        mean_val = np.nanmean(col)   # or you can define a fallback if the column is all NaNs
        # Find the rows where it's NaN
        nan_mask = np.isnan(col)
        # Replace with mean_val
        col[nan_mask] = mean_val

    print("X_race_numeric_train.dtype =", X_race_numeric_train.dtype)
    print("X_race_numeric_val.dtype =", X_race_numeric_val.dtype)
    input("Press Enter to continue...")

    X_course_cd_train = X_course_cd[train_inds]
    X_course_cd_val   = X_course_cd[val_inds]
    print("X_course_cd_train.dtype =", X_course_cd_train.dtype)
    print("X_course_cd_val.dtype =", X_course_cd_val.dtype)
    input("Press Enter to continue...")

    X_trk_cond_train = X_trk_cond[train_inds]
    X_trk_cond_val   = X_trk_cond[val_inds]

    print("X_trk_cond_train.dtype =", X_trk_cond_train.dtype)
    print("X_trk_cond_val.dtype =", X_trk_cond_val.dtype)
    input("Press Enter to continue...")

    unique_horses = np.unique(horse_ids)
    num_horses = len(unique_horses)
    num_horse_stats = X_horse_stats.shape[1]
    num_race_numeric = X_race_numeric.shape[1]

    print("num_horses =", num_horses)
    print("num_horse_stats =", num_horse_stats)
    print("num_race_numeric =", num_race_numeric)
    
    check_nan_inf("X_horse_id_train", X_horse_id_train)
    check_nan_inf("X_horse_id_val", X_horse_id_val)
    
    check_nan_inf("X_horse_stats_train", X_horse_stats_train)
    check_nan_inf("X_horse_stats_val", X_horse_stats_val)
    
    check_nan_inf("X_race_numeric_train", X_race_numeric_train)
    check_nan_inf("X_race_numeric_val", X_race_numeric_val)
    
    check_nan_inf("X_course_cd_train", X_course_cd_train)
    check_nan_inf("X_course_cd_val", X_course_cd_val)
    
    check_nan_inf("X_trk_cond_train", X_trk_cond_train)
    check_nan_inf("X_trk_cond_val", X_trk_cond_val)
    
    check_nan_inf("y_train", y_train)

    input("Press Enter to continue...")
    
    # ---------------------------
    # 4) Define objective function
    #    with separate sub-networks: horse_id+stats => horse_embedding_out,
    #    race_numeric => race_subnet_out, plus cat features => final
    # ---------------------------
    def objective(trial):
        # hyperparams for horse sub-network
        # Taking best so far:
        # {'activation': 'tanh', 'batch_size': 256, 'dropout_rate': 0.1, 'epochs': 40,
        #  'horse_embedding_dim': 4, 'horse_hid_layers': 2, 'horse_units': 128,
        #  'learning_rate': 0.0009686, 'race_hid_layers': 2, 'race_units': 224}

        horse_embedding_dim = trial.suggest_int(
            "horse_embedding_dim",
            4,  # Start at 4
            8,  # Let it try e.g. 4 or 8
            step=4
        )

        horse_hid_layers = trial.suggest_int(
            "horse_hid_layers",
            1,  # let it be 1 or 2
            2
        )

        horse_units = trial.suggest_int(
            "horse_units",
            96,    # narrower range around 128
            160,   # let it explore maybe 96, 112, 128, 144, 160 (step=16)
            step=16
        )

        race_hid_layers = trial.suggest_int(
            "race_hid_layers",
            1,     # again, keep it 1 or 2
            2
        )

        race_units = trial.suggest_int(
            "race_units",
            192,   # around 224, so 192..256
            256,
            step=32
        )

        activation = trial.suggest_categorical(
            "activation",
            ["tanh", "relu"]  # you can keep "softplus" if you want, 
                            # but typically reduce to the top 1-2 from best trial
        )

        dropout_rate = trial.suggest_float(
            "dropout_rate",
            0.0,   # maybe 0.0..0.2
            0.2,
            step=0.05
        )

        learning_rate = trial.suggest_float(
            "learning_rate",
            5e-4,  # narrower range around ~1e-3
            2e-3,  # or 3e-3, depending how big a range you want
            log=True
        )

        batch_size = trial.suggest_categorical(
            "batch_size",
            [256]  # if 256 was best, we might just fix it 
                # or keep [128, 256] if you want
        )

        epochs = trial.suggest_int(
            "epochs",
            30,    # let it range from 30..50
            50,
            step=10
        )

        l2_reg = 1e-4  # keep it fixed for now

        # (A) horse sub-network
        horse_id_inp = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
        horse_id_emb_layer = layers.Embedding(input_dim=num_horses+1,
                                              output_dim=horse_embedding_dim,
                                              name="horse_id_embedding")
        horse_id_emb = layers.Flatten()(horse_id_emb_layer(horse_id_inp))

        horse_stats_inp = keras.Input(shape=(num_horse_stats,), name="horse_stats_input")
        x_horse = horse_stats_inp
        for _ in range(horse_hid_layers):
            x_horse = layers.Dense(horse_units, activation=activation, kernel_regularizer=l2(l2_reg))(x_horse)
            if dropout_rate > 0:
                x_horse = layers.Dropout(dropout_rate)(x_horse)

        combined_horse = layers.Concatenate()([horse_id_emb, x_horse])
        horse_embedding_out = layers.Dense(horse_embedding_dim, activation="linear", name="horse_embedding_out")(combined_horse)

        # (B) race sub-network
        race_numeric_inp = keras.Input(shape=(num_race_numeric,), name="race_numeric_input")
        x_race = race_numeric_inp
        for _ in range(race_hid_layers):
            x_race = layers.Dense(race_units, activation=activation, kernel_regularizer=l2(l2_reg))(x_race)
            if dropout_rate > 0:
                x_race = layers.Dropout(dropout_rate)(x_race)

        # (C) categorical columns
        cat_inputs = {}
        cat_embeddings = []
        for cat_col in cat_cols:
            lookup = layers.StringLookup(output_mode="int")
            vocab = np.unique(historical_pdf[cat_col].astype(str)).tolist()
            lookup.adapt(vocab)

            cat_in = keras.Input(shape=(), name=f"{cat_col}_input", dtype=tf.string)
            cat_inputs[cat_col] = cat_in

            cat_idx = lookup(cat_in)
            cat_embed_dim = 4
            cat_embed_layer = layers.Embedding(input_dim=len(vocab)+1, output_dim=cat_embed_dim, name=f"{cat_col}_embed")
            cat_emb = layers.Flatten()(cat_embed_layer(cat_idx))
            cat_embeddings.append(cat_emb)

        # (D) combine: [horse_embedding_out, x_race] + cat_embeddings => final
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

        # (E) Prepare train/val dict
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

        # (F) Fit with callbacks
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
            train_dict, y_train,
            validation_data=(val_dict, y_val),
            epochs=epochs,
            batch_size=batch_size,
            callbacks=[early_stopping, pruning_cb, lr_cb, ranking_cb],
            verbose=1
        )

        val_loss, _ = model.evaluate(val_dict, y_val, verbose=0)
        return val_loss


    # 5) run_optuna_study
    def run_optuna_study():
        import optuna
        sampler = optuna.samplers.TPESampler(seed=42)
        pruner = optuna.pruners.MedianPruner(n_warmup_steps=10)
        study_name = "horse_embedding_subnetwork"
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

        study.optimize(objective, n_trials=20)  # or 100
        return study

    # 6) RUN THE STUDY
    study = run_optuna_study()
    best_params = study.best_trial.params
    print("Best trial:", best_params)
    best_params = study.best_trial.params
    horse_embedding_dim = best_params["horse_embedding_dim"]
    horse_hidden_layers = best_params["horse_hid_layers"]
    horse_units = best_params["horse_units"]
    logging.info("Best parameters found by Optuna:")
    logging.info(best_params)
    print("Best parameters found by Optuna:")
    print(best_params)

    # -------------------------------------------------------------
    # After you have best_params from study.best_trial.params:
    # -------------------------------------------------------------
    horse_embedding_dim   = best_params["horse_embedding_dim"]
    horse_hid_layers      = best_params["horse_hid_layers"]
    horse_units           = best_params["horse_units"]
    race_hid_layers       = best_params["race_hid_layers"]
    race_units            = best_params["race_units"]
    activation            = best_params["activation"]
    dropout_rate          = best_params["dropout_rate"]
    learning_rate         = best_params["learning_rate"]
    batch_size            = best_params["batch_size"]
    epochs                = best_params["epochs"]

    l2_reg = 1e-4

    # 1) HORSE SUB-NETWORK
    horse_id_input = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
    horse_id_embed_layer = layers.Embedding(
        input_dim=num_horses + 1,
        output_dim=horse_embedding_dim,
        name="horse_id_embedding"
    )
    horse_id_emb = layers.Flatten()(horse_id_embed_layer(horse_id_input))

    horse_stats_input = keras.Input(shape=(len(horse_stats_cols),), name="horse_stats_input")
    x_horse_stats = horse_stats_input

    for _ in range(horse_hid_layers):
        x_horse_stats = layers.Dense(horse_units, activation=activation,
                                    kernel_regularizer=l2(l2_reg))(x_horse_stats)
        if dropout_rate > 0:
            x_horse_stats = layers.Dropout(dropout_rate)(x_horse_stats)

    combined_horse = layers.Concatenate()([horse_id_emb, x_horse_stats])
    # produce final "horse_embedding_out" (for extraction later)
    horse_embedding_out = layers.Dense(horse_embedding_dim, activation="linear",
                                    name="horse_embedding_out")(combined_horse)

    # 2) RACE SUB-NETWORK
    race_numeric_input = keras.Input(shape=(len(race_numeric_cols),),
                                    name="race_numeric_input")
    x_race = race_numeric_input
    for _ in range(race_hid_layers):
        x_race = layers.Dense(race_units, activation=activation,
                            kernel_regularizer=l2(l2_reg))(x_race)
        if dropout_rate > 0:
            x_race = layers.Dropout(dropout_rate)(x_race)

    # 3) CATEGORICAL FEATURES
    cat_cols = ["course_cd", "trk_cond"]
    cat_inputs = {}
    cat_embeddings = []

    for cat_col in cat_cols:
        lookup = layers.StringLookup(output_mode="int")
        vocab = np.unique(historical_pdf[cat_col].astype(str)).tolist()
        lookup.adapt(vocab)

        cat_in = keras.Input(shape=(), name=f"{cat_col}_input", dtype=tf.string)
        cat_inputs[cat_col] = cat_in

        cat_idx = lookup(cat_in)
        cat_embed_dim = 4  # or tune as a hyperparam
        cat_embed_layer = layers.Embedding(
            input_dim=len(vocab) + 1,
            output_dim=cat_embed_dim,
            name=f"{cat_col}_embed"
        )
        cat_emb = layers.Flatten()(cat_embed_layer(cat_idx))
        cat_embeddings.append(cat_emb)

    # 4) COMBINE EVERYTHING
    final_concat = layers.Concatenate()(
        [horse_embedding_out, x_race] + cat_embeddings
    )
    output = layers.Dense(1, activation="linear")(final_concat)

    final_model = keras.Model(
        inputs=[horse_id_input, horse_stats_input, race_numeric_input] + list(cat_inputs.values()),
        outputs=output
    )

    final_model.compile(
        optimizer=keras.optimizers.Adam(learning_rate=learning_rate),
        loss="mse",
        metrics=["mae"]
    )

    # 5) TRAIN
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

    early_stop = EarlyStopping(monitor="val_loss", patience=10, restore_best_weights=True)
    reduce_lr = ReduceLROnPlateau(monitor="val_loss", factor=0.5, patience=5, min_lr=1e-6, verbose=1)

    ranking_cb = RankingMetricCallback({
        "horse_id_val": X_horse_id_val,
        "horse_stats_val": X_horse_stats_val,
        "race_numeric_val": X_race_numeric_val,
        "course_cd_val": X_course_cd_val,
        "trk_cond_val": X_trk_cond_val,
        "y_val": y_val
    })

    final_model.fit(
        train_dict, y_train,
        validation_data=(val_dict, y_val),
        epochs=epochs,
        batch_size=batch_size,
        callbacks=[early_stop, reduce_lr, ranking_cb],
        verbose=1
    )
    # Below is an all-in-one final snippet that:
	# 1.	Builds your final model with a horse sub-network (for horse‐level stats + horse_id) plus a race sub-network (for race‐level numeric).
	# 2.	Offers two ways of extracting horse embeddings:
	# •	Option A: Raw horse_id_embedding weights (one vector per horse_id, ignoring horse stats).
	# •	Option B: Submodel forward pass that includes horse_id + horse_stats => “horse_embedding_out” (row‐level embedding that depends on both ID and stats).
	# 3.	Saves both sets of embeddings to separate database tables, so you can explore them without retraining.
	# 4.	Does the usual steps: merges these embeddings into historical_pdf, calls add_combined_feature, unions with future_df, attaches recent speed, writes to DB, and saves Parquet.

    # ----------------------------------------------------------------
    #  Assuming you have just built final_model with best_params
    #  from your horse+race sub-network approach, e.g.:
    #  final_model.compile(...), final_model.fit(...)
    # ----------------------------------------------------------------

    print("Final model training complete.")

    # ==========================
    # 1) Evaluate
    # ==========================
    val_loss, val_mae = final_model.evaluate(val_dict, y_val, verbose=0)
    print(f"Final Model - Val MSE: {val_loss:.4f}, Val MAE: {val_mae:.4f}")

    # ==========================
    # 2) OPTION A: EXTRACT RAW horse_id_embedding WEIGHTS
    # ==========================
    # This yields a single static vector per horse_id, ignoring horse_stats_inp.

    horse_id_embedding_layer = final_model.get_layer("horse_id_embedding")
    embedding_weights = horse_id_embedding_layer.get_weights()[0]  # shape: (num_horses+1, horse_embedding_dim)
    embedding_dim_actual = embedding_weights.shape[1]

    
    print("horse_id_index train:", X_horse_id_train.min(), X_horse_id_train.max())
    print("embedding layer input_dim=", horse_id_embedding_layer.input_dim)
    input("Press Enter to continue...")
    
    # Build a DF of these raw embeddings
    rows_raw = []
    for i in range(num_horses):
        # 'i' is the internal index for horse_id in the Embedding layer
        # map back to actual horse_id from 'idx_to_horse_id'
        horse_id = idx_to_horse_id.get(i, None)
        if horse_id is not None:
            emb_vec = embedding_weights[i].tolist()
            rows_raw.append([horse_id] + emb_vec)

    embed_cols_raw = ["horse_id"] + [f"embed_{i}" for i in range(embedding_dim_actual)]
    embed_df_raw = pd.DataFrame(rows_raw, columns=embed_cols_raw)
    print("\n[INFO] Raw horse_id_embedding weights => embed_df_raw:\n", embed_df_raw.head())

    # OPTIONAL: Save this raw embedding to DB (so you can experiment later)
    raw_embed_sdf = spark.createDataFrame(embed_df_raw)
    raw_embed_sdf.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "horse_embedding_raw_weights") \
        .option("user", jdbc_properties["user"]) \
        .option("driver", jdbc_properties["driver"]) \
        .mode("overwrite") \
        .save()
    print("[INFO] Wrote raw horse_id_embedding to table: horse_embedding_raw_weights.")

    # ==========================
    # 3) OPTION B: SUBMODEL FOR "horse_embedding_out" => includes horse_stats
    # ==========================
    # Build a submodel that outputs the "horse_embedding_out" layer
    # i.e. final_model.get_layer("horse_embedding_out")

    try:
        submodel = keras.Model(
            inputs=[final_model.get_layer("horse_id_input").input,
                    final_model.get_layer("horse_stats_input").input],
            outputs=final_model.get_layer("horse_embedding_out").output
        )
        # We'll create row-level embeddings by calling submodel.predict(...)
        # for each row in your dataset. This embedding depends on both (horse_id, horse_stats).
    except:
        # If there's a mismatch in layer naming or you changed the architecture,
        # handle that error here:
        print("[WARN] Could not build submodel for 'horse_embedding_out' - check layer names.")
        submodel = None

    if submodel:
        # Build arrays for the entire historical set
        # so we do a forward pass => row-level embeddings
        #  => shape (num_samples, horse_embedding_dim)

        horse_id_array = historical_pdf["horse_id"].astype(int).values
        horse_stats_array = historical_pdf[horse_stats_cols].astype(float).values
        row_embeddings = submodel.predict({
            "horse_id_input": horse_id_array,
            "horse_stats_input": horse_stats_array
        })
        print(f"[INFO] row_embeddings shape = {row_embeddings.shape}")

        # Create a DataFrame from these row-level embeddings
        # We'll have as many rows as 'historical_pdf', so to join them, we need an index or something.
        # simplest: just store them in the same order, combine on index
        embed_df_row = pd.DataFrame(row_embeddings, columns=[f"embrow_{i}" for i in range(row_embeddings.shape[1])])
        embed_df_row["row_idx"] = historical_pdf.index  # keep track of which row
        # or we can store the horse_id if you prefer

        # Merge with historical_pdf by row index
        # ensuring row_idx is the same
        merged_df_row = pd.concat([historical_pdf.reset_index(drop=True), embed_df_row.reset_index(drop=True)], axis=1)

        print("\n[INFO] 'horse_embedding_out' submodel => row-level embeddings sample:\n", merged_df_row.head())

        # Save them to DB as well, for your future experiments
        row_embed_sdf = spark.createDataFrame(merged_df_row)
        row_embed_sdf.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "horse_embedding_out_rowlevel") \
            .option("user", jdbc_properties["user"]) \
            .option("driver", jdbc_properties["driver"]) \
            .mode("overwrite") \
            .save()
        print("[INFO] Wrote row-level 'horse_embedding_out' to table: horse_embedding_out_rowlevel.")

    # ==========================
    # 4) If you want to do add_combined_feature(...) as in your original snippet
    #    ignoring row-level embeddings for the moment, you can do so using
    #    the raw weight-based embed_df_raw
    # ==========================
    # We can do the same approach you had:
    merged_raw = pd.merge(historical_pdf, embed_df_raw, on="horse_id", how="left")
    merged_df = add_combined_feature(merged_raw)
    print("\n[INFO] Merged columns after add_combined_feature:\n", merged_raw.columns)

    historical_with_embed_sdf = spark.createDataFrame(merged_df)

    all_df = historical_with_embed_sdf.unionByName(future_df, allowMissingColumns=True)
    all_df = attach_recent_speed_figure(all_df, historical_with_embed_sdf)

    # Save to DB
    staging_table = "horse_embedding_final"
    (
        all_df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", staging_table)
        .option("user", jdbc_properties["user"])
        .option("driver", jdbc_properties["driver"])
        .mode("overwrite")
        .save()
    )
    print("[INFO] All data + raw embeddings saved to DB table:", staging_table)

    # Save to Parquet
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    model_filename = f"horse_embedding_data-{current_time}"
    save_parquet(spark, all_df, model_filename, parquet_dir)
    print(f"[INFO] Final merged DataFrame saved as Parquet: {model_filename}")

    print("*** Horse embedding job completed successfully ***")