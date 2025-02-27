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
        logs = logs or {}
        logs["val_spearman"] = corr
        print(f" - val_spearman: {corr:.4f}")

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
    Given a Pandas DataFrame `pdf` that contains:
      - Raw embedding columns: embed_0, embed_1, ..., embed_6 (7 total)
      - A global_speed_score column,
      - Race-level aggregated features: race_std_speed_agg, race_avg_relevance_agg, race_std_relevance_agg,
      - Race-class aggregated features: race_class_avg_speed_agg, race_class_std_speed_agg, race_class_min_speed_agg, race_class_max_speed_agg,
      - Horse-level aggregated features: horse_avg_speed_agg, horse_std_speed_agg, horse_min_speed_agg, horse_max_speed_agg.
    
    This function creates a combined feature vector by concatenating these values for each row.
    The resulting vector will have 19 elements, which are then expanded into new columns:
      combined_0, combined_1, ..., combined_18.
    
    Adjust the lists below if you want to include a different set of features.
    """
    import pandas as pd

    # Define the required columns:
    required_cols = [f"embed_{i}" for i in range(7)] + ["global_speed_score"] + \
                    ["race_std_speed_agg", "race_avg_relevance_agg", "race_std_relevance_agg"] + \
                    ["race_class_avg_speed_agg", "race_class_std_speed_agg", "race_class_min_speed_agg", "race_class_max_speed_agg"] + \
                    ["horse_avg_speed_agg", "horse_std_speed_agg", "horse_min_speed_agg", "horse_max_speed_agg"]
    for col in required_cols:
        if col not in pdf.columns:
            raise ValueError(f"Column {col} not found in DataFrame.")

    # Create the combined vector for each row.
    def combine_row(row):
        # Get the raw embeddings: embed_0 ... embed_6
        emb = [row[f"embed_{i}"] for i in range(7)]
        # Get global_speed_score
        gss = [row["global_speed_score"]]
        # Get race-level aggregated features
        race_feats = [row[col] for col in ["race_std_speed_agg", "race_avg_relevance_agg", "race_std_relevance_agg"]]
        # Get race-class aggregated features
        race_class_feats = [row[col] for col in ["race_class_avg_speed_agg", "race_class_std_speed_agg", "race_class_min_speed_agg", "race_class_max_speed_agg"]]
        # Get horse-level aggregated features
        horse_feats = [row[col] for col in ["horse_avg_speed_agg", "horse_std_speed_agg", "horse_min_speed_agg", "horse_max_speed_agg"]]
        # Concatenate everything
        return emb + gss + race_feats + race_class_feats + horse_feats

    combined = pdf.apply(combine_row, axis=1)
    
    # Determine the number of features (should be 19 in this example)
    n_features = len(combined.iloc[0])
    
    # Create new columns: combined_0, combined_1, ..., combined_{n_features-1}
    combined_df = pd.DataFrame(combined.tolist(), columns=[f"combined_{i}" for i in range(n_features)])
    
    # Concatenate the new combined columns to the original DataFrame
    pdf = pd.concat([pdf.reset_index(drop=True), combined_df], axis=1)
    
    # Optionally, drop the raw embedding columns if they are no longer needed:
    pdf.drop(columns=[f"embed_{i}" for i in range(7)], inplace=True)
    
    return pdf

def embed_and_train(spark, jdbc_url, parquet_dir, jdbc_properties, global_speed_score):
    # Assume global_speed_score is a Spark DataFrame.
    historical_df_spark = global_speed_score.filter(F.col("data_flag") == "historical")
    future_df = global_speed_score.filter(F.col("data_flag") == "future")

    historical_pdf = historical_df_spark.toPandas()
    print("historical_pdf shape:", historical_pdf.shape)
    print("unique horse IDs in historical_pdf:", historical_pdf["horse_id"].nunique())

    unique_horses = historical_pdf["horse_id"].unique()
    horse_id_to_idx = {h: i for i, h in enumerate(unique_horses)}
    
    horse_stats_cols = ["horse_avg_speed_agg", "horse_std_speed_agg", "horse_min_speed_agg", "horse_max_speed_agg",
                        "horse_avg_strf_agg", "horse_std_strf_agg", "horse_min_strf_agg", "horse_max_strf_agg",
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
    X_horse_id_train = horse_ids[train_inds]
    X_horse_id_val   = horse_ids[val_inds]

    X_horse_stats_train = X_horse_stats[train_inds]
    X_horse_stats_val   = X_horse_stats[val_inds]

    X_race_numeric_train = X_race_numeric[train_inds]
    X_race_numeric_val   = X_race_numeric[val_inds]

    X_course_cd_train = X_course_cd[train_inds]
    X_course_cd_val   = X_course_cd[val_inds]

    X_trk_cond_train = X_trk_cond[train_inds]
    X_trk_cond_val   = X_trk_cond[val_inds]

    unique_horses = np.unique(horse_ids)
    num_horses = len(unique_horses)
    num_horse_stats = X_horse_stats.shape[1]
    num_race_numeric = X_race_numeric.shape[1]

    print("num_horses =", num_horses)
    print("num_horse_stats =", num_horse_stats)
    print("num_race_numeric =", num_race_numeric)
    
    # ---------------------------
    # 4) Define objective function
    #    with separate sub-networks: horse_id+stats => horse_embedding_out,
    #    race_numeric => race_subnet_out, plus cat features => final
    # ---------------------------
    def objective(trial):
        # hyperparams for horse sub-network
        horse_embedding_dim = trial.suggest_int("horse_embedding_dim", 4, 16, step=4)
        horse_hid_layers = trial.suggest_int("horse_hid_layers", 1, 3)
        horse_units = trial.suggest_int("horse_units", 16, 128, step=16)

        # hyperparams for race sub-network
        race_hid_layers = trial.suggest_int("race_hid_layers", 1, 3)
        race_units = trial.suggest_int("race_units", 16, 256, step=16)

        activation = trial.suggest_categorical("activation", ["relu", "tanh", "softplus"])
        dropout_rate = trial.suggest_float("dropout_rate", 0.0, 0.4, step=0.1)
        learning_rate = trial.suggest_float("learning_rate", 1e-4, 1e-2, log=True)
        batch_size = trial.suggest_categorical("batch_size", [128, 256])
        epochs = trial.suggest_int("epochs", 10, 50, step=10)
        l2_reg = 1e-4

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

    val_loss, val_mae = final_model.evaluate(val_dict, y_val, verbose=0)
    print(f"Final Model - Val MSE: {val_loss:.4f}, Val MAE: {val_mae:.4f}")

    # 6) EXTRACT HORSE EMBEDDINGS
    # Option A: from the "horse_id_embedding" weights
    # Option B: from the sub-model that outputs "horse_embedding_out"

    # Option B: build submodel
    submodel = keras.Model(
        inputs=[final_model.get_layer("horse_id_input").input,
                final_model.get_layer("horse_stats_input").input],
        outputs=final_model.get_layer("horse_embedding_out").output
    )

    # We can do submodel.predict(...) to get row-level embeddings. 
    # Or we can take the raw weights from the "horse_id_embedding" if you want 
    # a single vector per horse_id ignoring stats.

    # after that, same steps:
    # 1) Build a DataFrame from the embeddings
    # 2) merge with historical_pdf
    # 3) add_combined_feature, union with future_df, attach_recent_speed_figure
    # 4) write to DB, parquet, etc.
    # ---------------------------
    # Extract and Save Horse Embeddings
    # ---------------------------
    embedding_weights = horse_embedding_layer.get_weights()[0]  # shape: (num_horses+1, embedding_dim)
    embedding_dim_actual = embedding_weights.shape[1]

    # Map horse indices back to horse IDs using the dictionary built during preprocessing.
    idx_to_horse_id = {v: k for k, v in horse_id_to_idx.items()}

    rows = []
    for i in range(num_horses):
        horse_id = idx_to_horse_id.get(i, None)
        if horse_id is not None:
            emb_vec = embedding_weights[i].tolist()
            rows.append([horse_id] + emb_vec)

    embed_cols = ["horse_id"] + [f"embed_{i}" for i in range(embedding_dim_actual)]
    embed_df = pd.DataFrame(rows, columns=embed_cols)

    print("Sample final embeddings:")
    print(embed_df.head())
    print("Columns in historical_pdf before merge:", historical_pdf.columns.tolist())
    print("Columns in embed_df:", embed_df.columns.tolist())

    merged_df = pd.merge(historical_pdf, embed_df, on="horse_id", how="left")
    merged_df = add_combined_feature(merged_df)
    print("Merged columns:", merged_df.columns.tolist())
    print("Sample of final merged_df:")
    print(merged_df.head())

    historical_with_embed_sdf = spark.createDataFrame(merged_df)

    # ---------------------------
    # Combine Historical and Future Data
    # ---------------------------
    all_df = historical_with_embed_sdf.unionByName(future_df, allowMissingColumns=True)
    all_df.printSchema()
    print("Columns in final DF:", all_df.columns)

    all_df = attach_recent_speed_figure(all_df, historical_with_embed_sdf)

    # ---------------------------
    # Write Final DataFrame to Database and Save as Parquet
    # ---------------------------
    staging_table = "horse_embedding"
    print(f"Writing horse embeddings to table: {staging_table}")
    (
        all_df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", staging_table)
        .option("user", jdbc_properties["user"])
        .option("driver", jdbc_properties["driver"])
        .mode("overwrite")
        .save()
    )
    print("Embeddings saved to DB table 'horse_embedding'.")

    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    model_filename = f"horse_embedding_data-{current_time}"
    save_parquet(spark, all_df, model_filename, parquet_dir)
    print(f"Final merged DataFrame saved as Parquet: {model_filename}")

    print("*** Horse embedding job completed successfully ***")