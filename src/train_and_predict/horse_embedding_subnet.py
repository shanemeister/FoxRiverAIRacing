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
from tensorflow.keras.callbacks import EarlyStopping, Callback
from tensorflow.keras.regularizers import l2
from optuna.integration import TFKerasPruningCallback
from sklearn.model_selection import train_test_split
from pyspark.sql.types import DoubleType, IntegerType
import scipy.stats as stats
from src.data_preprocessing.data_prep1.data_utils import save_parquet
from pyspark.sql import functions as F, Window
import pyspark.sql.window as W
from scipy.stats import spearmanr
from sklearn.preprocessing import StandardScaler

############################
# 1) Custom Callbacks
############################
class SpearmanEarlyStopping(keras.callbacks.Callback):
    def __init__(self, X_val, y_val, patience=5):
        super().__init__()
        self.X_val = X_val
        self.y_val = y_val
        self.patience = patience
        self.best_spearman = -1.0
        self.wait = 0  # epochs since last improvement

    def on_epoch_end(self, epoch, logs=None):
        y_pred = self.model.predict(self.X_val)
        corr, _ = stats.spearmanr(self.y_val, y_pred.flatten())

        if corr > self.best_spearman:
            self.best_spearman = corr
            self.wait = 0
        else:
            self.wait += 1

        print(f"Epoch {epoch+1}: val_spearman = {corr:.4f}, best={self.best_spearman:.4f}, wait={self.wait}")
        
        if self.wait >= self.patience:
            print(f"Spearman hasn't improved in {self.patience} epochs. Stopping.")
            self.model.stop_training = True


class RankingMetricCallback(Callback):
    def __init__(self, val_data_dict):
        """
        val_data_dict is a dictionary with keys:
          "horse_id_val", "horse_stats_val", "y_val"
        """
        super().__init__()
        self.val_data_dict = val_data_dict

    def on_epoch_end(self, epoch, logs=None):
        preds = self.model.predict({
            "horse_id_input": self.val_data_dict["horse_id_val"],
            "horse_stats_input": self.val_data_dict["horse_stats_val"]
        })
        y_true = self.val_data_dict["y_val"]
        corr, _ = stats.spearmanr(y_true, preds.flatten())
        logs = logs or {}
        logs["val_spearman"] = corr
        print(f" - val_spearman: {corr:.4f}")

############################
# 2) Helper Functions
############################
def fill_forward_locf(df, columns, horse_id_col="horse_id", date_col="race_date"):
    w = (Window.partitionBy(horse_id_col)
              .orderBy(F.col(date_col).asc())
              .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    for c in columns:
        df = df.withColumn(c, F.last(F.col(c), ignorenulls=True).over(w))
    return df

def impute_with_race_and_global_mean(df, cols_to_impute, race_col="race_id"):
    for col in cols_to_impute:
        race_mean_col = f"{col}_race_mean"
        df = df.withColumn(race_mean_col, F.avg(F.col(col)).over(Window.partitionBy(race_col)))
        global_mean_col = f"{col}_global_mean"
        global_mean = df.select(F.avg(F.col(col)).alias(global_mean_col)).collect()[0][global_mean_col]
        df = df.withColumn(global_mean_col, F.lit(global_mean))
        df = df.withColumn(col, F.coalesce(F.col(col), F.col(race_mean_col), F.col(global_mean_col), F.lit(0)))
        df = df.drop(race_mean_col, global_mean_col)
    return df


def check_nan_inf(name, arr):
    if np.issubdtype(arr.dtype, np.floating) or np.issubdtype(arr.dtype, np.integer):
        nan_count = np.isnan(arr).sum()
        inf_count = np.isinf(arr).sum()
        print(f"[CHECK] {name}: nan={nan_count}, inf={inf_count}, shape={arr.shape}")
    else:
        print(f"[CHECK] {name}: (skipped - not numeric), dtype={arr.dtype}")


def add_embed_feature(pdf):
    embed_cols = sorted([c for c in pdf.columns if c.startswith("embed_")],
                        key=lambda x: int(x.split("_")[1]))
    print(f"[DEBUG] Found embedding columns: {embed_cols}")
    # You can do any other modifications if needed
    return pdf


############################
# 3) Build the Keras Model
############################
def build_horse_embedding_model(horse_stats_input_dim, num_horses, horse_embedding_dim,
                                horse_hid_layers, horse_units, activation, dropout_rate, l2_reg):
    horse_id_inp = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
    horse_stats_inp = keras.Input(shape=(horse_stats_input_dim,), name="horse_stats_input")
    
    horse_id_embedding = layers.Embedding(
        input_dim=num_horses + 1,
        output_dim=horse_embedding_dim,
        name="horse_id_embedding"
    )(horse_id_inp)
    horse_id_emb = layers.Flatten()(horse_id_embedding)
    
    x = horse_stats_inp
    for _ in range(horse_hid_layers):
        x = layers.Dense(horse_units, activation=activation, kernel_regularizer=l2(l2_reg))(x)
        if dropout_rate > 0:
            x = layers.Dropout(dropout_rate)(x)
    
    embed = layers.Concatenate()([horse_id_emb, x])
    output = layers.Dense(1, activation="linear", kernel_regularizer=l2(l2_reg), name="horse_embedding_out")(embed)
    model = keras.Model(inputs=[horse_id_inp, horse_stats_inp], outputs=output)
    return model


############################
# 4) The main pipeline function
############################
def embed_and_train(spark, jdbc_url, parquet_dir, jdbc_properties, global_speed_score, action="load"):
    """
    This function loads/trains an Optuna study for a horse embedding model,
    merges final embeddings with historical data + future data, and saves
    the result to DB + Parquet.
    """
    # 1) Mixed precision + Multi-GPU
    tf.keras.mixed_precision.set_global_policy("mixed_float16")
   # 2) Preprocess
    global_speed_score = global_speed_score.withColumn("relevance", F.col("running_time_target"))
        
    columns_to_drop = [
        "logistic_score", "median_logistic", "median_logistic_clamped", "par_diff_ratio",
        "raw_performance_score", "standardized_score", "wide_factor"
    ]
    global_speed_score = global_speed_score.drop(*columns_to_drop)

    # Impute columns
    cols_to_impute = ['base_speed', 'global_speed_score_iq', 'horse_mean_rps']
    global_speed_score = impute_with_race_and_global_mean(global_speed_score, cols_to_impute)
    
    # Separate historical/future
    historical_df_spark = global_speed_score

    historical_pdf = historical_df_spark.toPandas()
    print("historical_pdf shape:", historical_pdf.shape)

    # unique horses
    unique_horses = np.unique(historical_pdf["horse_id"])
    num_horses = len(unique_horses)
    idx_to_horse_id = {i: horse for i, horse in enumerate(unique_horses)}

    # Define columns for horse stats
    horse_stats_cols = ["sec_score", "sec_dim1", "sec_dim2", "sec_dim3", "sec_dim4", 
                        "sec_dim5", "sec_dim6", "sec_dim7", "sec_dim8",
                        "sec_dim9", "sec_dim10", "sec_dim11", "sec_dim12", 
                        "sec_dim13", "sec_dim14", "sec_dim15", "sec_dim16",
                        "global_speed_score_iq", "starts", "dam_itm_percentage", "cond_starts", 
                        "previous_class", "age_at_race_day", "avg_workout_rank_3", "speed_improvement", 
                        "horse_mean_rps", "trainer_win_percent", "horse_itm_percentage", "days_off", 
                        "base_speed", "sire_itm_percentage", "jt_win_track", "cond_show", 
                        "jock_win_percent", "horse_std_rps", "jock_win_track", "weight",
                        "avgtime_gate1", "avgtime_gate2", "avgtime_gate3", "avgtime_gate4", 
                        "dist_bk_gate1", "dist_bk_gate2", "dist_bk_gate3", "dist_bk_gate4", 
                        "speed_q1", "speed_q2", "speed_q3", "speed_q4", "speed_var", "avg_speed_fullrace", 
                        "accel_q1", "accel_q2", "accel_q3", "accel_q4", "avg_acceleration", "max_acceleration", 
                        "jerk_q1", "jerk_q2", "jerk_q3", "jerk_q4", "avg_jerk", "max_jerk", 
                        "dist_q1", "dist_q2", "dist_q3", "dist_q4", "total_dist_covered", 
                        "strfreq_q1", "strfreq_q2", "strfreq_q3", "strfreq_q4", "avg_stride_length"
                    ]

    X_horse_stats = historical_pdf[horse_stats_cols].astype(float).values
    y = historical_pdf["relevance"].values
    
    # for col in horse_stats_cols:
    #     corr, _ = spearmanr(historical_pdf[col], y)
    #     print(col, corr)
    
    # for c in horse_stats_cols:
    #     if c not in historical_pdf.columns:
    #         print(f"{c} is missing!")
            
    # for c in horse_stats_cols:
    #     vals = historical_pdf[c].values
    #     print(c, "NaN count=", np.isnan(vals).sum(), "std dev=", np.nanstd(vals))

    # train/val split
    all_inds = np.arange(len(historical_pdf))
    train_inds, val_inds = train_test_split(all_inds, test_size=0.2, random_state=42)
    X_horse_stats_train = X_horse_stats[train_inds]
    X_horse_stats_val   = X_horse_stats[val_inds]
    y_train = y[train_inds]
    y_val   = y[val_inds]
    
    y_train_2d = y_train.reshape(-1, 1)
    y_val_2d   = y_val.reshape(-1, 1)

    scaler_y = StandardScaler()
    y_train_scaled_2d = scaler_y.fit_transform(y_train_2d)
    y_val_scaled_2d   = scaler_y.transform(y_val_2d)

    y_train_scaled = y_train_scaled_2d.flatten()
    y_val_scaled   = y_val_scaled_2d.flatten()
    y_train = y_train_scaled
    y_val   = y_val_scaled
    
    # Check for NaN or Inf in y_train and y_val

    print("Check y_train for NaN or Inf")
    print("Any NaN in y_train?", np.isnan(y_train).any())
    print("Any Inf in y_train?", np.isinf(y_train).any())
    print("y_train min:", np.min(y_train), "y_train max:", np.max(y_train))

    # Similarly for y_val
    print("Check y_val for NaN or Inf")
    print("Any NaN in y_val?", np.isnan(y_val).any())
    print("Any Inf in y_val?", np.isinf(y_val).any())
    print("y_val min:", np.min(y_val), "y_val max:", np.max(y_val))

    # Map horse IDs -> indices
    def map_horse_id_to_idx(horse_id):
        return np.where(unique_horses == horse_id)[0][0]

    X_horse_id_train = np.array([map_horse_id_to_idx(horse) for horse in historical_pdf.loc[train_inds, "horse_id"]])
    X_horse_id_val   = np.array([map_horse_id_to_idx(horse) for horse in historical_pdf.loc[val_inds,   "horse_id"]])

    # Scale numeric columns
    scaler = StandardScaler()
    scaler.fit(X_horse_stats_train)
    X_horse_stats_train = scaler.transform(X_horse_stats_train)
    X_horse_stats_val   = scaler.transform(X_horse_stats_val)

    print(f"[INFO] horse_stats shape = {X_horse_stats.shape}, y shape = {y.shape}")

    print("Check X_horse_stats_train for NaN/Inf:")
    print("Any NaN in X_horse_stats_train?", np.isnan(X_horse_stats_train).any())
    print("Any Inf in X_horse_stats_train?", np.isinf(X_horse_stats_train).any())
    print("Min:", np.nanmin(X_horse_stats_train), "Max:", np.nanmax(X_horse_stats_train))

    print("Check X_horse_stats_val for NaN/Inf:")
    print("Any NaN in X_horse_stats_val?", np.isnan(X_horse_stats_val).any())
    print("Any Inf in X_horse_stats_val?", np.isinf(X_horse_stats_val).any())
    print("Min:", np.nanmin(X_horse_stats_val), "Max:", np.nanmax(X_horse_stats_val))

    # --------------------------
    # Helper: create_tf_datasets with drop_remainder=True
    # --------------------------
    def create_tf_datasets(X_horse_id_train, X_horse_stats_train, y_train,
                           X_horse_id_val, X_horse_stats_val, y_val,
                           batch_size=32):
        train_ds = tf.data.Dataset.from_tensor_slices((
            {"horse_id_input": X_horse_id_train, "horse_stats_input": X_horse_stats_train},
            y_train
        ))
        train_ds = train_ds.batch(batch_size, drop_remainder=True)

        val_ds = tf.data.Dataset.from_tensor_slices((
            {"horse_id_input": X_horse_id_val, "horse_stats_input": X_horse_stats_val},
            y_val
        ))
        val_ds = val_ds.batch(batch_size, drop_remainder=True)
        return train_ds, val_ds

    # -------------------------------------------------
    #  A) objective for Optuna
    # -------------------------------------------------
    def objective(trial):
        horse_embedding_dim = 9 # trial.suggest_int("horse_embedding_dim", 8, 32, step=2)
        horse_hid_layers    = trial.suggest_int("horse_hid_layers", 1, 2)
        horse_units         = trial.suggest_int("horse_units", 8, 64, step=8)
        activation          = trial.suggest_categorical("activation", ["relu", "gelu"])
        dropout_rate        = trial.suggest_float("dropout_rate", 0.0, 0.7, step=0.1)
        l2_reg              = trial.suggest_float("l2_reg", 1e-6, 1e-2, log=True)
        optimizer_name      = trial.suggest_categorical("optimizer", ["adam"])
        learning_rate       = trial.suggest_float("learning_rate", 1e-5, 1e-1, log=True)
        batch_size          = trial.suggest_categorical("batch_size", [64, 128, 256])
        epochs              = trial.suggest_int("epochs", 50, 100, step=50)

        train_ds, val_ds = create_tf_datasets(
            X_horse_id_train, X_horse_stats_train, y_train,
            X_horse_id_val,   X_horse_stats_val,   y_val,
            batch_size=batch_size
        )

        # Spearman callback
        spearman_callback = SpearmanEarlyStopping(
            X_val={"horse_id_input": X_horse_id_val, "horse_stats_input": X_horse_stats_val},
            y_val=y_val,
            patience=5
        )

        
        model = build_horse_embedding_model(
            horse_stats_input_dim=X_horse_stats.shape[1],
            num_horses=num_horses,
            horse_embedding_dim=horse_embedding_dim,
            horse_hid_layers=horse_hid_layers,
            horse_units=horse_units,
            activation=activation,
            dropout_rate=dropout_rate,
            l2_reg=l2_reg
        )
        if optimizer_name == "adam":
            opt = keras.optimizers.Adam(learning_rate=learning_rate, clipnorm=1.0)
        elif optimizer_name == "nadam":
            opt = keras.optimizers.Nadam(learning_rate=learning_rate, clipnorm=1.0)
        else:
            opt = keras.optimizers.RMSprop(learning_rate=learning_rate, clipnorm=1.0)

        model.compile(optimizer=opt, loss="mse", metrics=["mae"])

        model.fit(
            train_ds,
            validation_data=val_ds,
            epochs=epochs,
            callbacks=[spearman_callback],
            verbose=1
        )

        y_pred = model.predict({"horse_id_input": X_horse_id_val, "horse_stats_input": X_horse_stats_val})
        corr, _ = spearmanr(y_val, y_pred.flatten())
        return corr

    # Helper to run or load the study
    def run_optuna_study(study_name, storage_url):
        study = optuna.create_study(
            study_name=study_name,
            storage=storage_url,
            load_if_exists=True,
            direction="maximize"
        )
        study.optimize(objective, n_trials=30)  # or more
        return study

    # -------------------------------------------------
    #  B) run_final_model
    # -------------------------------------------------
    def run_final_model(action, study_name, storage_url, jdbc_url, jdbc_properties, spark,
                        historical_pdf):
        # run or load the study
        if action == "train":
            study = run_optuna_study(study_name, storage_url)
            best_params = study.best_trial.params
        elif action == "load":
            study = optuna.load_study(study_name=study_name, storage=storage_url)
            best_params = study.best_trial.params
        else:
            raise ValueError("action must be 'train' or 'load'.")

        # Build final ds with best_params["batch_size"]
        final_batch_size = best_params["batch_size"]
        train_ds, val_ds = create_tf_datasets(
            X_horse_id_train, X_horse_stats_train, y_train,
            X_horse_id_val,   X_horse_stats_val,   y_val,
            batch_size=final_batch_size
        )

        # final Spearman callback
        spearman_callback = SpearmanEarlyStopping(
            X_val={"horse_id_input": X_horse_id_val, "horse_stats_input": X_horse_stats_val},
            y_val=y_val,
            patience=5
        )

       
        final_model = build_horse_embedding_model(
            horse_stats_input_dim=X_horse_stats.shape[1],
            num_horses=num_horses,
            horse_embedding_dim=9, # best_params["horse_embedding_dim"],
            horse_hid_layers=best_params["horse_hid_layers"],
            horse_units=best_params["horse_units"],
            activation=best_params["activation"],
            dropout_rate=best_params["dropout_rate"],
            l2_reg=best_params["l2_reg"]
        )
        if best_params["optimizer"] == "adam":
            opt = keras.optimizers.Adam(learning_rate=best_params["learning_rate"])
        elif best_params["optimizer"] == "nadam":
            opt = keras.optimizers.Nadam(learning_rate=best_params["learning_rate"])
        else:
            opt = keras.optimizers.RMSprop(learning_rate=best_params["learning_rate"])

        final_model.compile(optimizer=opt, loss="mse", metrics=["mae"])

        final_model.fit(
            train_ds,
            validation_data=val_ds,
            epochs=best_params["epochs"],
            callbacks=[spearman_callback],
            verbose=1
        )

        # Extract embedding
        horse_id_embedding_layer = final_model.get_layer("horse_id_embedding")
        raw_embedding_weights = horse_id_embedding_layer.get_weights()[0]
        embedding_dim = raw_embedding_weights.shape[1]

        # Build embed df
        rows = []
        for i in range(num_horses):
            horse_id = idx_to_horse_id.get(i)
            if horse_id is not None:
                rows.append([horse_id] + raw_embedding_weights[i].tolist())
        embed_df = pd.DataFrame(rows, columns=["horse_id"] + [f"embed_{j}" for j in range(embedding_dim)])

        # Save raw embedding to DB table
        raw_embed_sdf = spark.createDataFrame(embed_df)
        raw_embed_sdf.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "horse_embedding_raw_weights") \
            .option("user", jdbc_properties["user"]) \
            .option("driver", jdbc_properties["driver"]) \
            .mode("overwrite") \
            .save()

        # Merge with historical
        merged_raw = pd.merge(historical_pdf, embed_df, on="horse_id", how="left")
        merged_df = add_embed_feature(merged_raw)
        embed_cols = [c for c in merged_df.columns if c.startswith("embed_")]

        historical_embed_sdf = spark.createDataFrame(merged_df)
    
        all_df = fill_forward_locf(
                historical_embed_sdf,
                embed_cols,
                "horse_id",
                "race_date"
            )


        # Save final
        staging_table = "horse_embedding_final"
        all_df.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", staging_table) \
            .option("user", jdbc_properties["user"]) \
            .option("driver", jdbc_properties["driver"]) \
            .mode("overwrite") \
            .save()

        current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        model_filename = f"horse_embedding_data-{current_time}"
        save_parquet(spark, all_df, model_filename, parquet_dir)
        print(f"[INFO] Final merged data => DB '{staging_table}', Parquet: {model_filename}")
        return model_filename

    # -----------
    # Pipeline
    # -----------
    def run_pipeline():
        study_name = "horse_embedding_v1"
        storage_url = "sqlite:///horse_embedding_optuna_study.db"
        model_filename = run_final_model(
            action, study_name, storage_url, jdbc_url, jdbc_properties,
            spark, historical_pdf
        )
        logging.info("[INFO] Pipeline completed. Final model data: %s", model_filename)
        return model_filename

    return run_pipeline()