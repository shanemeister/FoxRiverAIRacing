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
import pyspark.sql.window as W
from scipy.stats import spearmanr
from sklearn.preprocessing import StandardScaler

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
            self.wait = 0  # reset counter
        else:
            self.wait += 1

        print(f"Epoch {epoch+1}: val_spearman = {corr:.4f}, best={self.best_spearman:.4f}, wait={self.wait}")
        
        if self.wait >= self.patience:
            print("Spearman hasn't improved in {} epochs. Stopping.".format(self.patience))
            self.model.stop_training = True
            
# ---------------------------
# Custom Callback for Ranking Metric
# ---------------------------
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

# ---------------------------
# Helper Functions
# ---------------------------
def fill_forward_locf(df, columns, horse_id_col="horse_id", date_col="race_date"):
    w = (Window.partitionBy(horse_id_col)
              .orderBy(F.col(date_col).asc())
              .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    for c in columns:
        df = df.withColumn(c, F.last(F.col(c), ignorenulls=True).over(w))
    return df

# def assign_labels_spark(df, alpha=0.8):
#     df = df.withColumn(
#             "relevance",
#             F.when(F.col("official_fin").isNotNull(), F.pow(F.lit(alpha), F.col("official_fin") - 1))
#              .otherwise(F.lit(None).cast(DoubleType()))
#         ).withColumn(
#             "top4_label",
#             F.when(F.col("official_fin").isNotNull(),
#                    F.when(F.col("official_fin") <= 4, F.lit(1)).otherwise(F.lit(0)))
#              .otherwise(F.lit(None).cast(IntegerType()))
#         )
#     return df

from pyspark.sql import functions as F

def assign_piecewise_log_labels_spark(df):
    """
    For each row in the DataFrame, assign a relevance score based on 'official_fin':
      - If official_fin == 1, relevance = 70
      - If official_fin == 2, relevance = 56
      - If official_fin == 3, relevance = 44
      - If official_fin == 4, relevance = 34
      - Otherwise, relevance = 30 / log(4 + official_fin)
      
    Also creates a 'top4_label' column which is 1 when official_fin <= 4, else 0.
    
    Parameters:
        df: pyspark.sql.DataFrame with a column 'official_fin'
    
    Returns:
        DataFrame with two new columns: 'relevance' and 'top4_label'
    """
    df = df.withColumn(
        "relevance",
        F.when(F.col("official_fin") == 1, 70)
         .when(F.col("official_fin") == 2, 56)
         .when(F.col("official_fin") == 3, 44)
         .when(F.col("official_fin") == 4, 34)
         .otherwise(30.0 / F.log(4.0 + F.col("official_fin")))
    )
    
    df = df.withColumn(
        "top4_label",
        F.when(F.col("official_fin") <= 4, 1).otherwise(0)
    )
    
    return df

def add_embed_feature(pdf):
    # Minimal version: you can modify as needed.
    embed_cols = sorted([c for c in pdf.columns if c.startswith("embed_")],
                        key=lambda x: int(x.split("_")[1]))
    print(f"[DEBUG] Found embedding columns: {embed_cols}")
    return pdf

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

# ---------------------------
# Helper: Build Horse Embedding Model (modified to output a scalar prediction)
# ---------------------------
def build_horse_embedding_model(horse_stats_input_dim, num_horses, horse_embedding_dim,
                                horse_hid_layers, horse_units, activation, dropout_rate, l2_reg):
    # Two inputs: horse_id and horse_stats.
    horse_id_inp = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
    horse_stats_inp = keras.Input(shape=(horse_stats_input_dim,), name="horse_stats_input")
    
    # Embedding for horse IDs.
    horse_id_embedding = layers.Embedding(
        input_dim=num_horses + 1,  # +1 for unknown IDs.
        output_dim=horse_embedding_dim,
        name="horse_id_embedding"
    )(horse_id_inp)
    horse_id_emb = layers.Flatten()(horse_id_embedding)
    
    # MLP for horse_stats.
    x = horse_stats_inp
    for _ in range(horse_hid_layers):
        x = layers.Dense(horse_units, activation=activation, kernel_regularizer=l2(l2_reg))(x)
        if dropout_rate > 0:
            x = layers.Dropout(dropout_rate)(x)
    
    # Combine and project to a single scalar output.
    embed = layers.Concatenate()([horse_id_emb, x])
    output = layers.Dense(1, activation="linear", kernel_regularizer=l2(l2_reg), name="horse_embedding_out")(embed)
    model = keras.Model(inputs=[horse_id_inp, horse_stats_inp], outputs=output)
    return model

# ---------------------------
# Main Function: embed_and_train
# ---------------------------
def embed_and_train(spark, jdbc_url, parquet_dir, jdbc_properties, global_speed_score, action="load"):
    # ------------------------
    # (A) Set up once
    # ------------------------
    # 1) Enable mixed precision once
    tf.keras.mixed_precision.set_global_policy("mixed_float16")

    # 2) Create a MirroredStrategy once
    strategy = tf.distribute.MirroredStrategy(devices=None)  # or specify GPU:0,1

    # Preprocess: assign labels, drop unused columns, and impute.
    global_speed_score = assign_piecewise_log_labels_spark(global_speed_score)
    columns_to_drop = [
        "logistic_score", "median_logistic", "median_logistic_clamped", "par_diff_ratio",
        "raw_performance_score", "standardized_score", "wide_factor"
    ]
    global_speed_score = global_speed_score.drop(*columns_to_drop)
    
    # Impute only the columns needed for horse_stats.
    cols_to_impute = ['base_speed', 'global_speed_score_iq', 'horse_mean_rps']
    global_speed_score = impute_with_race_and_global_mean(global_speed_score, cols_to_impute)
    
    # Load historical and future data.
    historical_df_spark = global_speed_score.filter(F.col("data_flag") == "historical")
    future_df = global_speed_score.filter(F.col("data_flag") == "future")
    historical_pdf = historical_df_spark.toPandas()
    print("historical_pdf shape:", historical_pdf.shape)
    
    # Compute unique horse IDs and mapping.
    unique_horses = np.unique(historical_pdf["horse_id"])
    num_horses = len(unique_horses)
    idx_to_horse_id = {i: horse for i, horse in enumerate(unique_horses)}
    
    # Define feature columns for horse_stats.
    horse_stats_cols = [
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
        "strfreq_q1", "strfreq_q2", "strfreq_q3", "strfreq_q4", "avg_stride_length", 
    ]
    
    # Prepare data.
    X_horse_stats = historical_pdf[horse_stats_cols].astype(float).values
    y = historical_pdf["relevance"].values  
    
    # Split into training and validation sets.
    all_inds = np.arange(len(historical_pdf))
    train_inds, val_inds = train_test_split(all_inds, test_size=0.2, random_state=42)
    X_horse_stats_train = X_horse_stats[train_inds]
    X_horse_stats_val = X_horse_stats[val_inds]
    y_train = y[train_inds]
    y_val = y[val_inds]
    
    # Map horse_id values to indices.
    X_horse_id_train = np.array([np.where(unique_horses == horse)[0][0]
                                  for horse in historical_pdf.loc[train_inds, "horse_id"]])
    X_horse_id_val = np.array([np.where(unique_horses == horse)[0][0]
                                for horse in historical_pdf.loc[val_inds, "horse_id"]])
    
    # Scale horse_stats.
    scaler = StandardScaler()
    scaler.fit(X_horse_stats_train)
    X_horse_stats_train = scaler.transform(X_horse_stats_train)
    X_horse_stats_val = scaler.transform(X_horse_stats_val)
    
    print(f"num_horse_stats = {X_horse_stats.shape[1]}")
    
    # 3) Build the tf.data.Dataset for training & validation *outside*, 
    def create_tf_datasets(X_horse_id_train, X_horse_stats_train, y_train,
                        X_horse_id_val,   X_horse_stats_val,   y_val,
                        batch_size=32):
        """Wrap the numpy arrays into tf.data.Dataset with .batch(..., drop_remainder=True)."""
        train_ds = tf.data.Dataset.from_tensor_slices((
            {
                "horse_id_input": X_horse_id_train,
                "horse_stats_input": X_horse_stats_train
            },
            y_train
        ))
        # If leftover partial batch is causing shape mismatch, drop it:
        train_ds = train_ds.batch(batch_size, drop_remainder=True)

        val_ds = tf.data.Dataset.from_tensor_slices((
            {
                "horse_id_input": X_horse_id_val,
                "horse_stats_input": X_horse_stats_val
            },
            y_val
        ))
        val_ds = val_ds.batch(batch_size, drop_remainder=True)

        return train_ds, val_ds

    # ------------------------
    # (B) The actual objective function
    # ------------------------
    def objective(trial):
        import logging
        from tensorflow import keras
        from scipy import stats

        # --- Hyperparams from Optuna
        horse_embedding_dim = trial.suggest_int("horse_embedding_dim", 4, 32, step=2)
        horse_hid_layers    = trial.suggest_int("horse_hid_layers", 1, 4)
        horse_units         = trial.suggest_int("horse_units", 64, 256, step=32)
        activation          = trial.suggest_categorical("activation", ["relu", "selu", "elu"])
        dropout_rate        = trial.suggest_float("dropout_rate", 0.0, 0.7, step=0.05)
        l2_reg              = trial.suggest_float("l2_reg", 1e-6, 1e-2, log=True)
        optimizer_name      = trial.suggest_categorical("optimizer", ["adam", "nadam", "rmsprop"])
        learning_rate       = trial.suggest_float("learning_rate", 1e-5, 1e-1, log=True)
        batch_size          = trial.suggest_categorical("batch_size", [8, 16, 32, 64])
        epochs              = trial.suggest_int("epochs", 100, 200, step=10)

        logging.info(f"Proposed hyperparams: {trial.params}")
        logging.info(f"Shapes => X_horse_stats_train: {X_horse_stats_train.shape}, "
                    f"X_horse_id_train: {X_horse_id_train.shape}, y_train: {y_train.shape}")

        # Re-build the dataset with the chosen batch_size
        train_ds, val_ds = create_tf_datasets(
            X_horse_id_train, X_horse_stats_train, y_train,
            X_horse_id_val,   X_horse_stats_val,   y_val,
            batch_size=batch_size
        )

        # Quick check for NaN/Inf
        def check_nan_inf(arr, name):
            logging.info(
                f"{name} => NaN: {np.isnan(arr).sum()}, Inf: {np.isinf(arr).sum()}, shape: {arr.shape}"
            )
        check_nan_inf(X_horse_stats_train, "X_horse_stats_train")
        check_nan_inf(y_train, "y_train")

        # -- Custom early stopping on Spearman
        spearman_callback = SpearmanEarlyStopping(
            X_val={"horse_id_input": X_horse_id_val, "horse_stats_input": X_horse_stats_val},
            y_val=y_val,
            patience=5
        )

        # Build & compile under the already-created strategy scope
        # (We moved 'strategy = tf.distribute.MirroredStrategy()' to the top)
        with strategy.scope():
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
                optimizer = keras.optimizers.Adam(learning_rate=learning_rate)
            elif optimizer_name == "nadam":
                optimizer = keras.optimizers.Nadam(learning_rate=learning_rate)
            else:
                optimizer = keras.optimizers.RMSprop(learning_rate=learning_rate)

            model.compile(optimizer=optimizer, loss="mse", metrics=["mae"])

        # Train
        model.fit(
            train_ds,
            validation_data=val_ds,
            epochs=epochs,
            verbose=1,
            callbacks=[spearman_callback]
        )

        # Evaluate using Spearman correlation on the val set
        # (We can do this with the same val dataset or the original arrays)
        y_pred = model.predict({"horse_id_input": X_horse_id_val,
                                "horse_stats_input": X_horse_stats_val})
        corr, _ = stats.spearmanr(y_val, y_pred.flatten())
        logging.info(f"Trial completed with Spearman correlation: {corr:.4f}")
        return corr

    def run_optuna_study(study_name, storage_url):
        study = optuna.create_study(
            study_name=study_name,
            storage=storage_url,
            load_if_exists=True,      # or False, depending on your preference
            direction="maximize"
        )
        study.optimize(objective, n_trials=10)
        return study
# ---------------------------
    # Final Model Training & Saving
    # ---------------------------
    def run_final_model(action, study_name, storage_url, jdbc_url, jdbc_properties, spark,
                        historical_pdf, future_df):
        # Load or run study.
        if action == "train":
            study = run_optuna_study(study_name, storage_url)
            best_params = study.best_trial.params
        elif action == "load":
            study = optuna.load_study(study_name=study_name, storage=storage_url)
            best_params = study.best_trial.params
        else:
            raise ValueError("Action must be 'train' or 'load'.")
        
        spearman_callback = SpearmanEarlyStopping(
        ({"horse_id_input": X_horse_id_val, "horse_stats_input": X_horse_stats_val}),
            y_val,
            patience=5
        )
        
        strategy = tf.distribute.MirroredStrategy()
        with strategy.scope():
            # Build final model using best parameters.
            final_model = build_horse_embedding_model(
                horse_stats_input_dim=X_horse_stats.shape[1],
                num_horses=num_horses,
                horse_embedding_dim=best_params["horse_embedding_dim"],
                horse_hid_layers=best_params["horse_hid_layers"],
                horse_units=best_params["horse_units"],
                activation=best_params["activation"],
                dropout_rate=best_params["dropout_rate"],
                l2_reg=best_params["l2_reg"]
            )
            if best_params["optimizer"] == "adam":
                optimizer = keras.optimizers.Adam(learning_rate=best_params["learning_rate"])
            elif best_params["optimizer"] == "nadam":
                optimizer = keras.optimizers.Nadam(learning_rate=best_params["learning_rate"])
            else:
                optimizer = keras.optimizers.RMSprop(learning_rate=best_params["learning_rate"])
            final_model.compile(optimizer=optimizer, loss="mse", metrics=["mae"])
        
        # Train final model.
        final_model.fit(
            {"horse_id_input": X_horse_id_train, "horse_stats_input": X_horse_stats_train},
            y_train,
            validation_data=(
                {"horse_id_input": X_horse_id_val, "horse_stats_input": X_horse_stats_val},
                y_val
            ),
            epochs=best_params["epochs"],
            batch_size=best_params["batch_size"],
            callbacks=[spearman_callback],
            verbose=1
        )
        
        # Extract raw horse ID embedding weights.
        horse_id_embedding_layer = final_model.get_layer("horse_id_embedding")
        raw_embedding_weights = horse_id_embedding_layer.get_weights()[0]
        embedding_dim = raw_embedding_weights.shape[1]
        rows = []
        for i in range(num_horses):
            horse_id = idx_to_horse_id.get(i)
            if horse_id is not None:
                rows.append([horse_id] + raw_embedding_weights[i].tolist())
        embed_df = pd.DataFrame(rows, columns=["horse_id"] + [f"embed_{i}" for i in range(embedding_dim)])
        raw_embed_sdf = spark.createDataFrame(embed_df)
        raw_embed_sdf.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "horse_embedding_raw_weights") \
            .option("user", jdbc_properties["user"]) \
            .option("driver", jdbc_properties["driver"]) \
            .mode("overwrite") \
            .save()
        
        # Merge embeddings with historical data.
        merged_raw = pd.merge(historical_pdf, embed_df, on="horse_id", how="left")
        merged_df = add_embed_feature(merged_raw)
        # For this minimal example, we use the new embedding columns.
        embed_cols = [col for col in merged_df.columns if col.startswith("embed_")]
        historical_embed_sdf = spark.createDataFrame(merged_df)
        all_df = historical_embed_sdf.unionByName(future_df, allowMissingColumns=True)
        all_df = fill_forward_locf(all_df, embed_cols, "horse_id", "race_date")
        
        # Save final merged data to DB table and as Parquet.
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
        print(f"[INFO] Final merged data saved to DB table '{staging_table}' and as Parquet: {model_filename}")
        return model_filename

    def run_pipeline():
        study_name = "horse_embedding_v1"
        storage_url = "sqlite:///horse_embedding_optuna_study.db"
        model_filename = run_final_model(
            action, study_name, storage_url, jdbc_url, jdbc_properties,
            spark, historical_pdf, future_df
        )
        logging.info("Pipeline completed. Final model data saved as: %s", model_filename)
        return model_filename

    return run_pipeline()