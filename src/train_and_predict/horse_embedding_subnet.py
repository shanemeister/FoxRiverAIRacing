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

# ---------------------------
# Custom Callback for Ranking Metric
# ---------------------------
class SpearmanMetricCallback(Callback):
    def __init__(self, x_val, y_val):
        super().__init__()
        self.x_val = x_val
        self.y_val = y_val

    def on_epoch_end(self, epoch, logs=None):
        logs = logs or {}
        y_pred = self.model.predict(self.x_val)
        corr, _ = spearmanr(self.y_val, y_pred.flatten())
        logs["val_spearman"] = corr
        #print(f" - val_spearman: {corr:.4f}")
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
        
def fill_forward_locf(df, columns, horse_id_col="horse_id", date_col="race_date"):
    """
    Forward-fill the specified 'columns' by horse_id, in ascending order of date_col.
    For each row, if the value is null, fill with the last non-null from a previous row.
    """
    # 1) Define window partitioned by horse, ordered by date ascending
    w = (
        W.Window
         .partitionBy(horse_id_col)
         .orderBy(F.col(date_col).asc())
         .rowsBetween(W.Window.unboundedPreceding, W.Window.currentRow)
    )
    
    # 2) Apply last_value(..., ignorenulls=True) over the window for each col
    for c in columns:
        df = df.withColumn(
            c,
            F.last(F.col(c), ignorenulls=True).over(w)
        )
    
    return df

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
# Helper: add_combined_feature
# ---------------------------
def add_combined_feature(pdf):
    import pandas as pd
    import logging
    
    # Get all columns starting with "embed_"
    embed_cols = sorted(
        [c for c in pdf.columns if c.startswith("embed_")],
        key=lambda x: int(x.split("_")[1])
    )
    
    logging.info("Embed columns in pdf: %s", embed_cols)   
    print(f"[DEBUG] Found embedding columns: {embed_cols}")
    # List of other required columns
    other_cols = [
        "global_speed_score_iq",
        "race_std_speed_agg", "race_avg_relevance_agg", "race_std_relevance_agg",
        "race_class_avg_speed_agg",
        "race_class_min_speed_agg", "race_class_max_speed_agg",
        "horse_mean_rps", "horse_std_rps"]
    
    for col in other_cols:
        if col not in pdf.columns:
            raise ValueError(f"Column {col} not found in DataFrame!")
    def combine_row(row):
        emb_vals = [row[c] for c in embed_cols]
        gss = [row["global_speed_score_iq"]]
        race_feats = [row[c] for c in ["race_std_speed_agg", "race_avg_relevance_agg", "race_std_relevance_agg"]]
        race_class_feats = [row[c] for c in ["race_class_avg_speed_agg", "race_class_count_agg",
                                             "race_class_min_speed_agg", "race_class_max_speed_agg"]]
        horse_feats = [row[c] for c in ["horse_mean_rps", "horse_std_rps", "power", "base_speed", "avg_speed_fullrace_5", "speed_improvement"]]
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
def build_final_model(
    horse_vocab_size,
    horse_embedding_dim,
    horse_stats_dim,
    race_dim,
    cat_feature_info,
    horse_hid_layers,
    horse_units,
    race_hid_layers,
    race_units,
    activation,         # e.g. "elu"
    dropout_rate,       # e.g. 0.35
    l2_reg             # e.g. 1.5584070764453296e-05
):
    """
    Builds the final Keras model using sub-networks for:
      1) Horse ID embedding + horse stats sub-network
      2) Race numeric sub-network
      3) Multiple categorical inputs from cat_feature_info
    Then concatenates them all for the final Dense output layer.

    Arguments:
      horse_vocab_size (int)   : The size of the 'horse_id' vocabulary 
                                 (max horse_id index + 1).
      horse_embedding_dim (int): Dimension of the horse_id embedding.
      horse_stats_dim (int)    : Number of numeric horse stats features.
      race_dim (int)           : Number of numeric race-level features.
      cat_feature_info (dict)  : Dictionary of {column_name: {"vocab": [...], "embed_dim": int}}
                                 for each categorical feature.
      horse_hid_layers (int)   : Number of hidden layers in the horse sub-network.
      horse_units (int)        : #units per Dense layer in horse sub-network.
      race_hid_layers (int)    : Number of hidden layers in the race sub-network.
      race_units (int)         : #units per Dense layer in race sub-network.
      activation (str)         : Activation function for hidden layers (e.g. "relu", "selu").
      dropout_rate (float)     : Dropout rate for hidden layers. 0.0 => no dropout.
      l2_reg (float)           : L2 regularization factor for Dense layers.

    Returns:
      model (keras.Model): A compiled Keras model, 
                           but you still need to call .compile() with your chosen optimizer & loss.
    """
    # -------------------------------------------------
    # A) Define Keras Inputs
    # -------------------------------------------------
    # -- Horse sub-network
    
    horse_id_inp = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
    
    horse_stats_inp = keras.Input(shape=(horse_stats_dim,), name="horse_stats_input", dtype=tf.float32)
    # -- Race numeric sub-network
    race_numeric_inp = keras.Input(shape=(race_dim,),name="race_numeric_input")
  
    # -- Example categorical race inputs (course_cd, trk_cond)
    #    If these are string features:
    course_cd_in = keras.Input(shape=(1,), name="course_cd_input", dtype=tf.string)

    
    trk_cond_in = keras.Input(shape=(1,), name="trk_cond_input", dtype=tf.string)

    
    # -------------------------------------------------
    # B) Horse sub-network
    # -------------------------------------------------
    # Horse ID embedding
    horse_id_embedding = layers.Embedding(
        input_dim=horse_vocab_size + 1,   # must match your horse_id range
        output_dim=horse_embedding_dim,
        name="horse_id_embedding"
    )
    horse_id_emb = layers.Flatten()(horse_id_embedding(horse_id_inp))

    # MLP on horse stats
    x_horse = horse_stats_inp
    for _ in range(horse_hid_layers):
        x_horse = layers.Dense(
            horse_units,
            activation=activation,
            kernel_regularizer=l2(l2_reg)
        )(x_horse)
        if dropout_rate > 0:
            x_horse = layers.Dropout(dropout_rate)(x_horse)

    print("DEBUG: horse_id_emb =>", horse_id_emb)
    print("DEBUG: x_horse =>", x_horse)
    
    # Combine horse ID embedding + stats => project to horse_embedding_dim
    combined_horse = layers.Concatenate()([horse_id_emb, x_horse])
    horse_embedding_out = layers.Dense(
        horse_embedding_dim,
        activation="linear",
        kernel_regularizer=l2(l2_reg),
        name="horse_embedding_out"
    )(combined_horse)

    # -------------------------------------------------
    # C) Race numeric sub-network
    # -------------------------------------------------
    x_race = race_numeric_inp
    for _ in range(race_hid_layers):
        x_race = layers.Dense(
            race_units,
            activation=activation,
            kernel_regularizer=l2(l2_reg)
        )(x_race)
        if dropout_rate > 0:
            x_race = layers.Dropout(dropout_rate)(x_race)

    # -------------------------------------------------
    # D) Categorical sub-network example
    # -------------------------------------------------

    # 1) course_cd
    vocab_course_cd = [
        'CNL', 'SAR', 'PIM', 'TSA', 'BEL', 'MVR', 'TWO', 'CLS', 'KEE', 'TAM',
        'TTP', 'TKD', 'ELP', 'PEN', 'HOU', 'DMR', 'TLS', 'AQU', 'MTH', 'TGP',
        'TGG', 'CBY', 'LRL', 'TED', 'IND', 'ASD', 'TCD', 'LAD', 'TOP'
    ]
    # Suppose embed_dim=4 per your cat_feature_info
    course_cd_lookup = layers.StringLookup(vocabulary=vocab_course_cd, output_mode="int")
    course_cd_idx = course_cd_lookup(course_cd_in)
    course_cd_embed_layer = layers.Embedding(
        input_dim=len(vocab_course_cd) + 1,  # +1 for OOV/unknown
        output_dim=4,                        # from cat_feature_info["course_cd"]["embed_dim"]
        name="course_cd_embed"
    )
    course_cd_emb = layers.Flatten()(course_cd_embed_layer(course_cd_idx))

    # 2) trk_cond
    vocab_trk_cond = [
        'FM', 'FT', 'FZ', 'GD', 'HD', 'HY', 'MY', 'SF', 'SL', 'SY', 'WF', 'YL'
    ]
    # Suppose embed_dim=3 per your cat_feature_info
    trk_cond_lookup = layers.StringLookup(vocabulary=vocab_trk_cond, output_mode="int")
    trk_cond_idx = trk_cond_lookup(trk_cond_in)
    trk_cond_embed_layer = layers.Embedding(
        input_dim=len(vocab_trk_cond) + 1,  # +1 for OOV/unknown
        output_dim=3,                      # from cat_feature_info["trk_cond"]["embed_dim"]
        name="trk_cond_embed"
    )
    trk_cond_emb = layers.Flatten()(trk_cond_embed_layer(trk_cond_idx))
    # -------------------------------------------------
    # E) Combine all sub-networks
    # -------------------------------------------------
    final_concat = layers.Concatenate()([
        horse_embedding_out,
        x_race,
        course_cd_emb,
        trk_cond_emb
    ])
    output = layers.Dense(
        1,
        activation="linear",
        kernel_regularizer=l2(l2_reg),
        name="output"
    )(final_concat)

    model = keras.Model(
        inputs=[
            horse_id_inp,
            horse_stats_inp,
            race_numeric_inp,
            course_cd_in,
            trk_cond_in
        ],
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
    horse_stats_cols = ["horse_mean_rps", "horse_std_rps", "power", "base_speed", "avg_speed_fullrace_5", "speed_improvement", 
                        "days_off","trainer_win_percent", "jock_win_percent", "jt_win_percent", "trainer_itm_percent", "jock_itm_percent",
                        "sire_itm_percentage", "sire_roi", "dam_itm_percentage", "dam_roi"]
    race_numeric_cols = ["race_std_speed_agg", "purse","net_sentiment",
                         "race_avg_relevance_agg", "race_std_relevance_agg", "race_class_avg_speed_agg", 
                         "race_class_min_speed_agg", "race_class_max_speed_agg",
                         "class_rating", "official_distance", "morn_odds"]
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
        """
        Tunable parameters:
        - horse_embedding_dim, horse_hid_layers, horse_units
        - race_hid_layers, race_units
        - activation, dropout_rate, l2_reg
        - optimizer_name, learning_rate, batch_size, epochs
        Includes a sub-network for categorical inputs (e.g., course_cd, trk_cond).
        """

        # 1) Horse embedding dimensions & MLP
        #horse_embedding_dim = trial.suggest_int("horse_embedding_dim", 4, 16, step=4)
        horse_embedding_dim = trial.suggest_int("horse_embedding_dim", 8, 32, step=4)
        #horse_hid_layers = trial.suggest_int("horse_hid_layers", 1, 3)
        horse_hid_layers = trial.suggest_int("horse_hid_layers", 1, 4)
        #horse_units = trial.suggest_int("horse_units", 64, 256, step=32)
        horse_units = trial.suggest_int("horse_units", 128, 512, step=32)

        # 2) Race sub-network MLP
        #race_hid_layers = trial.suggest_int("race_hid_layers", 1, 3)
        race_hid_layers = trial.suggest_int("race_hid_layers", 1, 4)
        #race_units = trial.suggest_int("race_units", 128, 384, step=64)
        race_units = trial.suggest_int("race_units", 32, 512, step=64)

        # 3) General network hyperparams
        activation = trial.suggest_categorical("activation", ["relu", "gelu", "selu", "tanh", "softplus", "elu"])
        dropout_rate = trial.suggest_float("dropout_rate", 0.0, 0.7, step=0.05)
        # l2_reg = trial.suggest_float("l2_reg", 1e-6, 1e-3, log=True)
        l2_reg = trial.suggest_float("l2_reg", 1e-8, 1e-2, log=True)

        # 4) Optimization hyperparams
        optimizer_name = trial.suggest_categorical("optimizer", ["adam", "nadam", "rmsprop"])
        learning_rate = trial.suggest_float("learning_rate", 1e-4, 1e-2, log=True)
        batch_size = trial.suggest_categorical("batch_size", [128, 256, 512, 1024, 2048])
        epochs = trial.suggest_int("epochs", 100, 150, step=10)

        # -------------------------------------------------
        # A) Define Keras Inputs
        # -------------------------------------------------
        # -- Horse sub-network
        horse_id_inp = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
        horse_stats_inp = keras.Input(shape=(X_horse_stats.shape[1],), name="horse_stats_input")

        # -- Race numeric sub-network
        race_numeric_inp = keras.Input(shape=(X_race_numeric.shape[1],), name="race_numeric_input")

        # -- Example categorical race inputs (course_cd, trk_cond)
        #    If these are string features:
        course_cd_in = keras.Input(shape=(1,), name="course_cd_input", dtype=tf.string)
        trk_cond_in = keras.Input(shape=(1,), name="trk_cond_input", dtype=tf.string)

        # -------------------------------------------------
        # B) Horse sub-network
        # -------------------------------------------------
        # Horse ID embedding
        horse_id_embedding = layers.Embedding(
            input_dim=num_horses + 1,   # must match your horse_id range
            output_dim=horse_embedding_dim,
            name="horse_id_embedding"
        )
        horse_id_emb = layers.Flatten()(horse_id_embedding(horse_id_inp))

        # MLP on horse stats
        x_horse = horse_stats_inp
        for _ in range(horse_hid_layers):
            x_horse = layers.Dense(
                horse_units,
                activation=activation,
                kernel_regularizer=l2(l2_reg)
            )(x_horse)
            if dropout_rate > 0:
                x_horse = layers.Dropout(dropout_rate)(x_horse)

        # Combine horse ID embedding + stats => project to horse_embedding_dim
        combined_horse = layers.Concatenate()([horse_id_emb, x_horse])
        horse_embedding_out = layers.Dense(
            horse_embedding_dim,
            activation="linear",
            kernel_regularizer=l2(l2_reg),
            name="horse_embedding_out"
        )(combined_horse)

        # -------------------------------------------------
        # C) Race numeric sub-network
        # -------------------------------------------------
        x_race = race_numeric_inp
        for _ in range(race_hid_layers):
            x_race = layers.Dense(
                race_units,
                activation=activation,
                kernel_regularizer=l2(l2_reg)
            )(x_race)
            if dropout_rate > 0:
                x_race = layers.Dropout(dropout_rate)(x_race)

        # -------------------------------------------------
        # D) Categorical sub-network example
        # -------------------------------------------------

        # 1) course_cd
        vocab_course_cd = [
            'CNL', 'SAR', 'PIM', 'TSA', 'BEL', 'MVR', 'TWO', 'CLS', 'KEE', 'TAM',
            'TTP', 'TKD', 'ELP', 'PEN', 'HOU', 'DMR', 'TLS', 'AQU', 'MTH', 'TGP',
            'TGG', 'CBY', 'LRL', 'TED', 'IND', 'ASD', 'TCD', 'LAD', 'TOP'
        ]
        # Suppose embed_dim=4 per your cat_feature_info
        course_cd_lookup = layers.StringLookup(vocabulary=vocab_course_cd, output_mode="int")
        course_cd_idx = course_cd_lookup(course_cd_in)
        course_cd_embed_layer = layers.Embedding(
            input_dim=len(vocab_course_cd) + 1,  # +1 for OOV/unknown
            output_dim=4,                        # from cat_feature_info["course_cd"]["embed_dim"]
            name="course_cd_embed"
        )
        course_cd_emb = layers.Flatten()(course_cd_embed_layer(course_cd_idx))

        # 2) trk_cond
        vocab_trk_cond = [
            'FM', 'FT', 'FZ', 'GD', 'HD', 'HY', 'MY', 'SF', 'SL', 'SY', 'WF', 'YL'
        ]
        # Suppose embed_dim=3 per your cat_feature_info
        trk_cond_lookup = layers.StringLookup(vocabulary=vocab_trk_cond, output_mode="int")
        trk_cond_idx = trk_cond_lookup(trk_cond_in)
        trk_cond_embed_layer = layers.Embedding(
            input_dim=len(vocab_trk_cond) + 1,  # +1 for OOV/unknown
            output_dim=3,                      # from cat_feature_info["trk_cond"]["embed_dim"]
            name="trk_cond_embed"
        )
        trk_cond_emb = layers.Flatten()(trk_cond_embed_layer(trk_cond_idx))
        # -------------------------------------------------
        # E) Combine all sub-networks
        # -------------------------------------------------
        final_concat = layers.Concatenate()([
            horse_embedding_out,
            x_race,
            course_cd_emb,
            trk_cond_emb
        ])
        output = layers.Dense(
            1,
            activation="linear",
            kernel_regularizer=l2(l2_reg),
            name="output"
        )(final_concat)

        model = keras.Model(
            inputs=[
                horse_id_inp,
                horse_stats_inp,
                race_numeric_inp,
                course_cd_in,
                trk_cond_in
            ],
            outputs=output
        )

        # -------------------------------------------------
        # F) Compile with chosen optimizer
        # -------------------------------------------------
        if optimizer_name == "adam":
            optimizer = keras.optimizers.Adam(learning_rate=learning_rate)
        elif optimizer_name == "nadam":
            optimizer = keras.optimizers.Nadam(learning_rate=learning_rate)
        else:
            optimizer = keras.optimizers.RMSprop(learning_rate=learning_rate)

        model.compile(
            optimizer=optimizer,
            loss="mse",
            metrics=["mae"]
        )

        # -------------------------------------------------
        # G) Prepare training dict & validation dict
        # -------------------------------------------------
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

        # -------------------------------------------------
        # H) Callbacks & Training
        # -------------------------------------------------
        callbacks = [
            SpearmanMetricCallback(val_dict_local, y_val),  # must go first
            EarlyStopping(monitor="val_spearman", mode="max", patience=5, restore_best_weights=True),
            TFKerasPruningCallback(trial, "val_spearman"),
            # keras.callbacks.EarlyStopping(monitor="val_loss", patience=5, restore_best_weights=True),
            # TFKerasPruningCallback(trial, "val_loss"),
            ReduceLROnPlateau(monitor="val_spearman", mode="max", factor=0.5, patience=3, min_lr=1e-6, verbose=1),
            RankingMetricCallback({  # custom callback
                "horse_id_val": X_horse_id_val,
                "horse_stats_val": X_horse_stats_val,
                "race_numeric_val": X_race_numeric_val,
                "course_cd_val": X_course_cd_val,
                "trk_cond_val": X_trk_cond_val,
                "y_val": y_val
            })
        ]
        
        model.fit(
            train_dict_local,
            y_train,
            validation_data=(val_dict_local, y_val),
            epochs=epochs,
            batch_size=batch_size,
            callbacks=callbacks,
            verbose=1
        )

        # -------------------------------------------------
        # I) Evaluate and return val_loss for Optuna
        # -------------------------------------------------
            # Evaluate on validation
        y_pred = model.predict(val_dict_local).ravel()
        corr, _ = stats.spearmanr(y_val, y_pred)
        # val_loss, val_mae = model.evaluate(val_dict_local, y_val, verbose=0)
        return corr

    # ---------------------------
    # run_optuna_study function.
    # ---------------------------
    def run_optuna_study():
        sampler = optuna.samplers.RandomSampler(seed=42)  # using RandomSampler for now
        pruner = optuna.pruners.MedianPruner(n_warmup_steps=10)
        try:
            study = optuna.create_study(
                study_name=study_name,
                storage=storage_url,
                load_if_exists=True,
                direction="maximize",
                sampler=sampler,
                pruner=pruner
            )
            print(f"Created new study: {study_name}")
        except optuna.exceptions.DuplicatedStudyError:
            print(f"Study {study_name} already exists. Loading it.")
            study = optuna.load_study(study_name=study_name, storage=storage_url)
        study.optimize(objective, n_trials=50)
        return study
    
    def run_action(action, study_name, storage_url):
        # 1) Depending on action, either run the study or load best params
        if action == "train":
            # run the study => produce best params
            study = run_optuna_study()
            best_params = study.best_trial.params
            logging.info("Best parameters found by Optuna [train mode]:")
            logging.info(best_params)
        elif action == "load":
            # skip running => just load best params from existing DB
            study = optuna.load_study(study_name=study_name, storage=storage_url)
            best_params = study.best_trial.params
            logging.info("Best parameters loaded from existing study [final mode]:")
            logging.info(best_params)
        else:
            raise ValueError(f"Unknown action={action}. Must be 'load' or 'train'.")
        return best_params
    
    def run_final_training(best_params):
        """
        Final training using best hyperparameters, with Spearman-based early stopping.
        No pruning callback here, since we already have best_params from Optuna.
        """
        # 1) Unpack best_params
        horse_embedding_dim = best_params["horse_embedding_dim"]
        horse_hid_layers = best_params["horse_hid_layers"]
        horse_units = best_params["horse_units"]
        race_hid_layers = best_params["race_hid_layers"]
        race_units = best_params["race_units"]
        activation = best_params["activation"]
        dropout_rate = best_params["dropout_rate"]
        l2_reg = best_params["l2_reg"]
        optimizer_name = best_params["optimizer"]
        learning_rate = best_params["learning_rate"]
        batch_size = best_params["batch_size"]
        epochs = best_params["epochs"]
        
        logging.info("Num horses: %d", num_horses)
        # 2) Build final model
        final_model = build_final_model(
            horse_vocab_size=num_horses,  
            horse_embedding_dim=best_params["horse_embedding_dim"],
            horse_stats_dim=len(horse_stats_cols),
            race_dim=len(race_numeric_cols),
            cat_feature_info=cat_feature_info,
            horse_hid_layers=best_params["horse_hid_layers"],
            horse_units=best_params["horse_units"],
            race_hid_layers=best_params["race_hid_layers"],
            race_units=best_params["race_units"],
            activation=best_params["activation"],          # e.g. "elu"
            dropout_rate=best_params["dropout_rate"],      # e.g. 0.35
            l2_reg=best_params["l2_reg"]                   # e.g. 1.5584070764453296e-05
        )
        
        # 3) Pick optimizer
        if optimizer_name == "adam":
            optimizer = keras.optimizers.Adam(learning_rate=learning_rate)
        elif optimizer_name == "nadam":
            optimizer = keras.optimizers.Nadam(learning_rate=learning_rate)
        else:
            optimizer = keras.optimizers.RMSprop(learning_rate=learning_rate)

        final_model.compile(
            optimizer=optimizer,
            loss="mse",
            metrics=["mae"]
        )

        # 4) Prepare train/val dicts
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
        
        # 5) Define callbacks
        callbacks = [
            SpearmanMetricCallback(val_dict_local, y_val),  # Must go first => sets logs["val_spearman"]
            EarlyStopping(monitor="val_spearman", mode="max", patience=30, restore_best_weights=True),
            ReduceLROnPlateau(monitor="val_spearman", mode="max", factor=0.5, patience=3, min_lr=1e-6, verbose=1),
            # We skip TFKerasPruningCallback, because no pruning in final run
            RankingMetricCallback({
                "horse_id_val": X_horse_id_val,
                "horse_stats_val": X_horse_stats_val,
                "race_numeric_val": X_race_numeric_val,
                "course_cd_val": X_course_cd_val,
                "trk_cond_val": X_trk_cond_val,
                "y_val": y_val
            })
        ]

        # 6) Train final model
        final_model.fit(
            train_dict_local,
            y_train,
            validation_data=(val_dict_local, y_val),
            epochs=epochs,
            batch_size=batch_size,
            callbacks=callbacks,
            verbose=1
        )

        # 7) Evaluate
        y_pred = final_model.predict(val_dict_local).ravel()
        corr, _ = stats.spearmanr(y_val, y_pred)
        val_loss, val_mae = final_model.evaluate(val_dict_local, y_val, verbose=0)
        logging.info(f"[FINAL] Val MSE: {val_loss:.4f}, Val MAE: {val_mae:.4f}, Spearman: {corr:.4f}")

        return final_model
    
    study_name = "horse_embedding_v1"
    storage_url = "sqlite:///horse_embedding_optuna_study.db"
    
    best_params = run_action(action, study_name, storage_url)
    final_model = run_final_training(best_params)
    
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
    # try:
    #     submodel = keras.Model(
    #         inputs=[
    #             final_model.get_layer("horse_id_input").input,
    #             final_model.get_layer("horse_stats_input").input
    #         ],
    #         outputs=final_model.get_layer("horse_embedding_out").output
    #     )
    # except Exception as e:
    #     print("[WARN] Could not build submodel for 'horse_embedding_out':", e)
    #     submodel = None
    
    # if submodel is not None:
    #     row_embeddings = submodel.predict({
    #         "horse_id_input": historical_pdf["horse_id"].astype(int).values,
    #         "horse_stats_input": historical_pdf[horse_stats_cols].astype(float).values
    #     })
    #     print(f"[INFO] Row-level embeddings shape (Option B): {row_embeddings.shape}")
    #     embed_df_row = pd.DataFrame(row_embeddings, columns=[f"embrow_{i}" for i in range(row_embeddings.shape[1])])
    #     embed_df_row["row_idx"] = historical_pdf.index
    #     merged_df_row = pd.concat([historical_pdf.reset_index(drop=True), embed_df_row.reset_index(drop=True)], axis=1)
    #     row_embed_sdf = spark.createDataFrame(merged_df_row)
        
    #     # Suppose you want to fill forward "global_speed_score_iq" in the future rows
        
    #     row_embed_sdf.write.format("jdbc") \
    #         .option("url", jdbc_url) \
    #         .option("dbtable", "horse_embedding_out_rowlevel") \
    #         .option("user", jdbc_properties["user"]) \
    #         .option("driver", jdbc_properties["driver"]) \
    #         .mode("overwrite") \
    #         .save()
    #     print("[INFO] Saved row-level 'horse_embedding_out' to DB table: horse_embedding_out_rowlevel.")
    
    # ---------------------------
    # Merge embeddings with historical data using Option A.
    # ---------------------------
    merged_raw = pd.merge(historical_pdf, embed_df_raw, on="horse_id", how="left")
    merged_df = add_combined_feature(merged_raw)
    # Suppose merged_df is your output from add_combined_feature
    combined_cols = [col for col in merged_df.columns if col.startswith("combined_")]
    combined_cols_sorted = sorted(combined_cols, key=lambda x: int(x.split("_")[1]))

    print("[DEBUG] Found combined columns:", combined_cols_sorted)
    # Then pass them to fill_forward_locf or wherever:
    embedding_cols = combined_cols_sorted

    print("\n[INFO] Merged columns after add_combined_feature:")
    print(merged_df.columns.tolist())
    print("[INFO] Sample of merged DataFrame:")
    print(merged_df.head())
    
    historical_with_embed_sdf = spark.createDataFrame(merged_df)
    all_df = historical_with_embed_sdf.unionByName(future_df, allowMissingColumns=True)
    all_df = fill_forward_locf(all_df, embedding_cols, "horse_id", "race_date")
    # cols_to_locf = ["global_speed_score_iq"]
    #all_df = fill_forward_locf(all_df, cols_to_locf, "horse_id", "race_date")
    # all_df.printSchema()
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
 
    return model_filename