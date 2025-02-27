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
from pyspark.sql.types import DoubleType, IntegerType
from optuna.integration import TFKerasPruningCallback
from sklearn.model_selection import train_test_split
import scipy.stats as stats
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.data_preprocessing.data_prep1.data_utils import save_parquet

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
        emb = [row[f"embed_{i}"] for i in range(8)]
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
    # ---------------------------
    # Define the Optuna Objective Function
    # ---------------------------
    def objective(trial):
        # Use all features for simplicity.
        features = embedding_features  # embedding_features defined later

        # Rebuild numeric inputs based on chosen features.
        X_num_train_subset = historical_pdf.iloc[X_train_indices][features].astype(float).values
        X_num_val_subset   = historical_pdf.iloc[X_val_indices][features].astype(float).values

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
        
        # Define hyperparameters.
        embedding_dim = 8  
        n_hidden_layers = trial.suggest_int("n_hidden_layers", 1, 5)
        units = trial.suggest_int("units_per_layer", 16, 512, step=16)
        activation = trial.suggest_categorical("activation", ["relu", "selu", "tanh", "gelu", "softplus"])
        learning_rate = trial.suggest_float("learning_rate", 5e-4, 0.01, log=True)
        batch_size = trial.suggest_categorical("batch_size", [128, 256, 512, 1024])
        epochs = trial.suggest_int("epochs", 10, 100, step=10)
        use_dropout = trial.suggest_categorical("use_dropout", [False, True])
        dropout_rate = 0.0
        if use_dropout:
            dropout_rate = trial.suggest_float("dropout_rate", 0.1, 0.5, step=0.1)
        
        # Build categorical branch.
        categorical_cols = ["course_cd", "trk_cond"]
        cat_inputs = {}
        cat_embeddings = []
        embedding_dims = {"course_cd": 4, "trk_cond": 4}
        for col in categorical_cols:
            lookup = tf.keras.layers.StringLookup(output_mode='int')
            vocab = historical_pdf[col].unique().tolist()
            lookup.adapt(vocab)
            input_layer = keras.Input(shape=(), name=f"{col}_input", dtype=tf.string)
            cat_inputs[col] = input_layer
            indices = lookup(input_layer)
            embed_layer = layers.Embedding(input_dim=len(vocab) + 1, output_dim=embedding_dims[col], name=f"{col}_embed")
            embedded = layers.Flatten()(embed_layer(indices))
            cat_embeddings.append(embedded)
        
        # Horse ID branch.
        horse_id_input = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
        horse_embedding_layer = layers.Embedding(input_dim=num_horses + 1, output_dim=embedding_dim, name="horse_embedding")
        horse_embedded = layers.Flatten()(horse_embedding_layer(horse_id_input))
        
        # Numeric branch.
        numeric_input = keras.Input(shape=(X_num_train_subset.shape[1],), name="numeric_input")
        x = numeric_input
        for _ in range(n_hidden_layers):
            x = layers.Dense(units, activation=activation, kernel_regularizer=l2(1e-4))(x)
            if use_dropout:
                x = layers.Dropout(dropout_rate)(x)
        
        # Concatenate outputs.
        combined = layers.Concatenate()([x, horse_embedded] + cat_embeddings)
        output = layers.Dense(1, activation="linear")(combined)
        
        model = keras.Model(
            inputs=[numeric_input, horse_id_input] + list(cat_inputs.values()),
            outputs=output
        )
        model.compile(
            optimizer=keras.optimizers.Adam(learning_rate=learning_rate),
            loss="mse",
            metrics=["mae"]
        )
        
        # Callbacks.
        early_stopping = EarlyStopping(monitor="val_loss", patience=10, restore_best_weights=True)
        pruning_callback = TFKerasPruningCallback(trial, "val_loss")
        lr_callback = ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=5, min_lr=1e-6, verbose=1)
        ranking_metric_callback = RankingMetricCallback((X_num_val_subset, X_horse_val, course_cd_val, trk_cond_val, y_val))
        
        model.fit(
            train_inputs_subset, y_train,
            validation_data=(val_inputs_subset, y_val),
            epochs=epochs,
            batch_size=batch_size,
            callbacks=[early_stopping, pruning_callback, lr_callback, ranking_metric_callback],
            verbose=1
        )
        
        val_loss, _ = model.evaluate(val_inputs_subset, y_val, verbose=0)
        # model.load_weights(pruning_callback.best_weights_file)
        return val_loss

    def run_optuna_study():
        sampler = optuna.samplers.TPESampler(seed=42)
        pruner = optuna.pruners.MedianPruner(n_warmup_steps=10)
        study_name = "horse_embedding_speed_stats"
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
        study.optimize(objective, n_trials=100)
        print("Completed trials:", len(study.trials))
        best_trial = study.best_trial
        print("Best trial value (Val MSE):", best_trial.value)
        for k, v in best_trial.params.items():
            print(f"{k}: {v}")
        df_trials = study.trials_dataframe()
        df_trials.to_csv("optuna_study_results.csv", index=False)
        return study

    # ---------------------------
    # Data Loading and Preparation
    # ---------------------------
    global_speed_score = assign_labels_spark(global_speed_score, alpha=0.8)
    # Assume global_speed_score is your Spark DataFrame with all required columns.
    historical_df_spark = global_speed_score.filter(F.col("data_flag") == "historical")
    future_df = global_speed_score.filter(F.col("data_flag") == "future")

    historical_pdf = historical_df_spark.toPandas()
    print("historical_pdf shape:", historical_pdf.shape)
    print("unique horse IDs in historical_pdf:", historical_pdf["horse_id"].nunique())

    unique_horses = historical_pdf["horse_id"].unique()
    horse_id_to_idx = {h: i for i, h in enumerate(unique_horses)}

    # Define embedding_features (all numeric columns for embeddings).
    embedding_features = [
        "off_finish_last_race", "time_behind", "pace_delta_time",
        "all_starts", "all_win", "all_place", "all_show", "all_fourth", "horse_itm_percentage",
        "cond_starts", "cond_win", "cond_place", "cond_show", "cond_fourth", "cond_earnings",
        "sire_itm_percentage", "sire_roi", "dam_itm_percentage", "dam_roi", "age_at_race_day",
        "speed_rating", "prev_speed_rating", "speed_improvement",
        "power", "previous_class", "class_rating",
        "avg_dist_bk_gate1_5", "avg_dist_bk_gate2_5", "avg_dist_bk_gate3_5",
        "avg_dist_bk_gate4_5", "avg_speed_fullrace_5", "avg_stride_length_5", "avg_strfreq_q1_5",
        "avg_strfreq_q2_5", "avg_strfreq_q3_5", "avg_strfreq_q4_5", 
        "running_time", "official_distance"]

    # Prepare numeric inputs and target.
    X_numerical = historical_pdf[embedding_features].astype(float).values
    historical_pdf["horse_id"] = historical_pdf["horse_id"].astype(int)
    X_horse = historical_pdf["horse_id"].values.reshape(-1)
    y = historical_pdf["relevance"].values

    all_indices = np.arange(len(historical_pdf))
    X_train_indices, X_val_indices, _, _ = train_test_split(all_indices, y, test_size=0.2, random_state=42)
    X_num_train = historical_pdf.iloc[X_train_indices][embedding_features].astype(float).values
    X_num_val   = historical_pdf.iloc[X_val_indices][embedding_features].astype(float).values
    course_cd_train = historical_pdf.iloc[X_train_indices]["course_cd"].values
    course_cd_val   = historical_pdf.iloc[X_val_indices]["course_cd"].values
    trk_cond_train  = historical_pdf.iloc[X_train_indices]["trk_cond"].values
    trk_cond_val    = historical_pdf.iloc[X_val_indices]["trk_cond"].values
    X_horse_train = X_horse[X_train_indices]
    X_horse_val   = X_horse[X_val_indices]
    y_train = historical_pdf.iloc[X_train_indices]["relevance"].values
    y_val   = historical_pdf.iloc[X_val_indices]["relevance"].values

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
    print(f"Number of horses: {num_horses}")
    print(f"Number of numeric features: {num_numeric_feats}")

    # ---------------------------
    # Run the Optuna Study and Retrieve Best Parameters
    # ---------------------------
    study = run_optuna_study()
    best_params = study.best_trial.params
    logging.info("Best parameters found by Optuna:")
    logging.info(best_params)
    print("Best parameters found by Optuna:")
    print(best_params)

    embedding_dim = 4  
    activation = best_params["activation"]
    batch_size = best_params["batch_size"]
    epochs = best_params["epochs"]

    # ---------------------------
    # Build the Final Model with Best Parameters
    # ---------------------------
    horse_id_input = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
    numeric_input = keras.Input(shape=(X_num_train.shape[1],), name="numeric_input")
    categorical_cols = ["course_cd", "trk_cond"]
    cat_inputs = {}
    cat_embeddings = []
    embedding_dims = {"course_cd": 4, "trk_cond": 4}
    for col in categorical_cols:
        lookup = tf.keras.layers.StringLookup(output_mode='int')
        vocab = historical_pdf[col].unique().tolist()
        lookup.adapt(vocab)
        input_layer = keras.Input(shape=(), name=f"{col}_input", dtype=tf.string)
        cat_inputs[col] = input_layer
        indices = lookup(input_layer)
        embed_layer = layers.Embedding(input_dim=len(vocab) + 1, output_dim=embedding_dims[col], name=f"{col}_embed")
        embedded = layers.Flatten()(embed_layer(indices))
        cat_embeddings.append(embedded)

    horse_embedding_layer = layers.Embedding(input_dim=num_horses + 1, output_dim=embedding_dim, name="horse_embedding")
    horse_embedded = layers.Flatten()(horse_embedding_layer(horse_id_input))

    x = numeric_input
    l2_reg = 1e-4
    for _ in range(best_params["n_hidden_layers"]):
        x = layers.Dense(best_params["units_per_layer"], activation=activation, kernel_regularizer=l2(l2_reg))(x)
        if best_params["use_dropout"]:
            x = layers.Dropout(best_params["dropout_rate"])(x)

    combined = layers.Concatenate()([x, horse_embedded] + cat_embeddings)
    output = layers.Dense(1, activation="linear")(combined)

    final_model = keras.Model(
        inputs=[numeric_input, horse_id_input] + list(cat_inputs.values()),
        outputs=output
    )
    final_model.compile(
        optimizer=keras.optimizers.Adam(learning_rate=best_params["learning_rate"]),
        loss="mse",
        metrics=["mae"]
    )

    final_model.fit(
        train_inputs, y_train,
        validation_data=(val_inputs, y_val),
        epochs=epochs,
        batch_size=batch_size,
        callbacks=[EarlyStopping(monitor="val_loss", patience=10, restore_best_weights=True),
                ReduceLROnPlateau(monitor="val_loss", factor=0.5, patience=5, min_lr=1e-6, verbose=1),
                RankingMetricCallback((X_num_val, X_horse_val, course_cd_val, trk_cond_val, y_val))],
        verbose=1
    )

    val_loss, val_mae = final_model.evaluate(val_inputs, y_val, verbose=0)
    print(f"Final Model - Val MSE: {val_loss:.4f}, Val MAE: {val_mae:.4f}")

    # ---------------------------
    # Extract and Save Embeddings
    # ---------------------------
    embedding_weights = horse_embedding_layer.get_weights()[0]  # shape: (num_horses+1, embedding_dim)
    embedding_dim_actual = embedding_weights.shape[1]

    # Assume horse_id_to_idx is defined earlier.
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
    merged_df = add_combined_feature(merged_df)  # This function should create your combined features.
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
    # Write Final Data to Database and Parquet
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