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
from pyspark.sql import functions as F

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.sql import functions as F, Window

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

# EXAMPLE USAGE:
# df_final = attach_recent_speed_figure(df_final, historical_pdf)
# Then proceed with writing out df_final to the database/parquet, etc.

def add_combined_feature(pdf):
    """
    Given a Pandas DataFrame `pdf` that contains the raw embedding columns (embed_0, embed_1, embed_2, embed_3)
    and a global_speed_score column, create 5 new columns (combined_0..combined_4) where:
      combined_0 = embed_0
      combined_1 = embed_1
      combined_2 = embed_2
      combined_3 = embed_3
      combined_4 = global_speed_score
    """
    # Verify that the required columns exist
    required_cols = [f"embed_{i}" for i in range(4)] + ["global_speed_score"]
    for col in required_cols:
        if col not in pdf.columns:
            raise ValueError(f"Column {col} not found in DataFrame.")
    
    # Create the combined vector for each row
    combined = pdf.apply(lambda row: [row[f"embed_{i}"] for i in range(4)] + [row["global_speed_score"]], axis=1)
    # Expand the list into separate columns:
    combined_df = pd.DataFrame(combined.tolist(), columns=[f"combined_{i}" for i in range(5)])
    # Concatenate the new combined columns to the DataFrame.
    pdf = pd.concat([pdf.reset_index(drop=True), combined_df], axis=1)
    # Optionally, drop the raw embedding columns:
    pdf.drop(columns=[f"embed_{i}" for i in range(4)], inplace=True)
    return pdf


def embed_and_train(spark, jdbc_url, parquet_dir, jdbc_properties, conn, global_speed_score):
    # ---------------------------------------------------
    # 1) LOAD AND PREPARE DATA
    # ---------------------------------------------------
    
    # Separate historical and future data
    # Step 1: split them properly and KEEP the subsets separate:
    historical_pdf = global_speed_score.filter(F.col("data_flag") == "historical")
    future_df     = global_speed_score.filter(F.col("data_flag") == "future")
    
    # Then convert each to Pandas if that’s your plan
    historical_pdf = historical_pdf.toPandas()
    # future_pdf     = future_pdf.toPandas()
   
    print("historical_pdf shape:", historical_pdf.shape)
    print("unique horse IDs in historical_pdf:", historical_pdf["horse_id"].nunique())
    
    # Example function to compute recent average global_speed_score per horse
    def compute_recent_avg(df, window=5):
        """
        For a given DataFrame df (for one horse) sorted by race_date,
        compute the average of the previous up to `window` races of the global_speed_score.
        """
        # Shift the score so that the current race is not included in its own average
        df['recent_avg_speed'] = df['global_speed_score'].shift(1).rolling(window=window, min_periods=1).mean()
        return df

    # Make sure your historical_pdf DataFrame is sorted correctly:
    historical_pdf = historical_pdf.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    
    # Group by horse_id and apply the rolling average calculation.
    historical_pdf = historical_pdf.groupby('horse_id', group_keys=False).apply(lambda x: compute_recent_avg(x, window=5))
    
    # Add an "as_of_date" column (you might simply use the race_date)
    historical_pdf['as_of_date'] = historical_pdf['race_date']

    # (Optional) Fill missing recent_avg_speed values with a default (e.g., global average or NaN)
    global_avg = historical_pdf['global_speed_score'].mean()
    historical_pdf['recent_avg_speed'].fillna(global_avg, inplace=True)
    
    # Now, to save this as a separate table in your database, convert it to a Spark DataFrame.
    reduced_speed_df = spark.createDataFrame(historical_pdf)

    # Define the target table name for the recent averages.
    recent_avg_table = "recent_avg_speed"  # choose an appropriate name

    # Write the DataFrame to your database using the JDBC settings.
    (
        reduced_speed_df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", recent_avg_table)
        .option("user", jdbc_properties["user"])
        .option("driver", jdbc_properties["driver"])
        .mode("overwrite")
        .save()
    )

    print("Recent average speed feature saved to table:", recent_avg_table)
    # Map each horse_id to a unique integer index (horse_id)
    unique_horses = historical_pdf["horse_id"].unique()
    horse_id_to_idx = {h: i for i, h in enumerate(unique_horses)}
    horse_id_series = historical_pdf["horse_id"].map(horse_id_to_idx)
    #historical_pdf = pd.concat([historical_pdf, horse_id_series.rename("horse_id")], axis=1)  Duplicates the horse_id and breaks the code.

    # Define numeric features (embedding_features) and target.
    embedding_features = [
        "off_finish_last_race", "time_behind", "pace_delta_time",
        "all_starts", "all_win", "all_place", "all_show", "all_fourth", "horse_itm_percentage",
        "sire_itm_percentage", "sire_roi", "dam_itm_percentage", "dam_roi", "age_at_race_day",
        "speed_rating", "prev_speed_rating", "speed_improvement",
        "power", "previous_class", "class_rating", 
        "avg_dist_bk_gate1_5", "avg_dist_bk_gate2_5", "avg_dist_bk_gate3_5",
        "avg_dist_bk_gate4_5", "avg_speed_fullrace_5", "avg_stride_length_5", "avg_strfreq_q1_5",
        "avg_strfreq_q2_5", "avg_strfreq_q3_5", "avg_strfreq_q4_5", 
        "global_speed_score", "recent_avg_speed", "running_time",
        "official_distance"
    ]
    
    X_numerical = historical_pdf[embedding_features].astype(float).values
    X_horse_id = historical_pdf["horse_id"].values
    y = historical_pdf["official_fin"].values

    # Check for NaNs and infinities.
    df_check = historical_pdf[embedding_features].copy()
    print("NaN counts in each feature:")
    print(df_check.isna().sum()[df_check.isna().sum() > 0])
    print("Infinity counts in each feature:")
    print(df_check.apply(lambda col: np.isinf(col).sum())[df_check.apply(lambda col: np.isinf(col).sum()) > 0])
    print("official_fin unique values:", historical_pdf["official_fin"].unique())
    print("Min, Max official_fin:", historical_pdf["official_fin"].min(), historical_pdf["official_fin"].max())
    
    
    # Ensure horse_id is integer and 1D.
    historical_pdf["horse_id"] = historical_pdf["horse_id"].astype(int)
    # Create a 1D numpy array of horse IDs.
    X_horse = historical_pdf["horse_id"].values.reshape(-1)

    # Train/validation split.
    all_indices = np.arange(len(historical_pdf))
    X_train_indices, X_val_indices, _, _ = train_test_split(all_indices, y, test_size=0.2, random_state=42)

    # Numeric and categorical inputs.
    X_num_train = historical_pdf.iloc[X_train_indices][embedding_features].astype(float).values
    X_num_val   = historical_pdf.iloc[X_val_indices][embedding_features].astype(float).values
    course_cd_train = historical_pdf.iloc[X_train_indices]["course_cd"].values
    course_cd_val   = historical_pdf.iloc[X_val_indices]["course_cd"].values
    trk_cond_train  = historical_pdf.iloc[X_train_indices]["trk_cond"].values
    trk_cond_val    = historical_pdf.iloc[X_val_indices]["trk_cond"].values

    # Use the pre-computed one-D X_horse array for the splits.
    X_horse_train = X_horse[X_train_indices]
    X_horse_val   = X_horse[X_val_indices]

    y_train = historical_pdf.iloc[X_train_indices]["official_fin"].values
    y_val   = historical_pdf.iloc[X_val_indices]["official_fin"].values

    train_inputs = {
        "numeric_input": X_num_train,
        "horse_id_input": X_horse_train,   # This is now 1D.
        "course_cd_input": course_cd_train,
        "trk_cond_input": trk_cond_train,
    }
    val_inputs = {
        "numeric_input": X_num_val,
        "horse_id_input": X_horse_val,     # This is now 1D.
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
    # # Hyperparameters for the model.
    # embedding_dim_trial = trial.suggest_categorical("embedding_dim", [2, 4, 8, 16, 32, 64])
    # n_hidden_layers_trial = trial.suggest_int("n_hidden_layers", 1, 5)
    # units_trial = trial.suggest_int("units_per_layer", 16, 512, step=16)
    # activation_trial = trial.suggest_categorical("activation", ["relu", "selu", "tanh", "gelu", "softplus"])
    # learning_rate_trial = trial.suggest_float("learning_rate", 1e-4, 1e-2, log=True)
    # batch_size_trial = trial.suggest_categorical("batch_size", [128, 256, 512, 1024])
    # epochs_trial = trial.suggest_int("epochs", 10, 100, step=10)
    # use_dropout_trial = trial.suggest_categorical("use_dropout", [False, True])
    # dropout_rate_trial = 0.0
    # if use_dropout_trial:
    #     dropout_rate_trial = trial.suggest_float("dropout_rate", 0.1, 0.5, step=0.1)

    def objective(trial):
        # Choose feature subset as a hyperparameter.
        feature_set_choice = trial.suggest_categorical("feature_set", ["all_features", "core_speed", "race_context"])
        if feature_set_choice == "all_features":
            features = embedding_features
        
        # Rebuild numeric inputs based on the chosen feature subset.
        X_num_train_subset = historical_pdf.iloc[X_train_indices][features].astype(float).values
        X_num_val_subset   = historical_pdf.iloc[X_val_indices][features].astype(float).values

        # Build new train and validation dictionaries (categorical inputs remain unchanged).
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

        # Reduced hyperparameter search space:
        # Fix embedding_dim and activation to the previously best values.
        embedding_dim = 4  
        n_hidden_layers_trial = 3
        units_trial = 350
        activation_trial = "softplus"  
        learning_rate_trial = 0.001605835112037
        batch_size_trial = 128  
        epochs_trial = 90  
        use_dropout_trial = False  
        dropout_rate_trial = 0.0

        # Build the categorical branch as before.
        categorical_cols_trial = ["course_cd", "trk_cond"]
        cat_inputs_trial = {}
        cat_embeddings_trial = []
        embedding_dims_trial_dict = {"course_cd": 4, "trk_cond": 4}
        for col in categorical_cols_trial:
            lookup = tf.keras.layers.StringLookup(output_mode='int')
            vocab = historical_pdf[col].unique().tolist()
            lookup.adapt(vocab)
            input_layer = keras.Input(shape=(), name=f"{col}_input", dtype=tf.string)
            cat_inputs_trial[col] = input_layer
            indices = lookup(input_layer)
            embed_layer = layers.Embedding(input_dim=len(vocab) + 1,
                                        output_dim=embedding_dims_trial_dict[col],
                                        name=f"{col}_embed")
            embedded = layers.Flatten()(embed_layer(indices))
            cat_embeddings_trial.append(embedded)

        # Build the core inputs for numeric features and horse_id.
        horse_id_input_trial = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
        numeric_input_trial = keras.Input(shape=(X_num_train_subset.shape[1],), name="numeric_input")
        horse_embedding_layer_trial = layers.Embedding(input_dim=num_horses + 1,
                                                        output_dim=embedding_dim,
                                                        name="horse_embedding")
        horse_embedded_trial = layers.Flatten()(horse_embedding_layer_trial(horse_id_input_trial))

        # Define a simple dense block.
        def dense_block(x, units, activation, dropout_rate, l2_reg):
            x = layers.Dense(units, activation=activation, kernel_regularizer=l2(l2_reg))(x)
            if dropout_rate > 0:
                x = layers.Dropout(dropout_rate)(x)
            return x

        x_trial = numeric_input_trial
        l2_reg = 1e-4
        for _ in range(n_hidden_layers_trial):
            x_trial = dense_block(x_trial, units_trial, activation_trial, dropout_rate_trial, l2_reg)

        # Concatenate the numeric branch, horse embedding, and categorical embeddings.
        combined_trial = layers.Concatenate()([x_trial, horse_embedded_trial] + cat_embeddings_trial)
        output_trial = layers.Dense(1, activation="linear")(combined_trial)

        model_trial = keras.Model(
            inputs=[numeric_input_trial, horse_id_input_trial] + list(cat_inputs_trial.values()),
            outputs=output_trial
        )
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
        model_checkpoint = keras.callbacks.ModelCheckpoint(
            filepath=checkpoint_filepath,
            save_weights_only=True,
            monitor="val_loss",
            mode="min",
            save_best_only=True
        )
        lr_callback_trial = ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=5, min_lr=1e-6, verbose=1)
        ranking_metric_callback_trial = RankingMetricCallback(
            (X_num_val_subset, X_horse_val, course_cd_val, trk_cond_val, y_val)
        )
        
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
        # study_name = "horse_embedding_search_v2"
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
    # study = run_optuna_study()
    # best_params = study.best_params
    # print("Best Params:\n", best_params)

    # ---------------------------
    # Build final model with best hyperparameters.
    # ---------------------------
    # Hard-coded best parameters from the Optuna study:
    # Extract best parameters from the Optuna study result:

    # Troubleshooting: Check if the CSV file exists and log that we're entering the block.
    logging.info("Entering final-model-building block.")
    csv_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/optuna_study_results.csv"
    if os.path.exists(csv_path):
        logging.info(f"CSV file found at: {csv_path}")
    else:
        logging.error(f"CSV file not found at: {csv_path}")

    try:
        df = pd.read_csv(csv_path)
        logging.info("CSV file loaded successfully. DataFrame shape: " + str(df.shape))
    except Exception as e:
        logging.error("Error loading CSV: " + str(e))
        raise

    # Continue with finding the best trial...
    best_trial = df.loc[df["value"].idxmin()]
    best_params = best_trial.to_dict()
    logging.info("Best parameters found by Optuna:")
    logging.info(best_params)
    print("Best parameters found by Optuna:")
    print(best_params)
    best_feature_set = best_params["params_feature_set"]  # Should be 'all_features'
    best_learning_rate = best_params["params_learning_rate"]
    best_n_hidden_layers = best_params["params_n_hidden_layers"]
    best_units_per_layer = best_params["params_units_per_layer"]

    # Fix other parameters:
    embedding_dim = 4  
    activation = "softplus"
    batch_size = 128  
    epochs = 90  
    use_dropout = False  
    dropout_rate = 0.0

    # Now, build your final model using these values:
    horse_id_input = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
    numeric_input  = keras.Input(shape=(num_numeric_feats,), name="numeric_input")

    # Categorical inputs (assuming these are processed separately):
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

    # Horse ID embedding:
    horse_embedding_layer = layers.Embedding(input_dim=num_horses, output_dim=embedding_dim, name="horse_embedding")
    horse_embedded = layers.Flatten()(horse_embedding_layer(horse_id_input))

    # Numeric branch:
    x = numeric_input
    for _ in range(best_n_hidden_layers):  # using the best n_hidden_layers value
        x = layers.Dense(best_units_per_layer, activation=activation, kernel_regularizer=l2(1e-4))(x)
        if use_dropout:
            x = layers.Dropout(dropout_rate)(x)

    # Concatenate numeric branch, horse embedding, and categorical embeddings:
    combined = layers.Concatenate()([x, horse_embedded] + cat_embeddings)
    output = layers.Dense(1, activation="linear")(combined)

    final_model = keras.Model(
        inputs=[numeric_input, horse_id_input] + list(cat_inputs.values()),
        outputs=output
    )

    final_model.compile(
        optimizer=keras.optimizers.Adam(learning_rate=best_learning_rate),
        loss="mse",
        metrics=["mae"]
    )

    # Then train with:
    final_model.fit(
        train_inputs, y_train,
        validation_data=(val_inputs, y_val),
        epochs=epochs,
        batch_size=batch_size,
        callbacks=[EarlyStopping(monitor="val_loss", patience=10, restore_best_weights=True),
                ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=5, min_lr=1e-6, verbose=1),
                RankingMetricCallback((X_num_val, X_horse_val, course_cd_val, trk_cond_val, y_val))],
        verbose=1
    )

    val_loss, val_mae = final_model.evaluate(val_inputs, y_val, verbose=0)
    print(f"Final Model - Val MSE: {val_loss:.4f}, Val MAE: {val_mae:.4f}")

    # ---------------------------------------------------
    # Extract & Save Embeddings
    # ---------------------------------------------------
    embedding_weights = horse_embedding_layer.get_weights()[0]  # shape: (num_horses, actual_embedding_dim)
    embedding_dim = embedding_weights.shape[1]  # Set embedding_dim to the actual number of columns

    idx_to_horse_id = {v: k for k, v in horse_id_to_idx.items()}

    rows = []
    for i in range(num_horses):
        horse_id = idx_to_horse_id[i]
        emb_vec = embedding_weights[i].tolist()
        rows.append([horse_id] + emb_vec)

    embed_cols = ["horse_id"] + [f"embed_{k}" for k in range(embedding_dim)]
    embed_df = pd.DataFrame(rows, columns=embed_cols)
    
    print("Sample of final embeddings:\n", embed_df.head())
    print("Columns before merge:", historical_pdf.columns.tolist())
    print("Columns in embed_df:", embed_df.columns.tolist())
        
    merged_df = pd.merge(historical_pdf, embed_df, on="horse_id", how="left")
    # Now combine the Keras embeddings with global_speed_score to get a unified 5-D feature.
    merged_df = add_combined_feature(merged_df)

    print("Merged columns:", merged_df.columns.tolist())
    
    print("Sample of final merged_df:\n", merged_df.head())
    print("Columns in merged_df:", merged_df.columns.tolist())
    
    # Convert the final merged_df (with embeddings) back to Spark:
    historical_with_embed_sdf = spark.createDataFrame(merged_df)

    # Convert future_pdf to Spark if you are still in Pandas, or keep if you already have future_df in Spark
    #future_df = spark.createDataFrame(future_pdf)  # if you had it in Pandas

    # Union them => all rows: historical + future, but only historical have the real combined_0..4
    all_df = historical_with_embed_sdf.unionByName(future_df, allowMissingColumns=True)

    all_df.printSchema()
    print("Columns in final DF:", all_df.columns)
    
    # Now attach last known speed:
    all_df = attach_recent_speed_figure(all_df, historical_with_embed_sdf)
    #   “df_final” => all_df
    #   “historical_df” => historical_with_embed_sdf

    staging_table = "horse_embedding"
    logging.info(f"Writing horse embeddings to table: {staging_table}")
    (
        all_df.write.format("jdbc")
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
    
    save_parquet(spark, all_df, model_filename, parquet_dir)
    logging.info(f"Final merged DataFrame saved as Parquet: {model_filename}")

    logging.info("*** Horse embedding job completed successfully ***")
    return model_filename