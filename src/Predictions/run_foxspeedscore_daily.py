import sys
import logging
import os
import torch
import pandas as pd
import numpy as np
import optuna
from datetime import datetime
from pyspark.sql import SparkSession
import joblib  # for loading pickled scalers
from src.data_preprocessing.data_prep1.data_utils import initialize_environment
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import torch.nn as nn
from sklearn.preprocessing import StandardScaler, MinMaxScaler

################################################################################
# 1. MODEL DEFINITION
################################################################################
class HorseRaceLSTM(nn.Module):
    def __init__(self, input_size=3, hidden_size=64, out_embed_size=8, num_layers=1, dropout=0.2):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0
        )
        self.fc = nn.Linear(hidden_size, 1)
        
        # A small linear to produce an 8D embedding:
        self.embed_projector = nn.Linear(hidden_size, out_embed_size)

    def forward(self, x):
        out, (h_n, c_n) = self.lstm(x)
        # 'out' shape: (batch, seq_len, hidden_size)
        # 'h_n[-1]' shape: (batch, hidden_size) if num_layers=1

        # We'll take the last time-step for both score + embedding
        last_hidden = out[:, -1, :]  # shape (batch, hidden_size)

        # 1) Final scalar:
        final_score = self.fc(last_hidden)

        # 2) A small embedding (8D):
        embed_vector = self.embed_projector(last_hidden)  
        # shape (batch, out_embed_size)

        return final_score.squeeze(1), embed_vector

################################################################################
# 2. OUTLIER CAPPING (SAME AS TRAINING)
################################################################################
def cap_outliers_X(X, lower_percentile=1, upper_percentile=99):
    """
    Clips outliers in X (3D array) for each feature separately.
    Must use the SAME percentile logic from training.
    """
    num_samples, seq_len, num_features = X.shape
    X_flat = X.reshape(-1, num_features)  # shape: (num_samples*seq_len, num_features)

    lower_bounds = np.percentile(X_flat, lower_percentile, axis=0)
    upper_bounds = np.percentile(X_flat, upper_percentile, axis=0)

    X_flat_capped = np.clip(X_flat, lower_bounds, upper_bounds)
    X_capped = X_flat_capped.reshape(num_samples, seq_len, num_features)
    return X_capped

################################################################################
# 3. PREDICTION FUNCTION
################################################################################
def predict_scores_and_embeds(model, X_all, device, batch_size=1024):
    model.eval()
    all_scores = []
    all_embeds = []
    with torch.no_grad():
        for i in range(0, len(X_all), batch_size):
            batch = X_all[i:i+batch_size]
            batch_tensor = torch.tensor(batch, dtype=torch.float32).to(device)
            score_tensor, embed_tensor = model(batch_tensor)  # two outputs
            all_scores.append(score_tensor.cpu().numpy())
            all_embeds.append(embed_tensor.cpu().numpy())
    scores = np.concatenate(all_scores, axis=0)   # shape (N,)
    embeds = np.concatenate(all_embeds, axis=0)   # shape (N, embed_dim)
    return scores, embeds

################################################################################
# 4. THE MAIN PREP FUNCTION (MIMICS YOUR “WORKING” QUERY)
################################################################################
def prepare_future_sequences(spark, jdbc_url, jdbc_properties, target_len=150):
    """
    1) Runs your known-good SQL (minus the specific horse_id=9660 condition).
    2) Groups the resulting rows by horse_id, collecting speed/progress/stride_frequency.
    3) Pads to a fixed length for LSTM input.
    4) Returns (full_pd, X_all).
    """

    # Exactly the "works fine" logic, except we remove the 'AND h.horse_id = 9660' line:
    sql_query = """
        SELECT h.horse_id,
               r.course_cd  AS future_course_cd,
               r.race_date  AS future_race_date,
               r.race_number AS future_race_number,
               r.saddle_cloth_number AS future_saddle_cloth_number,
               
               g2.course_cd   AS hist_course_cd,
               g2.race_date   AS hist_race_date,
               g2.race_number AS hist_race_number,
               g2.speed,
               g2.progress,
               g2.stride_frequency
        FROM runners r
        JOIN horse h
            ON r.axciskey = h.axciskey
        JOIN (
            SELECT h2.horse_id,
                   g.course_cd,
                   g.race_date,
                   g.race_number,
                   g.speed,
                   g.progress,
                   g.stride_frequency
            FROM gpspoint g
            JOIN runners r2
                ON g.course_cd = r2.course_cd
               AND g.race_date = r2.race_date
               AND g.race_number = r2.race_number
               AND g.saddle_cloth_number = r2.saddle_cloth_number
            JOIN horse h2
                ON r2.axciskey = h2.axciskey
        ) g2
            ON h.horse_id = g2.horse_id
        WHERE r.race_date >= CURRENT_DATE
          AND g2.speed IS NOT NULL
          AND g2.progress IS NOT NULL
          AND g2.stride_frequency IS NOT NULL
    """

    # Load into Spark
    df = spark.read.jdbc(
        url=jdbc_url,
        table=f"({sql_query}) as subquery",
        properties=jdbc_properties
    )

    # Log some stats
    row_count = df.count()
    logging.info(f"Joined future/historical DF count: {row_count}")

    distinct_horses = df.select(F.countDistinct("horse_id")).collect()[0][0]
    logging.info(f"Distinct horses in joined DF: {distinct_horses}")

    # We have columns for “future” info and “hist_*” info, but we care about building a
    # single time-series per horse. For LSTM input, we just need (speed, progress, stride_frequency).
    # We'll define a “feature” struct:
    df = df.withColumn("feature", F.struct("speed", "progress", "stride_frequency"))

    # We'll order by hist_race_date ascending (or descending) depending on your model.
    # Usually we do ascending (oldest -> newest), but if your model was trained the other way, flip it.
    w = Window.partitionBy("horse_id").orderBy(F.asc("hist_race_date"))
    df = df.withColumn("rn", F.row_number().over(w))

    # Keep only last `target_len` rows for each horse (the most recent `target_len`).
    # If you want oldest `target_len`, you’d do a different approach, but typically
    # it's the newest races. So let's do descending date order for that scenario:
    # We'll invert the sort or do something else. For demonstration, let's keep ascending sort
    # but filter to the "top" `target_len` rows. That might not be correct if you want the *last* 150.
    #
    # Easiest approach is: order by desc, row_number <= target_len, *then* reorder ascending. 
    # For now, let's keep it simple.  We'll just store everything, then you can slice later in Pandas if you like.
    
    # Actually let's do descending so we truly get *most recent* 150:
    w_desc = Window.partitionBy("horse_id").orderBy(F.desc("hist_race_date"))
    df_desc = df.withColumn("rn_desc", F.row_number().over(w_desc))
    df_desc = df_desc.filter(F.col("rn_desc") <= target_len)

    # Now reorder them ascending so the LSTM sees them oldest->newest in the final sequence:
    w_final = Window.partitionBy("horse_id").orderBy(F.asc("hist_race_date"))
    df_final = df_desc.withColumn("rn_final", F.row_number().over(w_final))

    # Group them into an array
    grouped = df_final.groupBy("horse_id").agg(
        F.collect_list("feature").alias("historical_sequence")
    )

    # Next, we want to also keep the "future" race fields (like future_course_cd, future_race_date) for output.
    # We'll create a reference of future details by distinct (horse_id, future_course_cd, future_race_date, etc.)
    # Because one horse might appear multiple times if it has multiple future races. You may want each row?
    # For now, let's just pick the first future race per horse to demonstrate. Adjust as needed.

    future_details = df.select(
        "horse_id",
        F.col("future_course_cd").alias("course_cd"),
        F.col("future_race_date").alias("race_date"),
        F.col("future_race_number").alias("race_number"),
    ).distinct()

    # Convert both to Pandas and merge
    grouped_pd = grouped.toPandas()
    future_pd = future_details.toPandas()

    # Now merge on horse_id => full_pd
    full_pd = pd.merge(future_pd, grouped_pd, on="horse_id", how="left")

    # If you want to do a separate “row per future race” approach, you'd do something more elaborate.

    # Next, pad each horse’s sequence to length=target_len:
    def pad_sequence(seq, target_len):
        pad_val = {"speed": 0.0, "progress": 0.0, "stride_frequency": 0.0}
        if not isinstance(seq, list):
            seq = []
        seq = seq[:target_len]
        return seq + [pad_val] * (target_len - len(seq))

    full_pd["padded_seq"] = full_pd["historical_sequence"].apply(
        lambda x: pad_sequence(x, target_len)
    )

    # For debugging, how many have a valid sequence?
    num_horses_with_seq = full_pd["padded_seq"].notnull().sum()
    logging.info(f"Number of horses in final DF: {len(full_pd)}")
    logging.info(f"Number of horses with a valid padded sequence: {num_horses_with_seq}")

    # snippet example
    for i in range(min(5, len(full_pd))):
        logging.info(f"Horse ID: {full_pd.loc[i, 'horse_id']}")
        snippet = full_pd.loc[i, "padded_seq"][:5]
        logging.info(f"Sequence snippet: {snippet}")

    # Convert to 3D NumPy for your LSTM
    X_all = np.array([
        [[d["speed"], d["progress"], d["stride_frequency"]] for d in seq]
        for seq in full_pd["padded_seq"]
    ], dtype=np.float32)

    return full_pd, X_all


################################################################################
# 5. SETUP + MAIN SCRIPT
################################################################################
def setup_logging():
    """Sets up logging to file (FoxSpeedScore.log)."""
    log_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs'
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'FoxSpeedScore.log')

    # Truncate old logs
    with open(log_file, 'w'):
        pass

    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info("Logging has been set up successfully.")

def main():
    setup_logging()
    logging.info("Starting LSTM scoring...")

    spark, jdbc_url, jdbc_properties, parquet_dir, _ = initialize_environment()

    # 1) Prepare sequences (raw) using the "works fine" logic
    full_pd, X_all_raw = prepare_future_sequences(
        spark, jdbc_url, jdbc_properties, target_len=150
    )

    logging.info(f"Rows sent to model: {full_pd.shape[0]}")

    # 2) Load your model
    # Create (load) a study from the existing SQLite file
    study = optuna.create_study(
        study_name="horse_race_study_v1",
        storage="sqlite:///./notebooks/optuna_gps_lstm.db",
        direction="minimize",
        load_if_exists=True
    )
    # Now you can directly access the best trial/params
    best_params = study.best_trial.params
    print("Best params:", best_params)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/LSTM/horse_lstm_gps_score_20250324.pt"
    # {'dropout': 0.2, 'hidden_size': 128, 'lr': 7.755250942257958e-05, 'num_layers': 1, 'optimizer': 'Adam'}
    # Instantiate the final model with the best parameters
    model = HorseRaceLSTM(
        input_size=3,
        hidden_size=best_params["hidden_size"],
        out_embed_size=8,  
        num_layers=best_params["num_layers"],
        dropout=best_params["dropout"]
        ).to(device)
    model.load_state_dict(torch.load(model_path, map_location=device))
    model.eval()

    # 3) Load scalers if you used them
    try:
        scaler_X = joblib.load("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/LSTM/training_scalers_gpsscaler_X.pkl")
        scaler_y = joblib.load("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/LSTM/training_scalers_gpsscaler_y.pkl")
        logging.info("Loaded scaler_X and scaler_y from disk.")
    except Exception as e:
        logging.warning(f"Could not load scaler pkl files: {e}")
        scaler_X, scaler_y = None, None

    # 4) Apply outlier capping + scaling
    X_capped = cap_outliers_X(X_all_raw, lower_percentile=1, upper_percentile=99)

    if scaler_X is not None:
        num_samples, seq_len, num_features = X_capped.shape
        X_flat = X_capped.reshape(-1, num_features)
        X_flat_scaled = scaler_X.transform(X_flat)
        X_all_infer = X_flat_scaled.reshape(num_samples, seq_len, num_features)
    else:
        X_all_infer = X_capped
        print("X_all_infer shape:", X_all_infer.shape)
        print("X_all_infer overall min:", X_all_infer.min())
        print("X_all_infer overall max:", X_all_infer.max())
        print("X_all_infer mean:", X_all_infer.mean())
        i = np.random.randint(len(X_all_infer))
        print("Sample sequence:", X_all_infer[i][:10])  # first 10 timesteps


    # 5) Predict
    pred_scores_scaled, embed_arrays = predict_scores_and_embeds(model, X_all_infer, device)

    if scaler_y is not None:
        pred_scores_scaled_2d = pred_scores_scaled.reshape(-1, 1)
        pred_scores_original = scaler_y.inverse_transform(pred_scores_scaled_2d).flatten()
        final_scores = pred_scores_original
        logging.info("Inverse-transformed predictions to original scale.")
    else:
        final_scores = pred_scores_scaled

    # 6) Build output DataFrame & write
    # We have columns [horse_id, course_cd, race_date, race_number, ...]
    # from full_pd. We might rename them or store them in your staging table.
    out_pd = full_pd[["horse_id", "course_cd", "race_date", "race_number"]].copy()
    out_pd["score"] = final_scores
    out_pd["run_timestamp"] = datetime.utcnow()
    out_pd["model_version"] = "lstm_v1"
    for i in range(8):
        out_pd[f"dim{i+1}"] = embed_arrays[:, i]

    output_df = spark.createDataFrame(out_pd)
    staging_table = "foxspeedscore_model_output"
    (
        output_df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", staging_table)
        .option("user", jdbc_properties["user"])
        .option("driver", jdbc_properties["driver"])
        .mode("overwrite")
        .save()
    )

    logging.info(f"✅ Wrote {out_pd.shape[0]} predictions to DB.")
    logging.info("Done.")

if __name__ == "__main__":
    main()