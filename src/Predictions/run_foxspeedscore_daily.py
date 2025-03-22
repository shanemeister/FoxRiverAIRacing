# run_foxspeedscore_daily.py
import torch
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from src.Predictions.my_model_module import HorseRaceLSTM, prepare_sequences_for_today, predict_scores  # You can modularize these


MODEL_PATH = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/models/LSTM/foxspeedscore_lstm_20250321.pt"
MODEL_VERSION = "lstm_v1"
TARGET_SEQ_LEN = 150

# Step 1: Set up Spark
spark = SparkSession.builder.appName("FoxSpeedScoreDaily").getOrCreate()

# Step 2: Query today's races + build full_pd and X_all
full_pd, X_all = prepare_sequences_for_today(spark, target_len=TARGET_SEQ_LEN)  # You define this
print(f"Races loaded: {full_pd['race_id'].nunique()}, Horses: {len(full_pd)}")

# Step 3: Load model
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = HorseRaceLSTM(input_size=3, hidden_size=64, num_layers=2)
model.load_state_dict(torch.load(MODEL_PATH, map_location=device))
model.to(device)
model.eval()

# Step 4: Predict in batches
def predict_scores(model, X_all, device, batch_size=1024):
    model.eval()
    preds = []
    with torch.no_grad():
        for i in range(0, len(X_all), batch_size):
            batch = X_all[i:i+batch_size]
            batch_tensor = torch.tensor(batch, dtype=torch.float32).to(device)
            batch_preds = model(batch_tensor).cpu().numpy()
            preds.extend(batch_preds)
    return np.array(preds)

pred_scores = predict_scores(model, X_all, device)

# Step 5: Build output DataFrame
output_pd = full_pd[["race_id", "horse_id", "course_cd", "race_date", "race_number"]].copy()
output_pd["score"] = pred_scores
output_pd["sequence_len"] = full_pd["sequence"].apply(len)
output_pd["model_version"] = MODEL_VERSION
output_pd["run_timestamp"] = datetime.utcnow()
output_df = spark.createDataFrame(output_pd)

# Step 6: Write to DB
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

print("✅ FoxSpeedScores written to DB.")