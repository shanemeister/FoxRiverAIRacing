from pyspark.sql.functions import col, concat_ws, lpad, date_format
import logging
import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql
from pyspark.sql.functions import struct, collect_list
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler

def prepare_future_sequences(spark, jdbc_url, jdbc_properties, target_len=150):
    import logging
    import numpy as np
    import pandas as pd
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    # --- Step 1: Load future race data (runners) ---
    future_query = """
        SELECT horse_id, course_cd, race_date, race_number, saddle_cloth_number, post_time
        FROM runners r
        JOIN horse h ON r.axciskey = h.axciskey
        WHERE race_date >= CURRENT_DATE
    """
    future_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"({future_query}) AS subquery",
        properties=jdbc_properties
    )
    # Ensure race_date is cast to a timestamp so it converts correctly
    future_df = future_df.withColumn("race_date", F.col("race_date").cast("timestamp"))
    
    # Generate race_id for future races using course_cd, formatted race_date, and race_number
    future_df = future_df.withColumn(
        "race_id",
        F.concat_ws(
            "_",
            F.col("course_cd"),
            F.date_format(F.col("race_date"), "yyyyMMdd"),
            F.lpad(F.col("race_number").cast("string"), 2, "0")
        )
    )
    
    # Enable Arrow for efficient conversion to Pandas
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    try:
        future_pd = future_df.toPandas()
    except Exception as e:
        print("Error converting future_df to Pandas:", e)
        raise e
    
    # Ensure that race_date is in proper datetime format in Pandas
    future_pd["race_date"] = pd.to_datetime(
        future_pd["race_date"], 
        format="%Y-%m-%d %H:%M:%S", 
        utc=True
    )
    
    # --- Step 2: Load historical GPS data for past races ---
    # We’ll join gpspoint with runners on (course_cd, race_date, race_number, saddle_cloth_number)
    # to get horse_id from the same table that your future races use (and skip results_entries).
    historical_query = """
        SELECT
            g.course_cd,
            g.race_date,
            g.race_number,
            REGEXP_REPLACE(TRIM(UPPER(g.saddle_cloth_number)), '\\s+$', '') AS saddle_cloth_number,
            g.time_stamp,
            g.longitude,
            g.latitude,
            g.speed,
            g.progress,
            g.stride_frequency,
            g.post_time,
            g.location,
            h.horse_id
        FROM gpspoint g
        JOIN runners r2
            ON g.course_cd = r2.course_cd
            AND g.race_date = r2.race_date
            AND g.race_number = r2.race_number
            AND REGEXP_REPLACE(TRIM(UPPER(g.saddle_cloth_number)), '\\s+$', '')
                = REGEXP_REPLACE(TRIM(UPPER(r2.saddle_cloth_number)), '\\s+$', '')
        JOIN horse h
            ON r2.axciskey = h.axciskey
        WHERE g.speed IS NOT NULL
          AND g.progress IS NOT NULL
          AND g.stride_frequency IS NOT NULL
          AND g.race_date < CURRENT_DATE
    """
    historical_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"({historical_query}) AS subquery",
        properties=jdbc_properties
    )
    
    row_count = historical_df.count()
    logging.info(f"historical_df count: {row_count}")
    historical_df.show(5, truncate=False)

    distinct_horses_hist = historical_df.select(F.countDistinct("horse_id")).collect()[0][0]
    logging.info(f"Distinct horses in historical_df: {distinct_horses_hist}")

    future_count = future_df.count()
    logging.info(f"future_df count: {future_count}")
    future_df.show(5, truncate=False)

    distinct_horses_future = future_df.select(F.countDistinct("horse_id")).collect()[0][0]
    logging.info(f"Distinct horses in future_df: {distinct_horses_future}")
    # Insert these lines:
    print("historical_df count:", historical_df.count())
    historical_df.show(5, truncate=False)  # see if we get any actual rows

    # how many distinct horses are found in the historical side?
    distinct_horses_hist = historical_df.select(F.countDistinct("horse_id")).collect()[0][0]
    print("Distinct horses in historical_df:", distinct_horses_hist)

    # same for future_df
    print("future_df count:", future_df.count())
    future_df.show(5, truncate=False)

    distinct_horses_future = future_df.select(F.countDistinct("horse_id")).collect()[0][0]
    print("Distinct horses in future_df:", distinct_horses_future)
    
    # Cast race_date to timestamp if needed
    historical_df = historical_df.withColumn(
        "race_date",
        F.col("race_date").cast("timestamp")
    )
    
    # --- Step 3: Group historical GPS data by horse_id to form sequences ---
    # Create a struct for the features we care about:
    historical_df = historical_df.withColumn(
        "feature",
        F.struct("speed", "progress", "stride_frequency")
    )
    
    # Define a window partitioned by horse_id, ordered by race_date descending (most recent first)
    w = Window.partitionBy("horse_id").orderBy(F.desc("race_date"))
    historical_df = historical_df.withColumn("rn", F.row_number().over(w))
    
    # Keep only the most recent 'target_len' rows for each horse
    historical_recent = historical_df.filter(F.col("rn") <= target_len)
    
    # Group by horse_id and collect the features into a sequence
    historical_grouped = historical_recent.groupBy("horse_id").agg(
        F.collect_list("feature").alias("historical_sequence")
    )
    
    # Convert to Pandas so we can merge with future_pd
    historical_pd = historical_grouped.toPandas()
    
    # --- Step 4: Merge future race data with the historical sequences ---
    full_pd = future_pd.merge(historical_pd, on="horse_id", how="left")
    
    # --- Step 5: Pad/truncate sequences to fixed length ---
    def pad_sequence(seq, target_len):
        pad_val = {"speed": 0.0, "progress": 0.0, "stride_frequency": 0.0}
        if not isinstance(seq, list):
            seq = []
        seq = seq[:target_len]
        return seq + [pad_val] * (target_len - len(seq))
    
    full_pd["padded_seq"] = full_pd["historical_sequence"].apply(
        lambda x: pad_sequence(x, target_len)
    )
    
    # Log some basic checks
    num_horses_with_seq = full_pd["padded_seq"].notnull().sum()
    logging.info(f"Number of horses with a valid padded sequence: {num_horses_with_seq}")
    # Just check the first 5 for variety
    for i in range(min(5, len(full_pd))):
        logging.info(f"Horse ID: {full_pd.loc[i, 'horse_id']}")
        logging.info(f"Sequence snippet: {full_pd.loc[i, 'padded_seq'][:5]}")
    
    # For each horse, compute mean of speed/progress/stride_frequency
    def seq_means(seq):
        arr = np.array([[d["speed"], d["progress"], d["stride_frequency"]] for d in seq])
        return arr.mean(axis=0)
    
    full_pd["seq_means"] = full_pd["padded_seq"].apply(seq_means)
    logging.info("Sequence means (first 10):")
    logging.info(full_pd["seq_means"].head(10))
    
    # --- Step 6: Convert padded sequences to a NumPy array ---
    X_all = np.array([
        [[d["speed"], d["progress"], d["stride_frequency"]] for d in seq]
        for seq in full_pd["padded_seq"]
    ], dtype=np.float32)
    
    return full_pd, X_all

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

# def pad_sequence(seq, target_len):
#     """
#     Pad or truncate a horse's race history sequence to a fixed length.
#     Each element of the sequence is a dictionary with keys like 'speed', 'progress', 'stride_frequency'.
#     """
#     pad_val = {"speed": 0.0, "progress": 0.0, "stride_frequency": 0.0}
#     seq = seq[:target_len]
#     return seq + [pad_val] * (target_len - len(seq))
