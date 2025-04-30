#!/usr/bin/env python3
import logging
import os
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession, functions as F

from src.wagering_bandit.wagering_bandit.bandit import ContextualBandit

################################################################################
# CONFIG - Adjust these paths/names to match your environment
################################################################################

# The “combined” Parquet with all races (past + future)
COMBINED_PARQUET = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/combined_wagering.parquet"

# The trained multi-arm bandit
MULTI_ARM_BANDIT_PATH = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/multi_arm_bandit_20250429.pkl"

# The columns needed for bandit context (same as training)
FEATURE_COLS = [
    "race_number",
    "field_size",
    "distance_meters",
    "avg_purse_val_calc",
    "avg_morn_odds",
    "fav_morn_odds",
    "max_prob",
    "second_prob",
    "prob_gap",
    "std_prob"
]

# The final picks CSV output
OUTPUT_CSV = "multi_arm_bandit_future_picks20250429.csv"

################################################################################

def main():
    # Start Spark
    spark = (SparkSession.builder
             .appName("PredictMultiArmBanditFromCombined")
             .getOrCreate())
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    logging.basicConfig(level=logging.INFO)
    logging.info(f"Reading combined Parquet: {COMBINED_PARQUET}")

    # 1) Read the single combined Parquet (all races, past + future)
    sdf = spark.read.parquet(COMBINED_PARQUET)

    # 2) Filter to future races. 
    #    Because race_date is a string, we might parse it or do a comparison.
    #    If your 'race_date' column is stored as string, do a to_date cast:
    sdf = sdf.withColumn("race_dt", F.to_date("race_date", "yyyy-MM-dd"))
    # Then filter where race_dt >= current_date
    sdf_filtered = sdf.withColumn("race_dt", F.to_date(F.col("race_date"), "yyyy-MM-dd")) \
                    .filter(F.col("race_dt") >= F.current_date())

    # Convert to Pandas
    pdf = sdf_filtered.toPandas()
    if pdf.empty:
        print("No future races found in the combined Parquet. Exiting.")
        spark.stop()
        return

    # 3) If your combined file is *already* aggregated to one-row-per-race,
    #    you can skip the aggregator. If it still has one row per horse,
    #    do the aggregator logic (like add_race_level_features, groupby, etc.)
    #    For example:
    # pdf = add_race_level_features(pdf)  # if needed

    # 4) Now each race should have the same numeric columns as training
    #    We'll group or assume each row is one race. 
    #    If there's indeed only one row per race, just iterate directly:
    #    If not, you need to groupby and pick the aggregator row. 
    #    We'll assume it's already aggregated.

    # 5) Load multi-arm bandit
    logging.info(f"Loading multi-arm bandit from {MULTI_ARM_BANDIT_PATH}")
    cb = ContextualBandit()
    cb.load(MULTI_ARM_BANDIT_PATH)
    print("Bandit arms:", cb.mab.arms)
    picks = []
    for idx, row in pdf.iterrows():
        # Build the context array for this row
        context = row[FEATURE_COLS].astype(float).values.reshape(1, -1)

        # Ask bandit for best arm
        best_arms, best_exps = cb.recommend(context)
        chosen_arm = best_arms[0]
        exp_reward = best_exps[0]

        picks.append({
            "course_cd":   row.get("course_cd", ""),
            "race_date":   row.get("race_date", ""),
            "race_number": row.get("race_number", None),
            "chosen_arm":  chosen_arm,
            "expected_net": exp_reward
        })

    # 6) Save picks
    picks_df = pd.DataFrame(picks)
    picks_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Wrote {OUTPUT_CSV} with {len(picks_df)} picks.")

    spark.stop()

if __name__ == "__main__":
    main()