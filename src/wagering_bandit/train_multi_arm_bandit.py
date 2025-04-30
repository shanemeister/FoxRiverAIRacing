import os
import logging
import pandas as pd
import sys
import numpy as np
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# Suppose you have the combined data in:
COMBINED_PARQUET = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/combined_wagering.parquet"

def add_no_bet_rows(pdf: pd.DataFrame):
    """
    Create a 'no_bet' row for each (course_cd, race_date, race_number) with net=0
    so the bandit can skip if it expects negative reward from any real arm.
    """
    race_cols = ["course_cd","race_date","race_number"]
    # Distill unique races
    unique_races = pdf[race_cols].drop_duplicates()

    # Build a “no bet” DataFrame
    no_bet_df = unique_races.copy()
    no_bet_df["arm"] = "no_bet"

    no_bet_df["cost"] = 0.0
    no_bet_df["payoff"] = 0.0
    no_bet_df["net"] = 0.0
    no_bet_df["hit_flag"] = 0
    no_bet_df["combos_generated"] = 0
    no_bet_df["actual_winning_combo"] = ""
    no_bet_df["generated_combos"] = ""

    # If you want to preserve race-level numeric features in no-bet rows (like field_size, etc.),
    # you can merge them in from the original pdf. This example merges a subset of columns:
    no_bet_df = pd.merge(
        no_bet_df,
        pdf[race_cols + [
            "field_size", "distance_meters",
            "avg_morn_odds","fav_morn_odds","max_prob","second_prob","prob_gap","std_prob",
            "avg_purse_val_calc","surface","track_condition"
            # plus anything else you want the bandit to see for no_bet
        ]].drop_duplicates(race_cols),
        on=race_cols,
        how="left"
    )

    # Then union it
    out = pd.concat([pdf, no_bet_df], ignore_index=True, sort=False)
    return out

def load_combined_parquet(spark):
    # Read the single combined file
    # 1. Load the Parquet file
    print(f"Loading Parquet file: {COMBINED_PARQUET}")
    sdf = spark.read.parquet(COMBINED_PARQUET)

    # 2. Display the schema
    print("Schema of the DataFrame:")
    sdf.printSchema()

    # 3. Filter for future races
    #    First, ensure the race_date column is in DateType format.
    #    If it's already a date, you can skip the to_date step.
    #    If it's a string (e.g., "yyyy-MM-dd"), convert it first.
    print("Filtering for races on or after the current date...")
    sdf_filtered = sdf.withColumn("race_dt", F.to_date(F.col("race_date"), "yyyy-MM-dd")) \
                    .filter(F.col("race_dt") < F.current_date())
    sdf_filtered = sdf_filtered.withColumnRenamed("bet_type", "arm")
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    # sdf_filtered_count = sdf_filtered.count()
    # print(f"Number of races after filtering: {sdf_filtered_count}")

    # sdf_filtered = sdf_filtered.select("arm", "course_cd", "race_date", "race_number",
    #     "field_size",
    #     "distance_meters",
    #     "avg_purse_val_calc",
    #     "avg_morn_odds",
    #     "fav_morn_odds",
    #     "max_prob",
    #     "second_prob",
    #     "prob_gap",
    #     "std_prob").show(10)
    #     root
    # |-- course_cd: string (nullable = true)
    # |-- race_date: string (nullable = true)
    # |-- race_number: integer (nullable = true)
    # |-- surface: string (nullable = true)
    # |-- distance_meters: float (nullable = true)
    # |-- track_condition: string (nullable = true)
    # |-- avg_purse_val_calc: float (nullable = true)
    # |-- race_type: string (nullable = true)
    # |-- wager_amount: float (nullable = true)
    # |-- dollar_odds: float (nullable = true)
    # |-- rank: float (nullable = true)
    # |-- score: float (nullable = true)
    # |-- fav_morn_odds: float (nullable = true)
    # |-- avg_morn_odds: float (nullable = true)
    # |-- max_prob: float (nullable = true)
    # |-- second_prob: float (nullable = true)
    # |-- prob_gap: float (nullable = true)
    # |-- std_prob: float (nullable = true)
    # |-- base_amount: float (nullable = true)
    # |-- combos_generated: integer (nullable = true)
    # |-- cost: float (nullable = true)
    # |-- payoff: float (nullable = true)
    # |-- net: float (nullable = true)
    # |-- hit_flag: integer (nullable = true)
    # |-- actual_winning_combo: string (nullable = true)
    # |-- generated_combos: string (nullable = true)
    # |-- roi: float (nullable = true)
    # |-- field_size: integer (nullable = true)
    # |-- race_base_amount: float (nullable = true)
    # |-- bet_type: string (nullable = true)
    # |-- profit: float (nullable = true)
    # |-- row_roi: double (nullable = true)

    return sdf_filtered

def build_bandit_training(pdf: pd.DataFrame, feature_cols: list):
    """
    Create X, y, r from the combined DataFrame.
    - X = pdf[feature_cols] as numeric array
    - y = pdf["arm"]
    - r = pdf["net"]  (the net is payoff - cost)
    """
    # If net is missing, or negative, or something, you might want to fill with 0
    # But presumably, net is already a float with payoff-cost included
    pdf = pdf.dropna(subset=feature_cols + ["arm", "net"])
    
    X = pdf[feature_cols].values
    y = pdf["arm"].values
    r = pdf["net"].values
    return X, y, r

def main():
    spark = (
        SparkSession.builder
        .appName("TrainMultiArmBandit")
        .getOrCreate()
    )

    # 1) Load the combined parquet
    sdf = load_combined_parquet(spark)
    # Now pdf has columns:
    #  [course_cd, race_date, race_number, surface, distance_meters, ...,
    #   cost, payoff, net, roi, arm, etc.]

    # 2) If you want "no_bet" as an extra arm with net=0, 
    #    you must create a row for each (race, 'no_bet'). 
    #    This snippet is optional:
    pdf = sdf.toPandas()
    pdf = add_no_bet_rows(pdf)

    # 3) Convert your 'net' as the reward. 
    #    The 'arm' is pdf["arm"], the context is your chosen columns.
    #    Suppose you pick e.g. ['field_size','distance_meters','track_condition','roi','avg_purse_val_calc'] 
    feature_cols = [
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

    # 4) Build X, y, r from the DataFrame
    #    We'll do a simple function below
    X, y, r = build_bandit_training(pdf, feature_cols)

    # 5) Train the multi-arm bandit
    from wagering_bandit.bandit import ContextualBandit
    cb = ContextualBandit(arms=list(np.unique(y)))  # all unique arms

    cb.train(contexts=X, decisions=y, rewards=r)

    # 6) Save the multi-arm bandit
    cb.save("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/multi_arm_bandit_20250429.pkl")

    logging.info("Multi-arm bandit trained and saved successfully.")
    
    spark.stop()

if __name__ == "__main__":
    main()