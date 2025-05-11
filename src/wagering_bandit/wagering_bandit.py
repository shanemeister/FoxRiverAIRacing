import logging
import os
import sys
from datetime import datetime
import pandas as pd
import itertools
from src.wagering_bandit.wagering_bandit.bandit import ContextualBandit
import src.wagering.wager_types as wt
from src.wagering.wager_functions import build_race_objects, build_wagers_dict
from src.data_preprocessing.data_prep1.data_utils import initialize_environment
from src.wagering.wagers import implement_ExactaWager, implement_multi_race_wager, implement_TrifectaWager, implement_SuperfectaWager
from src.wagering.wager_helper_functions import (setup_logging, read_config, get_db_pool, get_user_wager_preferences)
from wagering_bandit.data_loader import load_predictions_table, assemble_bandit_training_data
from wagering_bandit.context import build_race_context, load_historical_rewards

def build_deployment_context(races_pdf: pd.DataFrame, top_n: int):
    """
    For each race, take the top_n by score, form all 2-permutations (A→B),
    and compute exactly these features:
       field_size, distance_meters, race_number,
       fav_morn_odds, avg_morn_odds,
       max_prob, second_prob, prob_gap, std_prob
    """
    rows = []

    for race_key, grp in races_pdf.groupby("race_key"):
        # sort descending by model score
        grp = grp.sort_values("score", ascending=False).reset_index(drop=True)

        # race-level aggregates:
        field_size   = len(grp)
        race_number  = grp.at[0, "race_number"]
        fav_morn_odds= grp.at[0, "morn_odds"]                # top-score horse’s odds
        avg_morn_odds= grp["morn_odds"].mean()
        max_prob     = grp.at[0, "score"]
        second_prob  = grp.at[1, "score"] if len(grp) > 1 else 0.0
        std_prob     = grp["score"].std(ddof=0)               # population std

        # take top_n horses for arms
        top_horses = grp.head(top_n)

        # build every ordered pair (A->B)
        for A, B in itertools.permutations(top_horses["saddle_cloth_number"], 2):
            row_A = top_horses[top_horses["saddle_cloth_number"] == A].iloc[0]
            row_B = top_horses[top_horses["saddle_cloth_number"] == B].iloc[0]
            arm_str = f"{A}->{B}"
            rows.append({
                "race_key":        race_key,
                "arm":             arm_str,
                "field_size":      field_size,
                "distance_meters": row_A["distance_meters"],
                "race_number":     race_number,
                "fav_morn_odds":   fav_morn_odds,
                "avg_morn_odds":   avg_morn_odds,
                "max_prob":        max_prob,
                "second_prob":     second_prob,
                "prob_gap":        row_A["score"] - row_B["score"],
                "std_prob":        std_prob,
            })

    return pd.DataFrame(rows)

def main():
    """
    Main function to:
      - Initialize environment
      - Create SparkSession
      - Create DB connection pool
      - Run Spark-based sectionals aggregation
      - Run net sentiment update
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = read_config(script_dir)
    dataset_log_file = os.path.join(script_dir, f"../../logs/{datetime.now().strftime('%Y-%m-%d')}_dataset.log")
    setup_logging(script_dir, log_dir=dataset_log_file)
    # 1) Create DB pool
    db_pool = get_db_pool(config)
    conn = db_pool.getconn()
    
    # 2) Initialize Spark, logging, and load data
    try:
        spark, jdbc_url, jdbc_properties, parquet_dir, _ = initialize_environment()        
        setup_logging(script_dir, log_dir=dataset_log_file)
        
        # 1) load your predictions & wagers into pandas
        feature_cols = [
                    "field_size",
                    "distance_meters",
                    "race_number",
                    "fav_morn_odds",
                    "avg_morn_odds",
                    "max_prob",
                    "second_prob",
                    "prob_gap",
                    "std_prob",
                ]
        pred_df, wagers_df  = load_predictions_table(spark, conn, jdbc_url, jdbc_properties, parquet_dir)

        train = input("Do you want to train the bandit model? (y/n): ").strip().lower()
        if train == 'y':
            try:

                # 2) build race‐level context & reward log
                ctx_df      = build_race_context(pred_df)
                
                rewards_df  = load_historical_rewards(
                    pred_df, wagers_df, top_n=2
                )
                
                # 3) assemble X, y, r for training
                X, y, r = assemble_bandit_training_data(
                    ctx_df, rewards_df, feature_cols
                )
               
                # 4) train & save the Exacta bandit
                cb = ContextualBandit()
                
                cb.train(contexts=X, decisions=y, rewards=r)
                cb.save(os.path.join(parquet_dir, "exacta_bandit.pkl"))

                logging.info("Exacta bandit trained and saved successfully.")
            except Exception as e:
                logging.error(f"Error training Exacta bandit: {e}", exc_info=True)
                raise
            
        predict = input("Do you want to predict the bandit model? (y/n): ").strip().lower()
        if predict == 'y':
            try:
                future_df = pred_df[pred_df["official_fin"].isnull()]
                X_df = build_deployment_context(future_df, top_n=2)

                # 3) load your trained bandit, get arm & expected reward
                cb = ContextualBandit()
                BANDIT_PATH = os.path.join(parquet_dir, "exacta_bandit.pkl")
                cb.load(BANDIT_PATH)
                arms, exps = cb.recommend(X_df[feature_cols].values)
                X_df["arm"]        = arms
                X_df["exp_reward"] = exps

                # 4) pick the best per race
                picks = X_df.loc[X_df.groupby("race_key")["exp_reward"].idxmax()]
                picks.to_csv("today_exacta_recs.csv", index=False)
                print("Wrote today_exacta_recs.csv")
                
            except Exception as e:
                logging.error(f"Error predicting with Exacta bandit: {e}", exc_info=True)
                raise
        else:
            logging.info("Skipping bandit model prediction.")
            
    except Exception as e:
        logging.error(f"Error during Spark initialization: {e}")
        sys.exit(1)
    
        logging.info("All tasks completed. Spark session stopped and DB pool closed.")
    finally:
        if spark:
            spark.stop()
        if db_pool:
            db_pool.closeall()
            
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
