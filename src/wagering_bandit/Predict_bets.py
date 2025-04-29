import pandas as pd
from datetime import date
from src.wagering_bandit.wagering_bandit.bandit import ContextualBandit
from src.data_preprocessing.data_prep1.data_utils import initialize_environment
from src.wagering.wagering_helper_functions import setup_logging, read_config, get_db_pool
import itertools
import pandas as pd

def build_deployment_context(races_pdf: pd.DataFrame, top_n: int):
    """
    Input: races_pdf with columns:
      - race_key
      - saddle_cloth_number
      - field_size
      - distance_meters
      - score (model’s calibrated_prob)
      - morn_odds
      - …any other per-horse features…

    top_n: how many of the top scoring horses to form arms from

    Returns: DataFrame X_df where each row is one Exacta arm:
      - race_key
      - arm: tuple of two saddle_cloth_numbers (horse A then horse B)
      - features for (A,B):
         * field_size
         * distance_meters
         * morn_odds_A, morn_odds_B
         * score_A, score_B
         * prob_gap = score_A – score_B
         * etc.
    """

    rows = []
    for race_key, grp in races_pdf.groupby("race_key"):
        # pick top_n by model score
        top_horses = (
            grp
            .sort_values("score", ascending=False)
            .head(top_n)
            .reset_index(drop=True)
        )

        # for every 2‐permutation of those N horses
        for A, B in itertools.permutations(top_horses["saddle_cloth_number"], 2):
            row_A = top_horses[top_horses["saddle_cloth_number"] == A].iloc[0]
            row_B = top_horses[top_horses["saddle_cloth_number"] == B].iloc[0]

            rows.append({
                "race_key":       race_key,
                "arm":            (A, B),
                "field_size":     row_A["field_size"],  # same for entire race
                "distance_meters": row_A["distance_meters"],
                "morn_odds_A":    row_A["morn_odds"],
                "morn_odds_B":    row_B["morn_odds"],
                "score_A":        row_A["score"],
                "score_B":        row_B["score"],
                "prob_gap":       row_A["score"] - row_B["score"],
                # add any other engineered features here…
            })

    X_df = pd.DataFrame(rows)
    return X_df

def main():
    setup_logging("logs/bandit_deploy.log")
    config   = read_config()
    db_pool  = get_db_pool(config)
    spark, jdbc_url, jdbc_props, _, _ = initialize_environment()

    # 1) Load only the FUTURE rows from your calibrated table
    sql_future = f"""
      SELECT *
      FROM predictions_20250426_151421_1_calibrated
      WHERE race_date >= CURRENT_DATE
      ORDER BY race_key, calibrated_prob DESC
    """
    future_df = (
      spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"({sql_future}) as t")
            .option("user", jdbc_props["user"])
            .option("driver", jdbc_props["driver"])
            .load()
      .toPandas()
    )

    # 2) build your X context matrix for each horse/race
    #    build_deployment_context should calculate e.g. the top pick’s prob,
    #    the gap to 2nd, field_size, maybe avg_morn, etc.
    X_df = build_deployment_context(future_df, top_n=2)

    # 3) load your trained bandit, get arm & expected reward
    cb = ContextualBandit()
    cb.load("exacta_bandit.pkl")

    feature_cols = ["field_size", "calibrated_prob", "morn_odds", "prob_gap"]  # same as train
    arms, exps = cb.recommend(X_df[feature_cols].values)
    X_df["arm"]        = arms
    X_df["exp_reward"] = exps

    # 4) pick the best per race
    picks = X_df.loc[X_df.groupby("race_key")["exp_reward"].idxmax()]
    picks.to_csv("today_exacta_recs.csv", index=False)
    print("Wrote today_exacta_recs.csv")

if __name__=="__main__":
    main()