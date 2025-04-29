#!/usr/bin/env python
import logging, sys
import pandas as pd
from wagering_bandit.logging_config import configure_logging
from wagering_bandit.config        import settings
from wagering_bandit.data_loader   import load_predictions_table
from wagering_bandit.bandit        import ContextualBandit
from wagering_bandit.bets          import recommend_exacta

def main():
    configure_logging()
    log = logging.getLogger("predict")

    # 1) Load today’s predictions
    pred_df = load_predictions_table("predictions_today_calibrated")
    feature_cols = ["field_size","score","morn_odds"]
    X_today = pred_df.groupby("race_key")[feature_cols].first().values

    # 2) Load trained bandit
    cb = ContextualBandit()
    cb.load("exacta_bandit.pkl")

    # 3) Recommend arms + expectations
    arms, exps = cb.recommend(X_today)
    race_keys = pred_df["race_key"].unique()

    for rk, arm, exp in zip(race_keys, arms, exps.max(axis=1)):
        subset = pred_df[pred_df["race_key"] == rk]
        if arm == "exacta":
            horses = recommend_exacta(subset, top_n=2)
            log.info(f"{rk} → EXACTA on {horses}, exp reward={exp:.2f}")
        else:
            log.info(f"{rk} → NO BET, exp reward={exp:.2f}")

if __name__ == "__main__":
    main()