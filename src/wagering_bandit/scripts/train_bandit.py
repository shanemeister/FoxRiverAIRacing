#!/usr/bin/env python
import logging
from wagering_bandit.logging_config import configure_logging
from wagering_bandit.data_loader   import load_predictions_table, load_historical_rewards, assemble_bandit_training_data
from wagering_bandit.bandit        import ContextualBandit
from wagering_bandit.config        import settings

def main():
    configure_logging()
    log = logging.getLogger("train")

    # 1) Load predictions + build reward log
    pred_df    = load_predictions_table("predictions_20250426_151421_1_calibrated")
    rewards_df = load_historical_rewards(pred_df, "exotic_wagers")

    # 2) Assemble training arrays
    feature_cols = ["field_size","score","morn_odds"]
    X, y, r = assemble_bandit_training_data(pred_df, rewards_df, feature_cols)
    log.info(f"Training on {X.shape[0]} samples, features={feature_cols}")

    # 3) Train & save
    cb = ContextualBandit()
    cb.train(X, y, r)
    cb.save("exacta_bandit.pkl")
    log.info("Bandit model saved to exacta_bandit.pkl")

if __name__ == "__main__":
    main()