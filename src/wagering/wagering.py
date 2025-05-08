import logging
import os
import sys
import traceback
import time
from pyspark.sql.functions import to_timestamp
from pyspark.sql import Window
from datetime import datetime
import pandas as pd
# Local imports
import src.wagering.wager_types as wt
from src.wagering.wagering_functions import build_race_objects, build_wagers_dict, compute_or_load_green_tracks
from src.wagering.wagering_queries import wager_queries
from src.data_preprocessing.data_prep1.data_utils import initialize_environment
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql
from src.wagering.wagers import (
    implement_ExactaWager, implement_TrifectaWager, implement_SuperfectaWager,
    implement_multi_race_wager
)
from src.wagering.wagering_helper_functions import (
    setup_logging,
    read_config,
    get_db_pool,
    parse_winners_str,
    get_user_wager_preferences,
    convert_timestamp_columns,
    gather_bet_metrics,
    save_results_to_parquet
)
from src.data_preprocessing.data_prep1.data_utils import save_parquet

import numpy as np
import pandas as pd

def add_race_level_features(races_pdf: pd.DataFrame, top_n_for_std: int = 4) -> pd.DataFrame:
    """
    For each (course_cd, race_date, race_number), compute and append:
      - fav_morn_odds: minimum (lowest) morning-line odds across all horses
      - avg_morn_odds: mean of morning-line odds across all horses
      - max_prob: highest 'score' among horses
      - second_prob: second-highest 'score' (0 if only one horse in group)
      - prob_gap: max_prob - second_prob
      - std_prob: std of top_n_for_std 'score' (default top 4 horses)
    These new columns are added to every row for that race.
    
    Parameters
    ----------
    races_pdf : pd.DataFrame
        Must have columns:
          ["course_cd", "race_date", "race_number", "morn_odds", "score"]
        plus anything else you need. Each row is one horse in that race.
    top_n_for_std : int
        How many top horses (by 'score') to include when computing std_prob.

    Returns
    -------
    pd.DataFrame
        Same shape as races_pdf, but with new columns appended:
          ["fav_morn_odds", "avg_morn_odds", "max_prob",
           "second_prob", "prob_gap", "std_prob"]
    """
    def _calc_features(grp: pd.DataFrame) -> pd.DataFrame:
        grp = grp.copy()

        # 1) fav/avg morning line odds
        if "morn_odds" in grp.columns and grp["morn_odds"].notna().any():
            grp["fav_morn_odds"] = grp["morn_odds"].min()
            grp["avg_morn_odds"] = grp["morn_odds"].mean()
        else:
            grp["fav_morn_odds"] = np.nan
            grp["avg_morn_odds"] = np.nan
        
        # 2) sort descending by 'score'
        if "score" not in grp.columns:
            # If 'score' is missing, fill these new columns with NaN/0
            grp["max_prob"] = 0.0
            grp["second_prob"] = 0.0
            grp["prob_gap"] = 0.0
            grp["std_prob"] = 0.0
            return grp
        
        sorted_grp = grp.sort_values("score", ascending=False)

        # a) max_prob = top horse
        if len(sorted_grp) > 0:
            top1 = sorted_grp.iloc[0]["score"]
        else:
            top1 = 0.0
        
        # b) second_prob = second horse
        if len(sorted_grp) > 1:
            top2 = sorted_grp.iloc[1]["score"]
        else:
            top2 = 0.0
        
        # c) prob_gap
        gap = top1 - top2
        
        # d) std of top_n_for_std horses
        top_scores = sorted_grp["score"].head(top_n_for_std)
        std_top_n = top_scores.std(ddof=0)  # population std

        grp["max_prob"] = top1
        grp["second_prob"] = top2
        grp["prob_gap"] = gap
        grp["std_prob"] = std_top_n if not np.isnan(std_top_n) else 0.0
        
        return grp

    # Group by (course_cd, race_date, race_number) and apply
    out_df = (
        races_pdf
        .groupby(["course_cd","race_date","race_number"], group_keys=False)
        .apply(_calc_features)          # ← no include_groups=False
    )
    return out_df

# ────────────────────────────────────────────────────────────
# HISTORICAL EVALUATION (WIN tickets only in this demo)
# ────────────────────────────────────────────────────────────
def evaluate_roi(bets_pdf: pd.DataFrame, races_pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Join WIN tickets back to official results and compute ROI by
    track, bet_type, or overall.  Returns a small summary table.
    """
    merged = bets_pdf.merge(races_pdf, 
                        left_on = ["course_cd","race_date","race_number","ticket"],
                        right_on= ["course_cd","race_date","race_number","saddle_cloth_number"],
                        how="left")

    merged["won"]   = merged["official_fin"] == 1      # WIN ticket only
    merged["net"]   = np.where(merged["won"],
                               merged["stake"] * merged["morn_odds"],   # gross return
                               -merged["stake"])                        # loss

    roi_tbl = (
        merged.groupby(["bet_type","course_cd"], dropna=False)
              .agg(total_staked=("stake","sum"),
                   net_profit   =("net"  ,"sum"))
              .reset_index()
    )
    roi_tbl["roi_%"] = 100 * roi_tbl["net_profit"] / roi_tbl["total_staked"]
    return roi_tbl


# ────────────────────────────────────────────────────────────
# SAVE *live* bets (future races) to DB / CSV / etc.
# ────────────────────────────────────────────────────────────
def persist_live_bets(live_bets: pd.DataFrame, out_csv: str):
    """Very light-weight example – just write a CSV you can upload."""
    if live_bets.empty:
        logging.info("No live bets generated for today.")
        return
    live_bets.to_csv(out_csv, index=False)
    logging.info(f"Wrote {len(live_bets)} live bets → {out_csv}")

def implement_strategy(spark, parquet_dir, races_pdf, wagers_pdf, bets_pdf):
    """
    Decide which wager type to run (Exacta, Trifecta, Daily Double, etc.)
    based on user prefs, then call the relevant function in wagers.py. 
    """
    # 1) Gather user inputs for the wager
    user_prefs = get_user_wager_preferences()
    Per_Track_ROI = user_prefs["Per_Track_ROI"]
    wager_type = user_prefs["wager_type"]
    wager_amount = user_prefs["wager_amount"]
    top_n = user_prefs["top_n"]
    num_legs = user_prefs["num_legs"]
    box = user_prefs["is_box"]

    # 2) Build Race objects, Wagers dict
    all_races = build_race_objects(races_pdf)
    wagers_dict = build_wagers_dict(wagers_pdf)

    # 3) Dispatch to the correct function from wagers.py
    if Per_Track_ROI:
        track_ROI = compute_or_load_green_tracks(spark)
    elif wager_type == "Exacta":
        bet_results_df = implement_ExactaWager(
            spark,
            all_races,
            wagers_dict,
            wager_amount,
            top_n=top_n,
            box=box
        )
        save_parquet(spark, bet_results_df, "exacta_wagering", parquet_dir)

    elif wager_type == "Trifecta":
        bet_results_df = implement_TrifectaWager(
            spark,
            all_races,
            wagers_dict,
            wager_amount,
            top_n=top_n,
            box=box
        )
        filename = wager_type.lower().replace(" ", "") + "_wagering"
        save_parquet(spark, bet_results_df, filename, parquet_dir)

    elif wager_type == "Superfecta":
        bet_results_df = implement_SuperfectaWager(
            spark,
            all_races,
            wagers_dict,
            wager_amount,
            top_n=top_n,
            box=box
        )
        filename = wager_type.lower().replace(" ", "") + "_wagering"
        save_parquet(spark, bet_results_df, filename, parquet_dir)

    elif wager_type in ["Daily Double", "Pick 3", "Pick 4", "Pick 5", "Pick 6"]:
        # For 'Daily Double', we have forced num_legs=2 above.
        bet_results_df = implement_multi_race_wager(
            spark,
            all_races,
            wagers_dict,
            wager_type,
            num_legs=num_legs,
            wager_amount=wager_amount,
            top_n=top_n,
            box=box
        )
        filename = wager_type.lower().replace(" ", "") + "_wagering"
        save_parquet(spark, bet_results_df, filename, parquet_dir)

    else:
        logging.info(f"'{wager_type}' not yet implemented.")
        


def main():
    """
    Main function to:
      - Initialize environment
      - Create SparkSession
      - Create DB connection pool
      - Load "races" and "wagers" data
      - Parse and run implement_strategy
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = read_config(script_dir)
    dataset_log_file = os.path.join(
        script_dir,
        f"../../logs/{datetime.now().strftime('%Y-%m-%d')}_dataset.log"
    )
    setup_logging(script_dir, log_dir=dataset_log_file)

    # 1) Create DB pool
    db_pool = get_db_pool(config)

    # 2) Initialize Spark, logging, and load data
    try:
        spark, jdbc_url, jdbc_properties, parquet_dir, _ = initialize_environment()
        setup_logging(script_dir, log_dir=dataset_log_file)

        # Load data from your wager_queries
        queries = wager_queries()
        dfs = load_data_from_postgresql(spark, jdbc_url, jdbc_properties, queries, parquet_dir)
        if "races" not in dfs:
            raise ValueError("No 'races' key found in the loaded dictionary!")
        
        # Identify "races" and "wagers" from the DFS
        for name, df in dfs.items():
            logging.info(f"DataFrame '{name}' loaded. Schema:")
            df.printSchema()
            if name == "races":
                races_df = df
            elif name == "wagers":
                wagers_df = df
            else:
                logging.error(f"Unknown DataFrame name: {name}")
                continue

        # Convert timestamp columns
        races_df, _ = convert_timestamp_columns(races_df)
        wagers_df, _ = convert_timestamp_columns(wagers_df)
        if races_df.count() == 0:
            logging.warning("No rows in 'races' DataFrame. Exiting.")
            return
        # Convert to Pandas for usage in build_race_objects
        races_pdf = races_df.toPandas()
        wagers_pdf = wagers_df.toPandas() 
    
        races_pdf = add_race_level_features(races_pdf)

        ################################################################################
        # A)  IMPLIED POOL PROBABILITY  (morning-line or tote if you later refresh)
        ################################################################################
        # Morning-line fractional odds x-1  →  implied prob = 1 / (x + 1)
        

        races_pdf["implied_prob"] = 1.0 / (races_pdf["morn_odds"] + 1.0)
    
        ################################################################################
        # B)  OVERLAY & KELLY  (single-horse WIN bets)
        ################################################################################
        races_pdf["edge"]  = races_pdf["score"] - races_pdf["implied_prob"]
         
        races_pdf["kelly"] = (
            (races_pdf["score"] * races_pdf["morn_odds"]
            - (1 - races_pdf["score"]))
            / races_pdf["morn_odds"]
        ).clip(lower=0)                   # negative edge → stake = 0  
        ################################################################################
        # C)  PLACKETT–LUCE joint probabilities  (for exacta / trifecta)
        ################################################################################
        # Optional — only if you want mathematically-priced vertical exotics
        # Convert logits to “skill” parameter s_i = exp(logit)
        races_pdf["skill"] = np.exp(races_pdf["logit"])

        # -------------------------------------------------
        # 1)  build a key shared by both data sets
        # -------------------------------------------------
        wagers_pdf["race_key"] = (
            wagers_pdf["course_cd"].str.upper().str.strip() + "_" +
            wagers_pdf["race_date"].astype(str) + "_" +
            wagers_pdf["race_number"].astype(str)
        )

        races_pdf["race_key"] = (
            races_pdf["course_cd"].str.upper().str.strip() + "_" +
            races_pdf["race_date"].astype(str) + "_" +
            races_pdf["race_number"].astype(str)
        )

        # -------------------------------------------------
        # 2)  pick the pool you care about (WIN pool here)
        # -------------------------------------------------
        win_pool = (
            wagers_pdf[wagers_pdf["wager_type"] == "Exacta"]
                .groupby("race_key", as_index=False)["pool_total"]
                .sum()                                  # → one number per race
                .rename(columns={"pool_total": "pool_total"})
        )

        # -------------------------------------------------
        # 3)  merge into races_pdf
        # -------------------------------------------------
        races_pdf = races_pdf.merge(win_pool, on="race_key", how="left")

        # -----------------------------------------------
        #  AFTER you create implied_prob / edge / kelly / skill
        # -----------------------------------------------
        BANKROLL        = 5_000       # current USD bank roll
        MAX_FRACTION    = 0.05        # risk ≤ 5 % per race
        MIN_EDGE_SINGLE = 0.02        # ≥ 2 pp overlay to fire
        MIN_IMPLIED_VOL = 5_000       # ignore tiny pools

        bet_rows = []                 # collect dictionaries

        for (trk, dt, rno), g in races_pdf.groupby(
                ["course_cd", "race_date", "race_number"]):

            # ───────── 1) WIN bets via Kelly ─────────
            g = g.sort_values("kelly", ascending=False)

            for _, row in g.iterrows():
                if row["edge"] < MIN_EDGE_SINGLE:
                    break                    # sorted -> rest will be smaller
                if pd.isna(row["pool_total"]) or row["pool_total"] < MIN_IMPLIED_VOL:
                    continue

                stake = BANKROLL * MAX_FRACTION * row["kelly"]
                if stake < 2:                # track $2 minimum
                    continue
                bet_rows.append({
                    "course_cd" : trk,
                    "race_date" : dt,
                    "race_number": rno,
                    "bet_type"  : "Exacta",
                    "ticket"    : str(row["saddle_cloth_number"]),
                    "stake"     : round(stake, 2),
                    "edge"      : row["edge"],
                    "kelly"     : row["kelly"],
                    "generated_ts": pd.Timestamp.utcnow(),
                })
            # ───────── 2) Exacta KEY (best → box next 2) ─────────
            if len(g) >= 3:
                key_horse = g.iloc[0]["saddle_cloth_number"]
                others    = g.iloc[1:3]["saddle_cloth_number"].tolist()
                ticket    = f"{key_horse}>{'/'.join(map(str, others))}"

                bet_rows.append({
                    "course_cd" : trk,
                    "race_date" : dt,
                    "race_number": rno,
                    "bet_type"  : "EXACTA_KEY",
                    "ticket"    : ticket,
                    "stake"     : 2.00,          # flat stake – tune later
                    "edge"      : None,
                    "kelly"     : None,
                    "generated_ts": pd.Timestamp.utcnow(),
                })

        bets_pdf = pd.DataFrame(bet_rows)
        
        # ────────────────────────────────────────────────────────────
        #  A)  HISTORICAL back-test  (where past races have official_fin)
        # ────────────────────────────────────────────────────────────
        roi_table = evaluate_roi(bets_pdf, races_pdf)
        print("\n=== Historical ROI table ===")
        print(roi_table.to_string(index=False))

        # ────────────────────────────────────────────────────────────
        #  B)  Split live vs. historical
        # ────────────────────────────────────────────────────────────
        bets_pdf["race_date"] = pd.to_datetime(bets_pdf["race_date"]).dt.date
        today = pd.Timestamp.utcnow().normalize().date()
        live_bets      = bets_pdf[bets_pdf["race_date"] >= today]
        historical_bets= bets_pdf[bets_pdf["race_date"] <  today]

        # optional: save both
        persist_live_bets(
            live_bets,
            os.path.join(parquet_dir, f"live_bets_{today.strftime('%Y%m%d')}.csv")
        )
        print(races_pdf.dtypes)
        
        # Convert numeric columns
        decimal_cols = ['num_tickets', 'payoff', 'pool_total']
        for col in decimal_cols:
            if col in wagers_pdf.columns:
                wagers_pdf[col] = wagers_pdf[col].astype(float)
        
        # Acquire a DB connection (if needed)
        conn = db_pool.getconn()

        logging.info("Ingestion job succeeded")
        spark.catalog.clearCache()

        # 3) Parse 'winners' → 'parsed_winners' in wagers PDF
        try:
            wagers_pdf['winners'] = wagers_pdf['winners'].astype(str)
            wagers_pdf['parsed_winners'] = wagers_pdf['winners'].apply(parse_winners_str)
        except Exception as e:
            logging.error(f"Error updating wagers winners parsing: {e}")
            conn.rollback()

        # 4) Implement the main strategy
        try:
            implement_strategy(spark, parquet_dir, races_pdf, wagers_pdf, bets_pdf)
        except Exception as e:
            logging.error(f"Error in implement_strategy: {e}")
            raise

    except Exception as e:
        logging.error(f"Error during Spark initialization: {e}")
        sys.exit(1)

    finally:
        if spark:
            spark.stop()
        if db_pool:
            db_pool.closeall()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()