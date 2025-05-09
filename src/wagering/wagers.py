import logging
import time
import os
import json
import pandas as pd
import numpy as np
import src.wagering.wager_types as wt
from src.wagering.wager_types import MultiRaceWager, ExactaWager, TrifectaWager, SuperfectaWager
from src.wagering.wagering_helper_functions import parse_winners_str
from src.wagering.wagering_functions import find_race, refresh_upcoming_tracks
from src.wagering.wagering_helper_functions import gather_bet_metrics
from pyspark.sql.functions import col, sum as F_sum, when, count as F_count, lit
from pyspark.sql.types import (StructType, StructField, StringType, FloatType, IntegerType)
import pyspark.sql.functions as F
from src.wagering.wager_rules import WAGER_RULES, choose_rule
from decimal import Decimal

# ── CONFIG ───────────────────────────────────────────────
MIN_FIELD      = 4          # at least 4 runners
MAX_FIELD      = 20         # at most 20 runners

BANKROLL_START = 5_000.00   # starting cash balance
TRACK_MIN      = 2.00       # track $2 minimum bet
# ─────────────────────────────────────────────────────────

def implement_ExactaWager(
        spark,
        all_races,                # list[wc.Race]
        wagers_dict,              # {(trk,date,no,'Exacta'): {...}}
        user_cap,                 # max $ user typed
        top_n=3,
        box=False):
    """
    Build+back-test Exacta bets driven by the rule table.
    Returns a Spark DataFrame ready for parquet.
    """

    bankroll      = BANKROLL_START
    total_cost    = total_payoff = 0.0
    total_wagers  = wins = losses = 0
    bet_rows      = []

    # ── 1) filter by field size and keep only tracks that run again ──
    races = [r for r in all_races if MIN_FIELD <= len(r.horses) <= MAX_FIELD]
    green = refresh_upcoming_tracks(races)            # ← your helper

    logging.info("Exacta ▶ %d races after filters", len(races))

    # ── 2) iterate race-by-race ──────────────────────────────────────
    for race in races:

        # optional track filter
        if race.course_cd not in green:
            continue

        # a) pick a rule -------------------------------------------------
        rule = choose_rule(race)          # returns None → PASS
        if rule is None:
            continue

        # b) stake sizing ------------------------------------------------
        stake = bankroll * rule.pct_bankroll
        stake = max(TRACK_MIN, stake)
        stake = min(stake, user_cap)      # obey CLI cap

        # c) build ticket(s) --------------------------------------------
        combos = ExactaWager.combos_from_style(race,
                                               style=rule.bet_style,
                                               top_n=top_n)
        wager  = ExactaWager(stake, 0, box=False)      # helper for cost/ROIs
        cost   = wager.calculate_cost(combos)

        # d) historical payoff lookup -----------------------------------
        key       = (race.course_cd, race.race_date, race.race_number, "Exacta")
        info      = wagers_dict.get(key)
        payoff    = 0.0
        hit_flag  = 0

        if info:
            if any(wager.check_if_win(c, race, info["winning_combo"])
                   for c in combos):
                payoff   = wager.calculate_payoff(info["payoff"],
                                                  info["num_tickets"])
                hit_flag = 1

        # e) bankroll & counters ----------------------------------------
        bankroll      += payoff - cost
        total_cost    += cost
        total_payoff  += payoff
        total_wagers  += len(combos)
        wins          += hit_flag
        losses        += (1 - hit_flag) * len(combos)

        # f)   collect row data (trimmed to the essentials) -------------
        bet_rows.append({
            "course_cd"      : race.course_cd,
            "race_date"      : str(race.race_date),
            "race_number"    : race.race_number,
            "surface"        : race.surface,
            "distance_meters": race.distance_meters,
            "track_condition": race.track_condition,
            "avg_purse_val_calc": race.avg_purse_val_calc,
            "race_type"      : race.race_type,
            "wager_amount"   : stake,
            "rank"           : race.horses[0].rank if race.horses else None,
            "score"          : race.max_prob,
            "fav_morn_odds"  : race.fav_morn_odds,
            "avg_morn_odds"  : race.avg_morn_odds,
            "max_prob"       : race.max_prob,
            "second_prob"    : race.second_prob,
            "prob_gap"       : race.prob_gap,
            "std_prob"       : race.std_prob,
            "cost"           : cost,
            "payoff"         : payoff,
            "net"            : payoff - cost,
            "hit_flag"       : hit_flag,
            "actual_winning_combo": json.dumps(info["winning_combo"]) if info else None,
            "generated_combos"    : json.dumps(combos),
            "field_size"     : len(race.horses),
            "row_roi"        : (payoff - cost) / cost if cost else 0.0,
            "strategy"       : rule.name,
        })

    # ── 3) end-of-run summary ─────────────────────────────────────────
    net = total_payoff - total_cost
    roi = (net / total_cost) if total_cost else 0.0

    logging.info("Exacta ▶ Cost $%.2f | Payoff $%.2f | ROI %5.2f%% | "
                 "Bankroll $%.2f", total_cost, total_payoff, roi * 100, bankroll)
    print(f"Exacta ▶ Cost ${total_cost:.2f}, Return ${total_payoff:.2f}, "
          f"Net ${net:.2f}, ROI {roi:.2%}")

    # ── 4) Spark DataFrame & parquet write ────────────────────────────
    df_pd = pd.DataFrame(bet_rows)

    schema = StructType([
        StructField("course_cd",           StringType(),  True),
        StructField("race_date",           StringType(),  True),
        StructField("race_number",         IntegerType(), True),
        StructField("surface",             StringType(),  True),
        StructField("distance_meters",     FloatType(),   True),
        StructField("track_condition",     StringType(),  True),
        StructField("avg_purse_val_calc",  FloatType(),   True),
        StructField("race_type",           StringType(),  True),
        StructField("wager_amount",        FloatType(),   True),
        StructField("rank",                FloatType(),   True),
        StructField("score",               FloatType(),   True),
        StructField("fav_morn_odds",       FloatType(),   True),
        StructField("avg_morn_odds",       FloatType(),   True),
        StructField("max_prob",            FloatType(),   True),
        StructField("second_prob",         FloatType(),   True),
        StructField("prob_gap",            FloatType(),   True),
        StructField("std_prob",            FloatType(),   True),
        StructField("cost",                FloatType(),   True),
        StructField("payoff",              FloatType(),   True),
        StructField("net",                 FloatType(),   True),
        StructField("hit_flag",            IntegerType(), True),
        StructField("actual_winning_combo",StringType(),  True),
        StructField("generated_combos",    StringType(),  True),
        StructField("field_size",          IntegerType(), True),
        StructField("row_roi",             FloatType(),   True),
        StructField("strategy",            StringType(),  True),
    ])

    # ------------------------------------------------------------------
    # 4) Spark DataFrame & parquet write
    # ------------------------------------------------------------------
    df_pd = pd.DataFrame(bet_rows)

    INT_COLS = {"race_number", "field_size", "hit_flag"}   # keep these as int

    for col, dtype in df_pd.dtypes.items():
        if np.issubdtype(dtype, np.number):                # any numeric dtype
            if col in INT_COLS:
                df_pd[col] = df_pd[col].astype(int)
            else:
                df_pd[col] = df_pd[col].astype(float)

    # now the dtypes match the Spark schema exactly
    df_spark = (
        spark.createDataFrame(df_pd, schema=schema)
            .withColumn("bet_type", lit("exacta_rule_engine"))
    )

    hist_path = (
        "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/exacta_history"
    )
    df_spark.write.mode("overwrite").parquet(hist_path)

    return df_spark

def implement_TrifectaWager(spark, all_races, wagers_dict, wager_amount, top_n, box):
    """
    Generates a list of bets (combos) for each race using trifecta logic,
    calculates the cost/payoff, and returns a Spark DataFrame with row-level metrics 
    per race/bet scenario.

    In this context, a trifecta wager requires predicting the exact order 
    of the top three finishers. The function assumes a corresponding TrifectaWager 
    class is available with methods:
      - generate_combos(race): Returns a list of trifecta combinations (list of lists).
      - calculate_cost(combos): Calculates the total cost for the generated combos.
      - check_if_win(combo, race, actual_combo): Checks if the given combo wins.
      - calculate_payoff(race_payoff, posted_base): Calculates the payoff for a win.
    """
    total_cost = 0.0
    total_payoff = 0.0
    bet_results = []  # will hold dicts for each race (row_data)

    # Counters for wagers, wins, losses
    total_wagers = 0
    total_wins = 0
    total_losses = 0
    bad_race_wager_info = 0
    # Filter races by field size
    filtered_races = []
    filtered_races = [race for race in all_races if 5 <= len(race.horses) <= 20]
    
    for race in filtered_races:
        key = (race.course_cd, race.race_date, race.race_number, "Trifecta")
        race_wager_info = wagers_dict.get(key, None)
        num_tickets = race_wager_info["num_tickets"] if race_wager_info and "num_tickets" in race_wager_info else None 
        # Set base_amount for this race

        if race_wager_info and "num_tickets" in race_wager_info:
            try:
                num_tickets_val = float(num_tickets)
                if num_tickets_val is None or num_tickets_val == 0.0:
                    print(f"Debug 3: num_tickets is None or zero for {key}, setting race_base_amount = 1.0")
                    bad_race_wager_info += 1
                    continue
                else:
                    race_base_amount = num_tickets_val
            except Exception as e:
                logging.info(f"Debug 3: Could not convert num_tickets to float for {key}: {e} (value: {num_tickets}, type: {type(num_tickets)})")
                bad_race_wager_info += 1
                continue
        else:
            logging.info(f"Debug1 New Race: key={key}, race_wager_info={race_wager_info}, num_tickets={num_tickets} (type: {type(num_tickets)})") 
            bad_race_wager_info += 1 
            continue 
    
    # Instantiate the trifecta wager
    my_wager = TrifectaWager(wager_amount, top_n, box)
    
    # Filter races by field size (ensure there are at least 5 horses for a trifecta)
    filtered_races = [race for race in all_races if len(race.horses) >= 5]

    for race in filtered_races:
        # Generate trifecta combos, e.g., each combo is a list of three horse_ids
        combos = my_wager.generate_combos(race)
        cost = my_wager.calculate_cost(combos)   # total cost for these combos

        # Use a key indicating a trifecta wager
        key = (race.course_cd, race.race_date, race.race_number, "Trifecta")
        race_wager_info = wagers_dict.get(key, None)

        payoff = 0.0
        actual_combo = None
        winning_combo_found = False

        if race_wager_info:
            actual_combo = race_wager_info.get('winning_combo')  # e.g. [["5"], ["3"], ["8"]]
            winner_score = runnerup_score = None
            winner_rank  = runnerup_rank  = None

            if actual_combo and len(actual_combo) >= 2:
                # ────────────────────────────────────────────────────────────────
                #  Winner / runner-up model scores and ranks + gap metrics
                # ────────────────────────────────────────────────────────────────
                winner_score = runnerup_score = None
                winner_rank  = runnerup_rank  = None
                gap12 = gap23 = None            # default None → Spark NULL

                # Map program numbers → HorseEntry for quick lookup
                prog_map = {h.program_num: h for h in race.horses}

                # Pull CatBoost scores for the actual 1-2 finishers
                if actual_combo and len(actual_combo) >= 2:
                    win_prog, place_prog = actual_combo[0][0], actual_combo[1][0]

                    if win_prog in prog_map:
                        he = prog_map[win_prog]
                        winner_score, winner_rank = he.prediction, he.rank
                    if place_prog in prog_map:
                        he = prog_map[place_prog]
                        runnerup_score, runnerup_rank = he.prediction, he.rank

                # Gap metrics based on race-level features you already stored
                gap12 = race.prob_gap                          # top1 – top2
                gap23 = race.max_prob - race.second_prob       # top2 – top3

            # -------------------------------------------------
            # 1️⃣  Pull model scores / ranks of the actual 1-2 finishers
            # -------------------------------------------------
            race_payoff = float(race_wager_info.get('payoff', 0))
            logging.info(f"\n--- Debugging Race ---")
            logging.info(f"Race Key: {key}")
            logging.info(f"Wager Amount: {wager_amount}")
            logging.info(f"Generated Combos: {combos}")
            logging.info(f"Posted Base: {race_base_amount:.2f}")
            logging.info(f"Cost for Combos: {cost:.2f}")
            logging.info(f"Actual Winning Combo: {actual_combo}")
            logging.info(f"Listed Payoff: {race_payoff:.2f}")
            logging.info(f"Bad Race Wager info so far: {bad_race_wager_info}")

            # Check if any generated combo matches the actual winning combo
            for combo in combos:
                if my_wager.check_if_win(combo, race, actual_combo):
                    payoff = my_wager.calculate_payoff(race_payoff, race_base_amount)
                    logging.info(f"=> Match Found! Winning Combo: {combo}, Payoff = {payoff:.2f}")
                    winning_combo_found = True
                    break
        else:
            logging.debug(f"No wager info found for {key}")

        total_cost += cost
        total_payoff += payoff

        num_tickets = len(combos)
        total_wagers += num_tickets
        logging.info(f"Total Wagers: {total_wagers}, Cost: {total_cost}, Total Payoff: {total_payoff}, Net: {total_payoff - total_cost}")
        logging.info(f"Total Wagers: {total_wagers}, Wins: {total_wins}, Losses: {total_losses}") 
        if winning_combo_found:
            total_wins += 1
            total_losses += (num_tickets - 1)
        else:
            total_losses += num_tickets

        # Gather metrics for this race
        field_size = len(race.horses)
        row_data = gather_bet_metrics(
            race=race,
            combos=combos,
            cost=cost,
            payoff=payoff,
            my_wager=my_wager,
            actual_combo=actual_combo,
            field_size=field_size,
        )
        row_data.update({
            "winner_score"   : float(winner_score)   if winner_score  is not None else None,
            "runnerup_score" : float(runnerup_score) if runnerup_score is not None else None,
            "winner_rank"    : float(winner_rank)    if winner_rank   is not None else None,
            "runnerup_rank"  : float(runnerup_rank)  if runnerup_rank is not None else None,
            "gap12"          : float(gap12) if gap12 is not None else None,
            "gap23"          : float(gap23) if gap23 is not None else None,
        })
        row_data["fav_morn_odds"]   = float(race.fav_morn_odds) if race.fav_morn_odds is not None else None
        row_data["avg_morn_odds"]   = float(race.avg_morn_odds) if race.avg_morn_odds is not None else None
        row_data["max_prob"]        = float(race.max_prob)
        row_data["second_prob"]     = float(race.second_prob)
        row_data["prob_gap"]        = float(race.prob_gap)
        row_data["std_prob"]        = float(race.std_prob)      
        # Store the combos as arrays (lists) converted to JSON strings for persistence
        import json
        row_data["actual_winning_combo"] = json.dumps(actual_combo)  # e.g. '[["5"], ["3"], ["8"]]'
        row_data["generated_combos"] = json.dumps(combos)             # e.g. '[["3","5","8"], ...]'
        bet_results.append(row_data)

    net = total_payoff - total_cost
    roi = (net / total_cost) if total_cost > 0 else 0.0

    print(f"Trifecta Box (Top {top_n}) => "
          f"Cost: ${total_cost:.2f}, Return: ${total_payoff:.2f}, "
          f"Net: ${net:.2f}, ROI: {roi:.2%}")
    print(f"Total Wagers: {total_wagers}, Wins: {total_wins}, Losses: {total_losses}")

    # Convert bet_results (list of dicts) into a Spark DataFrame

    schema = StructType([
        StructField("course_cd", StringType(), True),
        StructField("race_date", StringType(), True),
        StructField("race_number", IntegerType(), True),
        StructField("surface", StringType(), True),
        StructField("distance_meters", FloatType(), True),
        StructField("track_condition", StringType(), True),
        StructField("avg_purse_val_calc", FloatType(), True),
        StructField("race_type", StringType(), True),
        StructField("wager_amount",  FloatType(), True),
        StructField("dollar_odds",  FloatType(), True),
        StructField("rank",  FloatType(), True),
        StructField("score",  FloatType(), True),
        StructField("fav_morn_odds",  FloatType(), True),
        StructField("avg_morn_odds",  FloatType(), True),
        StructField("max_prob",  FloatType(), True),
        StructField("second_prob",  FloatType(), True),
        StructField("prob_gap",  FloatType(), True),
        StructField("std_prob",  FloatType(), True),  
        StructField("base_amount", FloatType(), True),
        StructField("combos_generated", IntegerType(), True),
        StructField("cost", FloatType(), True),
        StructField("payoff", FloatType(), True),
        StructField("net", FloatType(), True),
        StructField("hit_flag", IntegerType(), True),
        StructField("actual_winning_combo", StringType(), True),
        StructField("generated_combos", StringType(), True),
        StructField("roi", FloatType(), True),
        StructField("field_size", IntegerType(), True),
        StructField("race_base_amount", FloatType(), True),
        StructField("winner_score"  , FloatType(), True),
        StructField("runnerup_score", FloatType(), True),
        StructField("winner_rank"   , FloatType(), True),
        StructField("runnerup_rank" , FloatType(), True),
        StructField("gap12"         , FloatType(), True),
        StructField("gap23"         , FloatType(), True),
    ])

    df_results = spark.createDataFrame(bet_results, schema=schema)
    
    df_results = df_results.withColumn("bet_type", lit(f"trifecta_top{top_n}_box"))
    df_results = df_results.withColumn("profit", col("payoff") - col("cost"))
    df_results = df_results.withColumn(
        "row_roi",
        when(col("cost") > 0, col("profit") / col("cost")).otherwise(0.0)
    )

    return df_results

def implement_SuperfectaWager(spark, all_races, wagers_dict, wager_amount, top_n, box):
    """
    Generates a list of bets (combos) for each race using superfecta logic, calculates the cost and payoff,
    and returns a Spark DataFrame with row-level metrics per race bet scenario.

    A Superfecta wager predicts the exact order of the top 4 finishers.
    """
    total_cost = 0.0
    total_payoff = 0.0
    bet_results = []  # will hold dicts for each race (row_data)

    # Counters for wagers, wins, losses
    total_wagers = 0
    total_wins = 0
    total_losses = 0
    bad_race_wager_info = 0
    # Filter races by field size
    filtered_races = []
    filtered_races = [race for race in all_races if 6 <= len(race.horses) <= 20]
    
    for race in filtered_races:
        key = (race.course_cd, race.race_date, race.race_number, "Superfecta")
        race_wager_info = wagers_dict.get(key, None)
        num_tickets = race_wager_info["num_tickets"] if race_wager_info and "num_tickets" in race_wager_info else None 
        # Set base_amount for this race

        if race_wager_info and "num_tickets" in race_wager_info:
            try:
                num_tickets_val = float(num_tickets)
                if num_tickets_val is None or num_tickets_val == 0.0:
                    print(f"Debug 3: num_tickets is None or zero for {key}, setting race_base_amount = 1.0")
                    bad_race_wager_info += 1
                    continue
                else:
                    race_base_amount = num_tickets_val
            except Exception as e:
                logging.info(f"Debug 3: Could not convert num_tickets to float for {key}: {e} (value: {num_tickets}, type: {type(num_tickets)})")
                bad_race_wager_info += 1
                continue
        else:
            logging.info(f"Debug1 New Race: key={key}, race_wager_info={race_wager_info}, num_tickets={num_tickets} (type: {type(num_tickets)})") 
            bad_race_wager_info += 1 
            continue 
    
    my_wager = SuperfectaWager(wager_amount, top_n, box)
    
    for race in filtered_races:
        # Generate combos (list of tuples), e.g. [("3", "7", "2", "5"), ...]
        combos = my_wager.generate_combos(race)
        cost = my_wager.calculate_cost(combos)   # total $ cost for these combos

        key = (race.course_cd, race.race_date, race.race_number, "Superfecta")
        race_wager_info = wagers_dict.get(key, None)

        payoff = 0.0
        actual_combo = None
        winning_combo_found = False

        if race_wager_info:
            actual_combo = race_wager_info.get('winning_combo')  # e.g. [["5"], ["3"], ["8"]]
            race_payoff = float(race_wager_info.get('payoff', 0))
            logging.info(f"\n--- Debugging Race ---")
            logging.info(f"Race Key: {key}")
            logging.info(f"Wager Amount: {wager_amount}")
            logging.info(f"Generated Combos: {combos}")
            logging.info(f"Posted Base: {race_base_amount:.2f}")
            logging.info(f"Cost for Combos: {cost:.2f}")
            logging.info(f"Actual Winning Combo: {actual_combo}")
            logging.info(f"Listed Payoff: {race_payoff:.2f}")
            logging.info(f"Bad Race Wager info so far: {bad_race_wager_info}")
            # Check each combo to see if it matches the actual winning combo
            for combo in combos:
                if my_wager.check_if_win(combo, race, actual_combo):
                    payoff = my_wager.calculate_payoff(race_payoff, race_base_amount)
                    logging.info(f"=> Match Found! Winning Combo: {combo}, Payoff = {payoff:.2f}")
                    winning_combo_found = True
                    break
        else:
            logging.debug(f"No wager info found for {key}")

        total_cost += cost
        total_payoff += payoff

        # Count wagers, wins, losses
        num_tickets = len(combos)  # how many superfecta combos we bet in this race
        total_wagers += num_tickets
        logging.info(f"Total Wagers: {total_wagers}, Cost: {total_cost}, Total Payoff: {total_payoff}, Net: {total_payoff - total_cost}")
        logging.info(f"Total Wagers: {total_wagers}, Wins: {total_wins}, Losses: {total_losses}")   
        if winning_combo_found:
            # Exactly 1 of the combos is a winner; the rest lose
            total_wins += 1
            total_losses += (num_tickets - 1)
        else:
            total_losses += num_tickets

        # Collect the row data for this race
        field_size = len(race.horses)
        row_data = gather_bet_metrics(
            race=race,
            combos=combos,
            cost=cost,
            payoff=payoff,
            my_wager=my_wager,
            actual_combo=actual_combo,
            field_size=field_size,
        )
        row_data["fav_morn_odds"]   = float(race.fav_morn_odds) if race.fav_morn_odds is not None else None
        row_data["avg_morn_odds"]   = float(race.avg_morn_odds) if race.avg_morn_odds is not None else None
        row_data["max_prob"]        = float(race.max_prob)
        row_data["second_prob"]     = float(race.second_prob)
        row_data["prob_gap"]        = float(race.prob_gap)
        row_data["std_prob"]        = float(race.std_prob)
        
        # === Add the combos as arrays (instead of storing them as strings) ===
        row_data["actual_winning_combo"] = actual_combo      # list of lists, e.g. [["1"], ["2"], ["3"], ["4"]]
        row_data["generated_combos"] = combos                # list of tuples, e.g. [("3", "7", "2", "5"), ...]
        bet_results.append(row_data)

    net = total_payoff - total_cost
    roi = (net / total_cost) if total_cost > 0 else 0.0

    print(f"Superfecta Box (Top {top_n}) => "
          f"Cost: ${total_cost:.2f}, Return: ${total_payoff:.2f}, "
          f"Net: ${net:.2f}, ROI: {roi:.2%}")
    print(f"Total Wagers: {total_wagers}, Wins: {total_wins}, Losses: {total_losses}")

    # ------------------------------------------------------------------------
    # Convert bet_results (list of dicts) into a Pandas DataFrame
    # ------------------------------------------------------------------------
    for row in bet_results:
        if "actual_winning_combo" in row:
            row["actual_winning_combo"] = json.dumps(row["actual_winning_combo"])  # Convert to JSON string
        if "generated_combos" in row:
            row["generated_combos"] = json.dumps(row["generated_combos"])  # Convert to JSON string
    df_results = pd.DataFrame(bet_results)

    schema = StructType([
        StructField("course_cd", StringType(), True),
        StructField("race_date", StringType(), True),
        StructField("race_number", IntegerType(), True),
        StructField("surface", StringType(), True),
        StructField("distance_meters", FloatType(), True),
        StructField("track_condition", StringType(), True),
        StructField("avg_purse_val_calc", FloatType(), True),
        StructField("race_type", StringType(), True),
        StructField("wager_amount",  FloatType(), True),
        StructField("dollar_odds",  FloatType(), True),
        StructField("rank",  FloatType(), True),
        StructField("score",  FloatType(), True),
        StructField("fav_morn_odds",  FloatType(), True),
        StructField("avg_morn_odds",  FloatType(), True),
        StructField("max_prob",  FloatType(), True),
        StructField("second_prob",  FloatType(), True),
        StructField("prob_gap",  FloatType(), True),
        StructField("std_prob",  FloatType(), True),  
        StructField("base_amount", FloatType(), True),
        StructField("combos_generated", IntegerType(), True),
        StructField("cost", FloatType(), True),
        StructField("payoff", FloatType(), True),
        StructField("net", FloatType(), True),
        StructField("hit_flag", IntegerType(), True),
        StructField("actual_winning_combo", StringType(), True),
        StructField("generated_combos", StringType(), True),
        StructField("roi", FloatType(), True),
        StructField("field_size", IntegerType(), True),
        StructField("race_base_amount", FloatType(), True),
    ])

    # Convert bet_results into a Spark DataFrame with the defined schema
    df_results = spark.createDataFrame(bet_results, schema=schema)
    
    df_results = df_results.withColumn("bet_type", lit(f"superfecta_top{top_n}_box"))
    df_results = df_results.withColumn("profit", col("payoff") - col("cost"))
    df_results = df_results.withColumn(
        "row_roi",
        when(col("cost") > 0, col("profit") / col("cost")).otherwise(0.0)
    )
    # Return the DataFrame with combos preserved as JSON strings
    return df_results

def implement_multi_race_wager(
    spark,
    all_races,
    wagers_dict,
    wager_type,
    num_legs,
    wager_amount,  # e.g. 2
    top_n,
    box
):    
    # Master counters
    total_cost = 0.0
    total_payoff = 0.0
    total_wagers = 0
    total_wins = 0
    total_losses = 0
    bad_race_wager_info = 0
    
    results = []

    # Build the multi-leg wager
    my_wager = MultiRaceWager(
        wager_amount,  # user-specified base
        num_legs,
        top_n,
        box=box
    )
    
    # 1) Filter wagers by the 4th item in key => must match wager_type exactly
    wagers_subset = {
        k: v
        for k, v in wagers_dict.items()
        if k[3] == wager_type
    }
    logging.info(f"Filtered wagers_subset for '{wager_type}': {len(wagers_subset)} entries")

    # 2) Process each wager from the subset
    for (course_cd, date, final_rn, wtype), wrow in wagers_subset.items():
        
        # Safely retrieve winning_combo; if missing or length < num_legs, skip
        leg_winners = wrow.get("winning_combo")
        if not leg_winners or len(leg_winners) < num_legs:
            continue
        
        # Attempt to convert final_rn to int
        if final_rn is None:
            logging.debug(f"Skipping key ({course_cd}, {date}, {final_rn}, {wtype}) because final_rn is None")
            bad_race_wager_info += 1
            continue
        try:
            final_rn_int = int(final_rn)
        except Exception as e:
            logging.debug(f"Cannot convert final_rn ({final_rn}) to int for key ({course_cd}, {date}, {final_rn}, {wtype}): {e}")
            bad_race_wager_info += 1
            continue

        # 3) Build consecutive legs => if final_rn_int=5 & num_legs=3 => [3,4,5]
        leg_numbers = [final_rn_int - (num_legs - 1) + i for i in range(num_legs)]

        # 4) Gather the Race objects for each leg, ensuring 4 <= #horses <= 20
        race_objs = []
        missing_leg = False
        for leg_num in leg_numbers:
            r_obj = find_race(all_races, course_cd, date, leg_num)
            if r_obj is None:
                missing_leg = True
                break
            # Filter by 4..20 horses in each leg
            if not (4 <= len(r_obj.horses) <= 20):
                missing_leg = True
                break
            race_objs.append(r_obj)

        if missing_leg or len(race_objs) < num_legs:
            bad_race_wager_info += 1
            continue

        # 5) parse the posted_base (num_tickets) for multi-race
        try:
            posted_base = float(wrow["num_tickets"])
            if posted_base is None or posted_base <= 0.0:
                logging.debug(f"Skipping multi-race {course_cd} {date}, final_rn={final_rn}, posted_base=0.0 or None invalid")
                bad_race_wager_info += 1
                continue
        except (TypeError, ValueError, KeyError) as e:
            logging.debug(f"Skipping because posted_base invalid for key=({course_cd},{date},{final_rn},{wtype}): {e}")
            bad_race_wager_info += 1
            continue

        # 6) Generate combos and cost
        all_combos = my_wager.generate_combos(race_objs)
        cost = my_wager.calculate_cost(all_combos)  # (#combos * user base)
        
        posted_payoff = float(wrow.get("payoff", 0.0))

        # 7) Check if any combo matches the winning combo -> scale payoff
        payoff = 0.0
        winning_found = False
        if len(leg_winners) == num_legs:
            for combo in all_combos:
                if my_wager.check_if_win(combo, race_objs, leg_winners):
                    payoff = posted_payoff * (my_wager.base_amount / posted_base)
                    winning_found = True
                    break

        logging.info("\n--- Multi-Race Wager Debug ---")
        logging.info(f"Race Key: ({course_cd}, {date}, Final Leg #{final_rn})")
        logging.info(f"Legs: {leg_numbers}")
        logging.info("Top Picks per Leg:")
        for i, race_obj in enumerate(race_objs):
            top_picks = my_wager.get_top_n_for_leg(i, race_obj)
            logging.info(f"  [Leg {i+1}] top picks => {top_picks}")

        logging.info(f"Generated {len(all_combos)} combos => {all_combos}")
        logging.info(f"Actual Winning Combo: {leg_winners}")
        logging.info(f"Posted Base: ${posted_base:.2f}")
        logging.info(f"Listed Payoff: ${posted_payoff:.2f}")
        logging.info(f"Cost for Combo Set: ${cost:.2f}")
        logging.info(f"Payoff: ${payoff:.2f}")

        if payoff > 0:
            logging.info("=> Match Found! ✅\n")
        else:
            logging.info("=> No Match ❌\n")

        # Increment master counters
        total_cost += cost
        total_payoff += payoff
        
        num_tickets = len(all_combos)  # combos = "tickets" for final leg
        total_wagers += num_tickets
        if winning_found:
            total_wins += 1  # exactly 1 final-leg "hit"
            total_losses += (num_tickets - 1)
        else:
            total_losses += num_tickets
        
        # Log incremental totals
        net_so_far = total_payoff - total_cost
        logging.info(
            f"[Running Totals] total_wagers={total_wagers}, "
            f"cost={total_cost:.2f}, payoff={total_payoff:.2f}, net={net_so_far:.2f}, "
            f"wins={total_wins}, losses={total_losses}, bad_race_wager_info={bad_race_wager_info}"
        )

        # 8) Gather row-level metrics
        final_race = race_objs[-1]  # the final leg
        field_size = len(final_race.horses)

        row_data = gather_bet_metrics(
            race=final_race,
            combos=all_combos,
            cost=cost,
            payoff=payoff,
            my_wager=my_wager,
            actual_combo=leg_winners,
            field_size=field_size
        )
        row_data["fav_morn_odds"]   = float(final_race.fav_morn_odds) if final_race.fav_morn_odds is not None else None
        row_data["avg_morn_odds"]   = float(final_race.avg_morn_odds) if final_race.avg_morn_odds is not None else None
        row_data["max_prob"]        = float(final_race.max_prob)
        row_data["second_prob"]     = float(final_race.second_prob)
        row_data["prob_gap"]        = float(final_race.prob_gap)
        row_data["std_prob"]        = float(final_race.std_prob)
        
        row_data["legs_str"] = '|'.join(str(x) for x in leg_numbers)
        row_data["official_winners_str"] = str(leg_winners)
        row_data["wager_type"] = str(wager_type)
        row_data["course_cd"] = str(course_cd)
        row_data["race_date"] = str(date)
        row_data["final_leg"] = int(final_rn)
        row_data["race_base_amount"] = posted_base
        
        results.append(row_data)

    # 9) Build Spark DataFrame
    schema = StructType([
        StructField("course_cd", StringType(), True),
        StructField("race_date", StringType(), True),
        StructField("race_number", IntegerType(), True),
        StructField("surface", StringType(), True),
        StructField("distance_meters", FloatType(), True),
        StructField("track_condition", StringType(), True),
        StructField("avg_purse_val_calc", FloatType(), True),
        StructField("race_type", StringType(), True),
        StructField("wager_amount",  FloatType(), True),
        StructField("dollar_odds",  FloatType(), True),
        StructField("rank",  FloatType(), True),
        StructField("score",  FloatType(), True),
        StructField("fav_morn_odds",  FloatType(), True),
        StructField("avg_morn_odds",  FloatType(), True),
        StructField("max_prob",  FloatType(), True),
        StructField("second_prob",  FloatType(), True),
        StructField("prob_gap",  FloatType(), True),
        StructField("std_prob",  FloatType(), True),  
        StructField("base_amount", FloatType(), True),
        StructField("combos_generated", IntegerType(), True),
        StructField("cost", FloatType(), True),
        StructField("payoff", FloatType(), True),
        StructField("net", FloatType(), True),
        StructField("hit_flag", IntegerType(), True),
        StructField("actual_winning_combo", StringType(), True),
        StructField("generated_combos", StringType(), True),
        StructField("roi", FloatType(), True),
        StructField("field_size", IntegerType(), True),
        StructField("race_base_amount", FloatType(), True),
    ])
    
    df_results = spark.createDataFrame(results, schema=schema)

    # compute profit, row_roi, etc.
    df_results = df_results.withColumn("bet_type", lit(f"{wager_type}_top{top_n}_box"))
    df_results = df_results.withColumn("profit", col("payoff") - col("cost"))
    df_results = df_results.withColumn(
        "row_roi",
        when(col("cost") > 0, col("profit") / col("cost")).otherwise(0.0)
    )

    # Summaries
    net = total_payoff - total_cost
    roi = (net / total_cost) if total_cost else 0.0

    print(f"--- Multi-Race Wager Summary ({wager_type}, {num_legs} legs) ---")
    print(f"  total_wagers: {total_wagers}, wins: {total_wins}, losses: {total_losses}")
    print(f"  bad_race_wager_info: {bad_race_wager_info}")
    print(f"  cost: ${total_cost:.2f}, payoff: ${total_payoff:.2f}, net: ${net:.2f}")
    print(f"  ROI: {roi:.2%}")

    return df_results