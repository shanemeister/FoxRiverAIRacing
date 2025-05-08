import logging
import time
import os
import json
import pandas as pd
import src.wagering.wager_types as wt
from src.wagering.wager_types import MultiRaceWager, ExactaWager, TrifectaWager, SuperfectaWager, ExactaStrategy, WagerStrategy
from src.wagering.wagering_helper_functions import parse_winners_str
from src.wagering.wagering_functions import find_race, get_box_close3, backtest_strategies, compute_or_load_green_tracks
import logging
import pandas as pd
from src.wagering.wagering_helper_functions import gather_bet_metrics
from pyspark.sql.functions import col, sum as F_sum, when, count as F_count, lit
from pyspark.sql.types import (StructType, StructField, StringType, FloatType, IntegerType)
import pyspark.sql.functions as F

def implement_ExactaWager(spark, all_races, wagers_dict, wager_amount, top_n, box):
    """
    Generates a list of bets (combos) for each race, calculates the cost/payoff,
    and returns a DataFrame with row-level metrics per race/bet scenario.

    Differences from your original version:
      1) We store 'actual_winning_combo' and 'generated_combos' as arrays/lists
         instead of strings, matching the style of pick3 code.
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
    filtered_races = [race for race in all_races if 4 <= len(race.horses) <= 20]
    # Set base_amount for this race
    # strategy_gap_box = ExactaStrategy(
    #     name="gap>0.10_box_close3",
    #     should_bet=has_confidence,
    #     build_combos=get_box_close3,
    #     base_amount=1.0
    #     )
    #GREEN_TRACKS = compute_or_load_green_tracks(spark)
    # ------------------------------------------------------------------
    # 0. TEMP: hard-code green tracks for the first run
    # ------------------------------------------------------------------
    # GREEN_TRACKS = {"TTP", "TSA", "TCD"}
    # GREEN_TRACKS = compute_or_load_green_tracks(
    #     spark,
    #     lookback_days=365,    # try rolling month
    #     min_bets=5,         # lower sample guard
    #     min_roi=0.60         # raise hurdle to 60 %
    # )
    GREEN_TRACKS = compute_or_load_green_tracks(spark)
    # fall-back for the *very first* run
    # if not GREEN_TRACKS:
    #     logging.info("No green_tracks.yaml yet – falling back to hard-coded set")
    #     GREEN_TRACKS = {"TTP", "TSA", "TCD"}
        
    # build strategy object
    strat_x_gap_box = WagerStrategy(
        name        = "X_gap>0.10_boxClose3",
        wager_type  = "Exacta",
        should_bet  = lambda r, g=GREEN_TRACKS: r.prob_gap > 0.10 and r.course_cd in g,
        build_combos= get_box_close3,
        base_amount = 1.0
    )
    strategies      = [strat_x_gap_box]
    active_strategy = strat_x_gap_box    # live strategy

    # results = backtest_strategies(all_races, wagers_dict, strategies)

    # logging.info(f"{'Strategy':35}  Bets  Cost    Payoff   ROI")
    # for name,bets,cost,pay,roi in results:
    #     logging.info(f"{name:35}  {bets:>4}  ${cost:>7.2f}  ${pay:>8.2f}  {roi:>6.2%}")    
    
    active_strategy = strat_x_gap_box
    
    for race in filtered_races:
        key = (race.course_cd, race.race_date, race.race_number, "Exacta")
        race_wager_info = wagers_dict.get(key, None)
        num_tickets = race_wager_info["num_tickets"] if race_wager_info and "num_tickets" in race_wager_info else None
            
        if race_wager_info and "num_tickets" in race_wager_info:
            try:
                num_tickets_val = float(num_tickets)
                if num_tickets_val is None or num_tickets_val == 0.0:
                    print(f"Debug 3: num_tickets is None or zero for {key}, BAD DATA")
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
            
        if not active_strategy.should_bet(race):
            continue                                         # skip this race

        combos = active_strategy.build_combos(race)
        my_wager = ExactaWager(active_strategy.base_amount, 0, True)  # “dummy” – used only for cost/payoff helpers
        cost = my_wager.calculate_cost(combos)   # total $ cost for these combos
        
        payoff = 0.0
        actual_combo = None
        winning_combo_found = False

        if race_wager_info:
            actual_combo = race_wager_info['winning_combo']  # e.g. [["7"], ["6"]]
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

            race_payoff = float(race_wager_info['payoff'])
            posted_base = float(race_wager_info['num_tickets'])
            logging.info(f"\n--- Debugging Race ---")
            logging.info(f"Race Key: {key}")
            logging.info(f"Wager Amount: {wager_amount}")
            logging.info(f"Generated Combos: {combos}")
            logging.info(f"Posted Base: {posted_base:.2f}")
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
            bad_race_wager_info += 1

        total_cost += cost
        total_payoff += payoff

        # Count wagers, wins, losses
        num_tickets = len(combos)  # how many exacta combos we bet in this race
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
        
        row_data["strategy"]      = active_strategy.name
        row_data["tickets_played"]= len(combos)
        row_data["ticket_cost"]   = cost  

        # === Add the combos as arrays (instead of storing them as strings) ===
        row_data["actual_winning_combo"] = actual_combo      # list of lists, e.g. [["7"], ["6"]]
        row_data["generated_combos"] = combos                # list of lists, e.g. [["3","7"],["2","5"],...]

        bet_results.append(row_data)

    net = total_payoff - total_cost
    roi = (net / total_cost) if total_cost > 0 else 0.0

    print(f"Exacta Box (Top {top_n}) => "
          f"Cost: ${total_cost:.2f}, Return: ${total_payoff:.2f}, "
          f"Net: ${net:.2f}, ROI: {roi:.2%}")
    print(f"Total Wagers: {total_wagers}, Wins: {total_wins}, Losses: {total_losses}")

    # ------------------------------------------------------------------------
    # Convert bet_results (list of dicts) into a Pandas DataFrame
    # ------------------------------------------------------------------------

    # Preprocess bet_results to flatten nested structures
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
        StructField("winner_score"  , FloatType(), True),
        StructField("runnerup_score", FloatType(), True),
        StructField("winner_rank"   , FloatType(), True),
        StructField("runnerup_rank" , FloatType(), True),
        StructField("gap12"         , FloatType(), True),
        StructField("gap23"         , FloatType(), True),
    ])

    # Convert bet_results into a Spark DataFrame with the defined schema
    df_results = spark.createDataFrame(bet_results, schema=schema)
    
    # Optionally, compute per-row profit/ROI if each row has cost & payoff
    df_results = df_results.withColumn("bet_type", lit(f"exacta_top{top_n}_box"))
    df_results = df_results.withColumn("profit", col("payoff") - col("cost"))

    df_results = df_results.withColumn(
        "row_roi",
        when(col("cost") > 0, col("profit") / col("cost")).otherwise(0.0)
    )
    
    # ------------- WRITE HISTORY ONCE / APPEND AFTERWARDS --------------
    HIST_PARQUET = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/track_roi/exacta_history"
    df_results.write.mode("append").parquet(HIST_PARQUET)
    # -------------------------------------------------------------------

    
    # Return the DataFrame with combos as arrays
    return df_results

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