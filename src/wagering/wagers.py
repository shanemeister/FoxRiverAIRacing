import logging
import time
import os
import pandas as pd
import src.wagering.wager_types as wt
from src.wagering.wagering_helper_functions import (gather_bet_metrics, group_races_for_pick3)
from src.wagering.wager_types import MultiRaceWager, ExactaWager, TrifectaWager, SuperfectaWager
from src.wagering.wagering_helper_functions import parse_winners_str
from src.wagering.wagering_functions import find_race
import logging
import pandas as pd
from src.wagering.wagering_helper_functions import gather_bet_metrics
from pyspark.sql.functions import col, sum as F_sum, when, count as F_count, lit

def implement_ExactaWager(spark, all_races, wagers_dict, base_amount, top_n, box):
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
    my_wager = ExactaWager(base_amount, top_n, box)
    # Filter races by field size
    filtered_races = []
    for race in all_races:
        field_size = len(race.horses)
        if 4 <= field_size <= 14:
            filtered_races.append(race)

    for race in filtered_races:
        # Generate combos (list of lists), e.g. [["3","7"],["2","5"], ...]
        combos = my_wager.generate_combos(race)
        cost = my_wager.calculate_cost(combos)   # total $ cost for these combos

        key = (race.course_cd, race.race_date, race.race_number, "Exacta")
        race_wager_info = wagers_dict.get(key, None)

        payoff = 0.0
        actual_combo = None
        winning_combo_found = False

        if race_wager_info:
            actual_combo = race_wager_info['winning_combo']  # e.g. [["7"], ["6"]]
            race_payoff = float(race_wager_info['payoff'])
            posted_base = float(race_wager_info['num_tickets'])
            logging.info(f"\n--- Debugging Race ---")
            logging.info(f"Race Key: {key}")
            logging.info(f"Generated Combos: {combos}")
            logging.info(f"Posted Base: {posted_base:.2f}")
            logging.info(f"Cost for Combos: {cost:.2f}")
            logging.info(f"Actual Winning Combo: {actual_combo}")
            logging.info(f"Listed Payoff: {race_payoff:.2f}")

            # Check each combo to see if it matches the actual winning combo
            for combo in combos:
                if my_wager.check_if_win(combo, race, actual_combo):
                    payoff = my_wager.calculate_payoff(race_payoff, posted_base)
                    logging.info(f"=> Match Found! Winning Combo: {combo}, Payoff = {payoff:.2f}")
                    winning_combo_found = True
                    break
        else:
            logging.debug(f"No wager info found for {key}")

        total_cost += cost
        total_payoff += payoff

        # Count wagers, wins, losses
        num_tickets = len(combos)  # how many exacta combos we bet in this race
        total_wagers += num_tickets

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
            field_size=field_size
        )

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
    import json

    # Preprocess bet_results to flatten nested structures
    for row in bet_results:
        if "actual_winning_combo" in row:
            row["actual_winning_combo"] = json.dumps(row["actual_winning_combo"])  # Convert to JSON string
        if "generated_combos" in row:
            row["generated_combos"] = json.dumps(row["generated_combos"])  # Convert to JSON string
        df_results = pd.DataFrame(bet_results)
        
    from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

    schema = StructType([
        StructField("course_cd", StringType(), True),
        StructField("race_date", StringType(), True),
        StructField("race_number", IntegerType(), True),
        StructField("surface", StringType(), True),
        StructField("distance_meters", FloatType(), True),
        StructField("track_condition", StringType(), True),
        StructField("avg_purse_val_calc", FloatType(), True),
        StructField("race_type", StringType(), True),
        StructField("base_amount", FloatType(), True),
        StructField("combos_generated", IntegerType(), True),
        StructField("cost", FloatType(), True),
        StructField("payoff", FloatType(), True),
        StructField("net", FloatType(), True),
        StructField("hit_flag", IntegerType(), True),
        StructField("actual_winning_combo", StringType(), True),  # Store as JSON string
        StructField("generated_combos", StringType(), True),      # Store as JSON string
        StructField("roi", FloatType(), True),
        StructField("field_size", IntegerType(), True)
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
    # Return the DataFrame with combos as arrays
    return df_results

def implement_TrifectaWager(
            spark,
            all_races,
            wagers_dict,
            base_amount,
            top_n, 
            box
        ):
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
    bet_results = []  # will hold a dictionary for each race's wager metrics

    # Counters for wagers, wins, losses
    total_wagers = 0
    total_wins = 0
    total_losses = 0
    
    # Instantiate the trifecta wager
    my_wager = TrifectaWager(base_amount=base_amount, top_n=top_n, box=box)
    
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
            race_payoff = float(race_wager_info.get('payoff', 0))
            posted_base = float(race_wager_info.get('num_tickets'))
            logging.info(f"\n--- Debugging Race ---")
            logging.info(f"Race Key: {key}")
            logging.info(f"Generated Combos: {combos}")
            logging.info(f"Posted Base: {posted_base:.2f}")
            logging.info(f"Cost for Combos: {cost:.2f}")
            logging.info(f"Actual Winning Combo: {actual_combo}")
            logging.info(f"Listed Payoff: {race_payoff:.2f}")

            # Check if any generated combo matches the actual winning combo
            for combo in combos:
                if my_wager.check_if_win(combo, race, actual_combo):
                    payoff = my_wager.calculate_payoff(race_payoff, posted_base)
                    logging.info(f"=> Match Found! Winning Combo: {combo}, Payoff = {payoff:.2f}")
                    winning_combo_found = True
                    break
        else:
            logging.debug(f"No wager info found for {key}")

        total_cost += cost
        total_payoff += payoff

        num_tickets = len(combos)
        total_wagers += num_tickets
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
            field_size=field_size
        )
        
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
    from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
    schema = StructType([
        StructField("course_cd", StringType(), True),
        StructField("race_date", StringType(), True),
        StructField("race_number", IntegerType(), True),
        StructField("surface", StringType(), True),
        StructField("distance_meters", FloatType(), True),
        StructField("track_condition", StringType(), True),
        StructField("avg_purse_val_calc", FloatType(), True),
        StructField("race_type", StringType(), True),
        StructField("base_amount", FloatType(), True),
        StructField("combos_generated", IntegerType(), True),
        StructField("cost", FloatType(), True),
        StructField("payoff", FloatType(), True),
        StructField("net", FloatType(), True),
        StructField("hit_flag", IntegerType(), True),
        StructField("actual_winning_combo", StringType(), True),  # JSON string
        StructField("generated_combos", StringType(), True),      # JSON string
        StructField("roi", FloatType(), True),
        StructField("field_size", IntegerType(), True)
    ])

    df_results = spark.createDataFrame(bet_results, schema=schema)
    
    df_results = df_results.withColumn("bet_type", lit(f"trifecta_top{top_n}_box"))
    df_results = df_results.withColumn("profit", col("payoff") - col("cost"))
    df_results = df_results.withColumn(
        "row_roi",
        when(col("cost") > 0, col("profit") / col("cost")).otherwise(0.0)
    )

    return df_results

def implement_SuperfectaWager(
            spark,
            all_races,
            wagers_dict,
            base_amount,
            top_n, 
            box
        ):
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
    my_wager = SuperfectaWager(base_amount=base_amount, top_n=top_n, box=box)
    
    # Filter races by field size (for superfecta, ensure at least 4 horses)
    filtered_races = []
    for race in all_races:
        field_size = len(race.horses)
        if 4 <= field_size <= 14:
            filtered_races.append(race)

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
            actual_combo = race_wager_info['winning_combo']  # e.g. [["1"], ["2"], ["3"], ["4"]]
            race_payoff = float(race_wager_info['payoff'])
            posted_base = float(race_wager_info['num_tickets'])
            logging.info(f"\n--- Debugging Race ---")
            logging.info(f"Race Key: {key}")
            logging.info(f"Generated Combos: {combos}")
            logging.info(f"Posted Base: {posted_base:.2f}")
            logging.info(f"Cost for Combos: {cost:.2f}")
            logging.info(f"Actual Winning Combo: {actual_combo}")
            logging.info(f"Listed Payoff: {race_payoff:.2f}")

            # Check each combo to see if it matches the actual winning combo
            for combo in combos:
                if my_wager.check_if_win(combo, race, actual_combo):
                    payoff = my_wager.calculate_payoff(race_payoff, posted_base)
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
            field_size=field_size
        )

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
    import json
    for row in bet_results:
        if "actual_winning_combo" in row:
            row["actual_winning_combo"] = json.dumps(row["actual_winning_combo"])  # Convert to JSON string
        if "generated_combos" in row:
            row["generated_combos"] = json.dumps(row["generated_combos"])  # Convert to JSON string
    df_results = pd.DataFrame(bet_results)

    from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

    schema = StructType([
        StructField("course_cd", StringType(), True),
        StructField("race_date", StringType(), True),
        StructField("race_number", IntegerType(), True),
        StructField("surface", StringType(), True),
        StructField("distance_meters", FloatType(), True),
        StructField("track_condition", StringType(), True),
        StructField("avg_purse_val_calc", FloatType(), True),
        StructField("race_type", StringType(), True),
        StructField("base_amount", FloatType(), True),
        StructField("combos_generated", IntegerType(), True),
        StructField("cost", FloatType(), True),
        StructField("payoff", FloatType(), True),
        StructField("net", FloatType(), True),
        StructField("hit_flag", IntegerType(), True),
        StructField("actual_winning_combo", StringType(), True),  # Store as JSON string
        StructField("generated_combos", StringType(), True),      # Store as JSON string
        StructField("roi", FloatType(), True),
        StructField("field_size", IntegerType(), True)
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
            base_amount,
            top_n,
            box
        ):

    results = []

    # Build the multi-leg wager
    my_wager = MultiRaceWager(
        base_amount=base_amount,
        num_legs=num_legs,
        top_n=top_n,
        box=box
    )

    # 1) Filter by the 4th item in key => must match wager_type exactly
    wagers_subset = {
        k: v
        for k, v in wagers_dict.items()
        if k[3] == wager_type
    }
    logging.info(f"Filtered wagers_subset for '{wager_type}': {len(wagers_subset)} entries")

    # 2) Process each wager from the subset
    for (course_cd, date, final_rn, wtype), wrow in wagers_subset.items():
        # Safely retrieve winning_combo; if missing or its length is insufficient, skip this entry.
        leg_winners = wrow.get("winning_combo")
        if not leg_winners or len(leg_winners) < num_legs:
            continue

        # Convert final_rn to an integer safely.
        if final_rn is None:
            logging.debug(f"Skipping key ({course_cd}, {date}, {final_rn}, {wtype}) because final_rn is None")
            continue
        try:
            final_rn_int = int(final_rn)
        except Exception as e:
            logging.debug(f"Cannot convert final_rn ({final_rn}) to int for key ({course_cd}, {date}, {final_rn}, {wtype}): {e}")
            continue

        # Build consecutive legs => e.g. if final_rn_int=5 and num_legs=3 then leg_numbers = [3, 4, 5]
        leg_numbers = [final_rn_int - (num_legs - 1) + i for i in range(num_legs)]

        # Gather the Race objects for each leg
        race_objs = []
        missing_leg = False
        for leg_num in leg_numbers:
            r_obj = find_race(all_races, course_cd, date, leg_num)
            if r_obj is None:
                missing_leg = True
                break
            race_objs.append(r_obj)
        if missing_leg or len(race_objs) < num_legs:
            continue

        # 3) Generate combos using my_wager and calculate cost
        all_combos = my_wager.generate_combos(race_objs)
        cost = my_wager.calculate_cost(all_combos)
        
        posted_payoff = float(wrow.get("payoff", 0.0))
        posted_base   = float(wrow.get("num_tickets"))

        # 4) Check if any generated combo matches the winning combo
        payoff = 0.0
        if len(leg_winners) == num_legs:
            for combo in all_combos:
                if my_wager.check_if_win(combo, race_objs, leg_winners):
                    payoff = posted_payoff * (my_wager.base_amount / posted_base)
                    break

        logging.info("\n--- Multi-Race Wager Debug ---")
        logging.info(f"Race Key: ({course_cd}, {date}, Final Leg #{final_rn})")
        logging.info(f"Legs: {leg_numbers}")
        logging.info("Top Picks per Leg:")
        for i, race in enumerate(race_objs):
            top_picks = my_wager.get_top_n_for_leg(i, race)
            logging.info(f"  [Leg {i+1}] top picks => {top_picks}")

        logging.info(f"Generated {len(all_combos)} combos => {all_combos}")
        logging.info(f"Actual Winning Combo: {leg_winners}")
        logging.info(f"Posted Base: ${posted_base:.2f}")
        logging.info(f"Listed Payoff: {posted_payoff:.2f}")
        logging.info(f"Cost for Combo Set: ${cost:.2f}")
        logging.info(f"Payoff: ${payoff:.2f}")

        if payoff > 0:
            logging.info("=> Match Found! ✅\n")
        else:
            logging.info("=> No Match ❌\n")
            
        # 5) Gather bet metrics into a sanitized dict
        final_race = race_objs[-1]
        row_data = gather_bet_metrics(
            race=final_race,
            combos=all_combos,
            cost=cost,
            payoff=payoff,
            my_wager=my_wager,
            actual_combo=leg_winners,
            field_size=len(final_race.horses)
        )

        # Additional fields (convert as needed)
        legs_str = '|'.join(str(x) for x in leg_numbers)
        row_data["legs_str"] = legs_str
        row_data["official_winners_str"] = str(leg_winners)
        row_data["wager_type"] = str(wager_type)
        row_data["course_cd"] = str(course_cd)
        row_data["race_date"] = str(date)
        row_data["final_leg"] = int(final_rn)

        results.append(row_data)
    
    from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

    schema = StructType([
        StructField("course_cd", StringType(), True),
        StructField("race_date", StringType(), True),
        StructField("race_number", IntegerType(), True),
        StructField("surface", StringType(), True),
        StructField("distance_meters", FloatType(), True),
        StructField("track_condition", StringType(), True),
        StructField("avg_purse_val_calc", FloatType(), True),
        StructField("race_type", StringType(), True),
        StructField("base_amount", FloatType(), True),
        StructField("combos_generated", IntegerType(), True),
        StructField("cost", FloatType(), True),
        StructField("payoff", FloatType(), True),
        StructField("net", FloatType(), True),
        StructField("hit_flag", IntegerType(), True),
        StructField("actual_winning_combo", StringType(), True),
        StructField("generated_combos", StringType(), True),
        StructField("roi", FloatType(), True),
        StructField("field_size", IntegerType(), True)
    ])
    
    df_results = spark.createDataFrame(results, schema=schema)

    df_results = df_results.withColumn("bet_type", lit(f"{wager_type}_top{top_n}_box"))
    df_results = df_results.withColumn("profit", col("payoff") - col("cost"))
    df_results = df_results.withColumn(
        "row_roi",
        when(col("cost") > 0, col("profit") / col("cost")).otherwise(0.0)
    )
    # Aggregate totals
    agg = df_results.agg(
    F_sum("cost").alias("total_cost"),
    F_sum("payoff").alias("total_payoff"),
    F_sum("hit_flag").alias("total_hits"),
    F_sum("combos_generated").alias("sum_combos"),
    F_count("*").alias("num_final_leg_rows")
    ).collect()[0]

    total_cost = agg["total_cost"] or 0.0
    total_payoff = agg["total_payoff"] or 0.0
    total_hits = agg["total_hits"] or 0
    sum_combos = agg["sum_combos"] or 0
    num_final_leg_rows = agg["num_final_leg_rows"] or 0

    net = total_payoff - total_cost
    roi = (net / total_cost) if total_cost > 0 else 0.0
    misses = total_hits - num_final_leg_rows  # or however you track that

    print(f"Multi-Race Wager ({wager_type}, {num_legs} legs) =>")
    print(f"  Total final-leg rows: {num_final_leg_rows}")
    print(f"  Sum of combos_generated: {sum_combos}")
    print(f"  Total cost: ${total_cost:.2f}, payoff: ${total_payoff:.2f}, ROI: {roi:.2%}")
    print(f"Net: ${net:.2f}, ROI: {roi:.2%}")

    return df_results
