import logging
import time
import os
import pandas as pd
import src.wagering.wager_types as wt
from src.wagering.wagering_helper_functions import (gather_bet_metrics, group_races_for_pick3)
from src.wagering.wager_types import MultiRaceWager, ExactaWager
from src.wagering.wagering_helper_functions import parse_winners_str
from src.wagering.wagering_functions import find_race
import logging
import pandas as pd
from src.wagering.wagering_helper_functions import gather_bet_metrics

def implement_ExactaWager(all_races, wagers_dict, base_amount=2.0, top_n=2, box=True):
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
    my_wager = ExactaWager(base_amount=2.0, top_n=2, box=True)
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
    df_results = pd.DataFrame(bet_results)

    # Optionally, compute per-row profit/ROI if each row has cost & payoff
    if "cost" in df_results.columns and "payoff" in df_results.columns:
        df_results["profit"] = df_results["payoff"] - df_results["cost"]
        df_results["row_roi"] = df_results.apply(
            lambda row: (row["profit"] / row["cost"]) if row["cost"] > 0 else 0.0,
            axis=1
        )

    # Return the DataFrame with combos as arrays
    return df_results

def implement_multi_race_wager(all_races, wagers_dict, wager_type, num_legs, base_amount=2.0):
    import logging
    import pandas as pd

    results = []

    # Build the multi-leg wager
    my_wager = MultiRaceWager(
        base_amount=base_amount,
        num_legs=num_legs,
        top_n=2,    # or some user param
        box=True    # or some user param
    )

    # 1) Filter by 4th item in key => must match wager_type exactly
    wagers_subset = {
        k: v
        for k, v in wagers_dict.items()
        if k[3] == wager_type
    }
    logging.info(f"Filtered wagers_subset for '{wager_type}': {len(wagers_subset)} entries")

    # 2) For each
    for (course_cd, date, final_rn, wtype), wrow in wagers_subset.items():
        leg_winners = wrow["winning_combo"]  # e.g. [['1'],['1'],['5']]
        if len(leg_winners) < num_legs:
            continue

        # Build consecutive legs => e.g. final_rn=5 => [3,4,5] if num_legs=3
        leg_numbers = [final_rn - (num_legs - 1) + i for i in range(num_legs)]

        # Gather the Race objects
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

        # 3) Generate combos => my_wager
        all_combos = my_wager.generate_combos(race_objs)
        cost = my_wager.calculate_cost(all_combos)

        posted_payoff = float(wrow.get("payoff", 0.0))
        posted_base   = float(wrow.get("num_tickets", 2.0))

        # 4) check if we hit
        payoff = 0.0
        if len(leg_winners) == num_legs:
            for combo in all_combos:
                if my_wager.check_if_win(combo, race_objs, leg_winners):
                    payoff = posted_payoff*(my_wager.base_amount / posted_base)
                    break

        # 5) gather bet metrics => returns a fully sanitized dict
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

        # Additional fields => convert to strings or numeric
        # e.g. legs => string
        legs_str = '|'.join(str(x) for x in leg_numbers)
        row_data["legs_str"] = legs_str

        # We can store official winners as a string too
        row_data["official_winners_str"] = str(leg_winners)

        # Store param as string or numeric
        row_data["wager_type"] = str(wager_type)
        row_data["course_cd"]  = str(course_cd)
        row_data["race_date"]  = str(date)
        row_data["final_leg"]  = int(final_rn)

        results.append(row_data)

    df_results = pd.DataFrame(results)
    return df_results