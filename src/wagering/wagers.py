import logging
import time
import src.wagering.wager_types as wt
from src.wagering.wagering_helper_functions import (gather_bet_metrics, group_races_for_pick3)
from src.wagering.wager_types import Pick3Wager 
from src.wagering.wagering_helper_functions import parse_winners_str
def implement_ExactaWager(all_races, wagers_dict, base_amount=2.0, top_n=2, box=True):
    my_wager = wt.ExactaWager(base_amount=base_amount, top_n=top_n, box=box)

    total_cost = 0.0
    total_payoff = 0.0
    bet_results = []  # will hold dicts for each race

    # Counters for wagers, wins, losses
    total_wagers = 0
    total_wins = 0
    total_losses = 0

    # Filter races by field size
    filtered_races = []
    for race in all_races:
        field_size = len(race.horses)
        if 4 <= field_size <= 14:
            filtered_races.append(race)

    for race in filtered_races:
        combos = my_wager.generate_combos(race)       # list of combos (tickets)
        cost = my_wager.calculate_cost(combos)        # total $ cost for these combos

        key = (race.course_cd, race.race_date, race.race_number, "Exacta")
        race_wager_info = wagers_dict.get(key, None)

        payoff = 0.0
        actual_combo = None
        winning_combo_found = False

        if race_wager_info:
            actual_combo = race_wager_info['winning_combo']  # e.g., [["7"], ["6"]]
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
        num_tickets = len(combos)  # how many exacta combos we bet on in this race
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
        bet_results.append(row_data)

    net = total_payoff - total_cost
    roi = (net / total_cost) if total_cost > 0 else 0.0

    print(f"Exacta Box (Top {top_n}) => "
          f"Cost: ${total_cost:.2f}, Return: ${total_payoff:.2f}, "
          f"Net: ${net:.2f}, ROI: {roi:.2%}")
    print(f"Total Wagers: {total_wagers}, Wins: {total_wins}, Losses: {total_losses}")

    return bet_results

def analyze_pick3_wagers(races_pdf, wagers_pdf, base_amount=2.0, parquet_path=None):
    """
    1) Build a dictionary of top-2 picks for each race (or top-N).
    2) For each 'Pick 3' in wagers_pdf, parse winners, compute combos cost, check if hit.
    3) Accumulate total cost/payoff, compute net, ROI at the end.
    4) Optionally save results to Parquet if parquet_path is provided.
    5) Keep track of total wagers, total wins, and total losses.
    """
    import logging
    import pandas as pd
    from decimal import Decimal

    # ---- STEP A: BUILD PREDICTED DICT (top-2 picks) ----
    predicted_dict = {}
    group_cols = ["course_cd", "race_date", "race_number"]
    sorted_df = races_pdf.sort_values(by="rank", ascending=True)

    for (cc, dt, rn), grp in sorted_df.groupby(group_cols):
        # Take top 2 picks (or fewer if not enough data)
        top2 = grp.head(2)
        picks = top2["saddle_cloth_number"].str.strip().str.upper().tolist()
        predicted_dict[(cc, dt, rn)] = picks  # e.g. ["3","5"]

    # ---- STEP B: FILTER 'Pick 3' ROWS ----
    pick3_rows = wagers_pdf[wagers_pdf["wager_type"] == "Pick 3"]

    # We'll collect row-level info in a list
    results = []

    # Summaries
    total_cost = 0.0
    total_payoff = 0.0

    # 1) New counters
    total_wagers = 0     # total number of pick-3 combo tickets
    total_wins = 0       # total number of hits (winning tickets)
    total_losses = 0     # total number of losing tickets

    for _, wrow in pick3_rows.iterrows():
        cc = wrow["course_cd"]
        dt = wrow["race_date"]
        final_rn = wrow["race_number"]

        # parse winners => e.g. "7-3-4"
        winners_str = wrow.get("winners") or ""
        leg_winners = winners_str.split("-")
        if len(leg_winners) < 3:
            logging.info(f"Skipping pick3 row with incomplete winners: '{winners_str}'")
            continue

        # triple of consecutive races
        first_leg = final_rn - 2
        second_leg = final_rn - 1
        third_leg = final_rn

        # predicted picks for each leg
        pred1_array = predicted_dict.get((cc, dt, first_leg), [])
        pred2_array = predicted_dict.get((cc, dt, second_leg), [])
        pred3_array = predicted_dict.get((cc, dt, third_leg), [])

        # cost = (# of combos) * base_amount
        num_combos = len(pred1_array) * len(pred2_array) * len(pred3_array)
        cost = num_combos * float(base_amount)

        # actual winners for each leg
        actual1 = leg_winners[0].strip().upper()
        actual2 = leg_winners[1].strip().upper()
        actual3 = leg_winners[2].strip().upper()

        # check coverage
        leg1_hit = (actual1 in pred1_array)
        leg2_hit = (actual2 in pred2_array)
        leg3_hit = (actual3 in pred3_array)
        hit = (leg1_hit and leg2_hit and leg3_hit)

        # payoff scaling
        posted_payoff_dec = wrow.get("payoff", 0.0)
        posted_base_dec = wrow.get("num_tickets", 2.0)

        posted_payoff = float(posted_payoff_dec) if isinstance(posted_payoff_dec, (Decimal, float)) else float(posted_payoff_dec)
        posted_base = float(posted_base_dec) if isinstance(posted_base_dec, (Decimal, float)) else float(posted_base_dec)

        payoff = 0.0
        if hit and num_combos > 0:
            # scale posted payoff by (base_amount / posted_base)
            payoff = posted_payoff * (float(base_amount) / posted_base)

        # Accumulate cost/payoff
        total_cost += cost
        total_payoff += payoff

        # 2) Update counters for wagers, wins, and losses
        # Each combo is one "ticket"
        total_wagers += num_combos
        if hit and num_combos > 0:
            # exactly 1 winning combo, rest are losses
            total_wins += 1
            total_losses += (num_combos - 1)
        else:
            total_losses += num_combos

        # Logging
        logging.info(
            f"\nPick3 final_leg={final_rn} => triple=({first_leg},{second_leg},{third_leg}),"
            f" winners={leg_winners}, predicted=({pred1_array},{pred2_array},{pred3_array}),"
            f" combos={num_combos}, cost={cost:.2f}, hit={hit}, payoff={payoff:.2f}"
        )

        row_data = {
            "course_cd": cc,
            "race_date": dt,
            "final_leg": final_rn,
            "triple": (first_leg, second_leg, third_leg),
            "actual_combo": leg_winners,
            "predicted": [pred1_array, pred2_array, pred3_array],
            "num_combos": num_combos,
            "cost": cost,
            "hit": hit,
            "payoff": payoff,
            "wager_payoff_posted": posted_payoff,
            "wager_posted_base": posted_base
        }
        results.append(row_data)

    # ---- Summaries & ROI ----
    net = total_payoff - total_cost
    roi = (net / total_cost) if total_cost > 0 else 0.0

    logging.info(f"\nSUMMARY: total pick3 cost={total_cost:.2f}, payoff={total_payoff:.2f}, net={net:.2f}, ROI={roi:.2%}")
    logging.info(f"Total Wagers: {total_wagers}, Wins: {total_wins}, Losses: {total_losses}")
    print(f"Pick 3 => Cost: {total_cost:.2f}, Return: {total_payoff:.2f}, Net: {net:.2f}, ROI: {roi:.2%}")
    print(f"Wagers: {total_wagers}, Wins: {total_wins}, Losses: {total_losses}")

    # ---- Optionally save to Parquet
    if parquet_path:
        df = pd.DataFrame(results)
        df.to_parquet(parquet_path, index=False)
        logging.info(f"Saved {len(df)} pick3 results to {parquet_path}")

    return results