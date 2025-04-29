import pandas as pd
import itertools
from tqdm import tqdm

def build_race_context(pred_df: pd.DataFrame) -> pd.DataFrame:
    # pred_df has one row per horse
    grp = pred_df.groupby("race_key")
    ctx = pd.DataFrame({
        "field_size":      grp["field_size"].first(),
        "distance_meters": grp["distance_meters"].first(),
        "race_number":     grp["race_number_int"].first(),
        "course_cd":       grp["course_cd"].first(),
        "track_name":      grp["track_name"].first(),
        "surface":         grp["surface"].first(),
        "track_condition": grp["track_condition"].first(),

        "fav_morn_odds":   grp["morn_odds"].min(),
        "avg_morn_odds":   grp["morn_odds"].mean(),

        "max_prob":        grp["score"].max(),
        "second_prob":     grp["score"]
                              .apply(lambda s: sorted(s, reverse=True)[1]
                                    if len(s)>1 else 0),
    })
    ctx["prob_gap"] = ctx["max_prob"] - ctx["second_prob"]
    ctx["std_prob"] = grp["score"].std().fillna(0)
    return ctx.reset_index()

import pandas as pd
import itertools
from collections import defaultdict

def load_historical_rewards(pred_df: pd.DataFrame,
                            wagers_df: pd.DataFrame,
                            top_n: int = 2
                           ) -> pd.DataFrame:
    """
    For each race_key in pred_df, compute the reward of placing a $1 Exacta
    on the top_n model picks, using your actual exotic_wagers table.
    Also include a “no_bet” arm with reward=0 for every race.
    """
    # 1) parse 'winners' into a tuple of cloth IDs
    wagers_df = wagers_df.copy()
    wagers_df["parsed_winners"] = (
        wagers_df["winners"]
          .astype(str)
          .str.split(",")
          .apply(lambda lst: tuple(w.strip() for w in lst))
    )

    # 2) build a dictionary of payoff info, keyed by (race_key, parsed_winners)
    payoff_dict = defaultdict(lambda: {"payoff": 0.0, "num_tickets": 0.0})
    exacta_wagers = wagers_df[wagers_df["wager_type"] == "Exacta"]
    for idx, row in tqdm(exacta_wagers.iterrows(), 
                     desc="Building payoff dictionary", 
                     total=len(exacta_wagers)):
        key = (row["race_key"], row["parsed_winners"])
        payoff_dict[key]["payoff"]     += float(row["payoff"])
        payoff_dict[key]["num_tickets"] += float(row["num_tickets"])

    records = []
    race_keys = pred_df["race_key"].unique()

    # 3) for each race, find top_n picks → build permutations → do a quick lookup in payoff_dict
    for race_key, grp in pred_df.groupby("race_key"):
        # sorted descending by model score
        picks = (grp
                 .sort_values("score", ascending=False)
                 .head(top_n)["saddle_cloth_number"]
                 .astype(str)
                 .tolist())

        # build permutations (e.g. two for top_n=2)
        for combo in itertools.permutations(picks, 2):
            combo_str = f"{combo[0]}->{combo[1]}" 
            key = (race_key, combo)
            payoff_info  = payoff_dict.get(key, {"payoff": 0.0, "num_tickets": 0.0})
            total_payoff = payoff_info["payoff"]
            total_cost   = payoff_info["num_tickets"]  # e.g. 1 ticket × $1 each
            reward       = total_payoff - total_cost

            records.append({
                "race_key": race_key,
                "arm":      combo_str,
                "reward":   reward
            })

    # 4) add a “no_bet” arm (reward = 0) for every race
    no_bet_records = [
        {"race_key": rk, "arm": "no_bet", "reward": 0.0}
        for rk in race_keys
    ]
    records.extend(no_bet_records)

    return pd.DataFrame(records)