import src.wagering.wagering_classes as wc
import pandas as pd
from typing import Dict, Any, Tuple, List 
from src.wagering.wager_types import ExactaWager, ExactaStrategy
from datetime import date, timedelta
from pyspark.sql import SparkSession, functions as F
import yaml, pathlib
import logging
# roi_utils.py  (or just paste it where the old helper was)
import yaml, pathlib, logging
from datetime import date, timedelta
from pyspark.sql import functions as F, DataFrame
from typing import Set

# --------------------------------------------------------------------
BASE_DIR      = pathlib.Path(
    "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/track_roi"
)
HIST_PARQUET  = BASE_DIR / "exacta_history"
ROI_YAML      = BASE_DIR / "green_tracks.yaml"

# sensible defaults for weekly refresh
DEF_LOOKBACK_DAYS = 365
DEF_MIN_BETS      = 5
DEF_MIN_ROI       = 0.20
# --------------------------------------------------------------------

def _load_yaml() -> Set[str]:
    if not ROI_YAML.exists():
        return set()
    data = yaml.safe_load(ROI_YAML.read_text()) or {}
    return set(data.get("green_tracks", []))

def _write_yaml(track_set: Set[str]) -> None:
    ROI_YAML.parent.mkdir(parents=True, exist_ok=True)
    with ROI_YAML.open("w") as f:
        yaml.safe_dump({"green_tracks": sorted(track_set)}, f)

def compute_or_load_green_tracks(
        spark,
        lookback_days: int = DEF_LOOKBACK_DAYS,
        min_bets: int      = DEF_MIN_BETS,
        min_roi: float     = DEF_MIN_ROI,
) -> Set[str]:
    """
    Return the current 'green' track set.
    • If YAML exists and is non-empty → just load & return.
    • Otherwise compute from the history parquet, write YAML, return set.
    • If parquet missing, log warning and return empty set.
    """

    # 1️⃣ fast path – YAML already populated
    tracks = _load_yaml()
    if tracks:
        return tracks          # ← finished

    if not HIST_PARQUET.exists():
        logging.warning(
            f"[ROI REFRESH] Parquet history not found at {HIST_PARQUET}. "
            "No green tracks this run.")
        return set()

    # 2️⃣ read history & compute
    df: DataFrame = spark.read.parquet(str(HIST_PARQUET))

    # if race_date is string, Spark can compare ISO strings safely
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()

    stats = (df.filter(F.col("race_date") >= cutoff)
               .groupBy("course_cd")
               .agg(
                   F.count("*").alias("bets"),
                   ((F.sum("payoff") - F.sum("cost")) /
                    F.sum("cost")).alias("roi"))
               .filter(
                   (F.col("bets") >= min_bets) &
                   (F.col("roi")  >  min_roi))
             )

    tracks = {row["course_cd"] for row in stats.collect()}
    _write_yaml(tracks)

    logging.info(
        f"[ROI REFRESH] Computed green tracks (lookback {lookback_days}d): "
        f"{sorted(tracks) if tracks else 'NONE'}")
    return tracks
        
def get_box_close3(r: wc.Race) -> List[Tuple[str,str]]:
    horses = r.get_sorted_by_prediction()

    # core rule: scores within 0.05 of leader, cap 4
    box_pool = [h for h in horses[:4]
                if h.prediction >= r.max_prob - 0.05]

    # Fallback: if we only captured the leader, add next-best horse
    if len(box_pool) == 1 and len(horses) > 1:
        box_pool.append(horses[1])

    # build exacta perms
    combos = [(a.program_num, b.program_num)
              for a in box_pool for b in box_pool if a != b]
    return combos

def backtest_strategies(
        races: List[wc.Race],
        wagers_dict: dict,
        strategies: List[ExactaStrategy]
):
    for race in races:
        wkey = (race.course_cd, race.race_date, race.race_number, "Exacta")
        actual = wagers_dict.get(wkey)

        for strat in strategies:
            if not strat.should_bet(race):
                continue

            combos = strat.build_combos(race)
            bet_cost = len(combos) * strat.base_amount
            strat.bets   += 1
            strat.cost   += bet_cost

            # check win
            payoff = 0.0
            if actual and len(actual["winning_combo"]) >= 2:
                posted_payoff = float(actual["payoff"])
                posted_base   = float(actual["num_tickets"])
                for c in combos:
                    if ExactaWager(1,0,False).check_if_win(
                            c, race, actual["winning_combo"]):
                        payoff = (strat.base_amount / posted_base) * posted_payoff
                        break
            strat.payoff += payoff

    # final report
    rows = []
    for s in strategies:
        roi = (s.payoff - s.cost) / s.cost if s.cost else 0.0
        rows.append((s.name, s.bets, s.cost, s.payoff, roi))
    return rows

def build_race_objects(races_pdf: pd.DataFrame) -> List[wc.Race]:
    """
    Groups the DataFrame by (course_cd, race_date, race_number),
    then for each group:
      - Compute aggregated race-level features like fav_morn_odds, max_prob, etc.
      - Create a Race object (with distance, surface, etc. from the group's first row),
      - Create multiple HorseEntry objects (one per row in the group).

    Returns a list of Race objects.
    """

    race_list = []
    group_cols = ["course_cd", "race_date", "race_number"]

    for (course_cd, race_date, race_number), group_df in races_pdf.groupby(group_cols):
        # Pull "race-level" attributes from the first row
        first_row = group_df.iloc[0]

        distance_meters  = first_row.get("distance_meters")
        surface          = first_row.get("surface")
        track_condition  = first_row.get("track_condition")  # e.g. "Fast", "Good"
        avg_purse_val    = first_row.get("avg_purse_val_calc")
        race_type        = first_row.get("race_type")
        # 'rank' is often horse-level, but if you want it at race-level, it's up to you
        # rank             = first_row.get("rank", None)

        # ---- Compute Race-Level Aggregates from group_df ----

        # 1) fav_morn_odds & avg_morn_odds
        if "morn_odds" in group_df.columns and group_df["morn_odds"].notna().any():
            fav_morn_odds = group_df["morn_odds"].min()
            avg_morn_odds = group_df["morn_odds"].mean()
        else:
            fav_morn_odds = None
            avg_morn_odds = None

        # 2) max_prob, second_prob, prob_gap, std_prob from 'score'
        #    (assuming 'score' is your calibrated_prob for each horse)
        if "score" in group_df.columns:
            sorted_grp = group_df.sort_values("score", ascending=False)
            if len(sorted_grp) > 0:
                top1 = sorted_grp.iloc[0]["score"]
            else:
                top1 = 0.0
            if len(sorted_grp) > 1:
                top2 = sorted_grp.iloc[1]["score"]
            else:
                top2 = 0.0

            max_prob    = float(top1)
            second_prob = float(top2)
            prob_gap    = float(top1 - top2)

            # e.g. std of the top 4 horses
            top_scores = sorted_grp["score"].head(4)
            std_prob = float(top_scores.std(ddof=0))  # population std
        else:
            max_prob    = 0.0
            second_prob = 0.0
            prob_gap    = 0.0
            std_prob    = 0.0

        # Build the list of HorseEntry objects
        horses = []
        for _, row in group_df.iterrows():
            entry = wc.HorseEntry(
                horse_id    = str(row.get("horse_id", "")),
                program_num = str(row["saddle_cloth_number"]),
                official_fin= row.get("official_fin"),
                prediction  = row.get("score", 0.0),
                rank        = row.get("rank"),
                final_odds  = row.get("dollar_odds")
            )
            horses.append(entry)

        # Create the Race object
        race_obj = wc.Race(
            course_cd         = course_cd,
            race_date         = race_date,
            race_number       = race_number,
            horses            = horses,
            distance_meters   = distance_meters,
            surface           = surface,
            track_condition   = track_condition,
            avg_purse_val_calc= avg_purse_val,
            race_type         = race_type,
        )

        # Option A: If you updated wc.Race to have these as constructor params, do:
        # race_obj = wc.Race(..., fav_morn_odds=fav_morn_odds, avg_morn_odds=avg_morn_odds, ...)
        #
        # Option B: Just set them as attributes, if Race class doesn't have them natively:
        race_obj.fav_morn_odds   = fav_morn_odds
        race_obj.avg_morn_odds   = avg_morn_odds
        race_obj.max_prob        = max_prob
        race_obj.second_prob     = second_prob
        race_obj.prob_gap        = prob_gap
        race_obj.std_prob        = std_prob

        # Append to race_list
        race_list.append(race_obj)

    return race_list

def parse_winners_str(winners_str: str):
    """
    Simple parser that splits by '-' into "positions/legs",
    then splits each part by '/' for multiple horses.
    Example: "7-6" => [['7'], ['6']]
             "6-1/4/7/9-5" => [['6'], ['1','4','7','9'], ['5']]
    """
    if not winners_str:
        return []
    parts = winners_str.split('-')
    parsed = [p.split('/') for p in parts]
    return parsed

def build_wagers_dict(wagers_pdf):
    wagers_dict = {}
    for _, row in wagers_pdf.iterrows():
        # Build the tuple key:
        key = (
            row["course_cd"],
            row["race_date"],
            row["race_number"],
            row["wager_type"]
        )
        # Parse the winners
        winners_str = str(row.get('winners', ''))
        parsed_winners = parse_winners_str(winners_str)

        # Convert payoff to float
        payoff_val = row.get('payoff')
        payoff_val = float(payoff_val) if payoff_val is not None else 0.0

        # Store EVERY needed field in the value:
        wagers_dict[key] = {
            "course_cd":   row["course_cd"],
            "race_date":   row["race_date"],
            "race_number": row["race_number"],
            "wager_type":  row["wager_type"],  # Now the value dict also has 'wager_type'
            "winning_combo": parsed_winners,
            "wager_id":    row["wager_id"],
            "num_tickets": row["num_tickets"],
            "payoff":      payoff_val
        }
    return wagers_dict

def find_race(all_races, course_cd, race_date, race_number):
    """
    Returns the Race object from all_races that matches (course_cd, race_date, race_number),
    or None if not found.
    """
    for r in all_races:
        if (r.course_cd == course_cd and 
            r.race_date == race_date and 
            r.race_number == race_number):
            return r
    return None