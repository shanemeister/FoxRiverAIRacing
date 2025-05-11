import src.wagering.wager_classes as wc
import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple, List, Iterable, Union
from src.wagering.wager_types import ExactaWager
from datetime import date, timedelta
from pyspark.sql import SparkSession, functions as F, DataFrame
import yaml, pathlib
import logging
# roi_utils.py  (or just paste it where the old helper was)
import yaml, pathlib, logging
from datetime import date, timedelta
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from typing import Set
from wager_config       import (MIN_FIELD, MAX_FIELD, BANKROLL_START,
                           TRACK_MIN, KELLY_THRESHOLD, KELLY_FRACTION,
                           MAX_FRACTION, EDGE_MIN)
from wager_rules  import WAGER_RULES, choose_rule

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

# Path objects declared elsewhere in your module
# BASE_DIR = pathlib.Path(...)
# ROI_YAML  = BASE_DIR / "green_tracks.yaml"
UPCOMING_YAML = BASE_DIR / "upcoming_tracks.yaml"  # sibling to ROI_YAML

###############################################################################
#                helper: write yaml (overwrite every run)                    #
###############################################################################

def _write_upcoming_yaml(track_set: Set[str]) -> None:
    """Persist the current *active* track codes.
    The file is recreated each run so it never goes stale.
    """
    UPCOMING_YAML.parent.mkdir(parents=True, exist_ok=True)
    with UPCOMING_YAML.open("w") as fh:
        yaml.safe_dump({"upcoming_tracks": sorted(track_set)}, fh)

###############################################################################
#                 public: refresh_upcoming_tracks                             #
###############################################################################

def refresh_upcoming_tracks(races: Union[SparkDataFrame, Iterable], *, today: date | None = None) -> Set[str]:
    """Return the set of *course_cd* that still have races *on or after* ``today``.

    Parameters
    ----------
    races : pyspark.sql.DataFrame  **or**  Iterable[wc.Race | dict]
        • If a Spark DataFrame, it must have columns ``course_cd`` and ``race_date`` (date or string).
        • If a plain Python iterable (your ``list[Race]``), each element must expose
          ``course_cd`` and ``race_date`` attributes.
    today : datetime.date | None (default **today()**)
        The cut‑off date.  Races on or after this date are considered *future*.

    Side‑effect
    -----------
    Always overwrites *upcoming_tracks.yaml* with the fresh set so the next run
    starts from a clean slate.
    """
    if today is None:
        today = date.today()
    today_iso = today.isoformat()

    # ------------------------------------------------------------------
    # Spark path
    # ------------------------------------------------------------------
    if isinstance(races, SparkDataFrame):
        future_tracks_df = (
            races
            .filter(F.col("race_date") >= today_iso)
            .select("course_cd")
            .distinct()
        )
        tracks: Set[str] = {row["course_cd"] for row in future_tracks_df.collect()}

    # ------------------------------------------------------------------
    # Python list / iterable path (e.g. list[wc.Race])
    # ------------------------------------------------------------------
    else:
        tracks = {
            getattr(r, "course_cd") if hasattr(r, "course_cd") else r["course_cd"]
            for r in races
            if (
                (getattr(r, "race_date", None) or r["race_date"]) >= today
            )
        }

    # Persist and log
    _write_upcoming_yaml(tracks)
    logging.info(
        "[UPCOMING] Tracks with races ≥ %s: %s",
        today_iso,
        sorted(tracks) if tracks else "NONE",
    )
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

# def backtest_strategies(
#         races: List[wc.Race],
#         wagers_dict: dict,
#         strategies: List[ExactaStrategy]
# ):
#     for race in races:
#         wkey = (race.course_cd, race.race_date, race.race_number, "Exacta")
#         actual = wagers_dict.get(wkey)

#         for strat in strategies:
#             if not strat.should_bet(race):
#                 continue

#             combos = strat.build_combos(race)
#             bet_cost = len(combos) * strat.base_amount
#             strat.bets   += 1
#             strat.cost   += bet_cost

#             # check win
#             payoff = 0.0
#             if actual and len(actual["winning_combo"]) >= 2:
#                 posted_payoff = float(actual["payoff"])
#                 posted_base   = float(actual["num_tickets"])
#                 for c in combos:
#                     if ExactaWager(1,0,False).check_if_win(
#                             c, race, actual["winning_combo"]):
#                         payoff = (strat.base_amount / posted_base) * posted_payoff
#                         break
#             strat.payoff += payoff

#     # final report
#     rows = []
#     for s in strategies:
#         roi = (s.payoff - s.cost) / s.cost if s.cost else 0.0
#         rows.append((s.name, s.bets, s.cost, s.payoff, roi))
#     return rows

############################################################################
# 1)  CLEAN-UP & FEATURE ADDER
############################################################################
DROP_DEFAULT = ["pool_total"]

def clean_races_df(
    races_df: pd.DataFrame,
    wagers_df: pd.DataFrame | None = None,
    cols_to_drop: Iterable[str] = DROP_DEFAULT,
) -> pd.DataFrame:
    """
    • Flattens the *_x / *_y duplicates coming from a merge.
    • Calculates implied_prob, edge, kelly, skill.
    • Optionally merges in a *filtered* copy of wagers_df if you still
      need something (e.g. pool_total before you drop it).
    """
    # -------------------------------------------------- #
    # A)  Deduplicate keys
    # -------------------------------------------------- #
    keep_map = {                                   # canonical -> prefer *_x
        "course_cd":      "course_cd_x" if "course_cd_x" in races_df else "course_cd",
        "race_date":      "race_date_x" if "race_date_x" in races_df else "race_date",
        "race_number":    "race_number_x" if "race_number_x" in races_df else "race_number",
    }
    races_df = races_df.rename(columns={v: k for k, v in keep_map.items() if v != k})

    # Drop *_y versions if they’re still present
    races_df = races_df.drop(columns=[c for c in races_df.columns if c.endswith("_y")], errors="ignore")

    # -------------------------------------------------- #
    # B)  Feature engineering
    # -------------------------------------------------- #
    # Avoid divide-by-zero if morn_odds == 0
    # ----------------------------------------------------------------------
    # NEW Kelly that works when morn_odds is already a probability (p_ml)
    # ----------------------------------------------------------------------
    p_ml = races_df["morn_odds"].clip(upper=0.999)          # guard ÷0
    b    = (1.0 / p_ml) - 1.0                               # fractional price

    races_df["implied_prob"] = p_ml
    races_df["edge"]         = races_df["score"] - p_ml

    races_df["kelly"] = (
        (races_df["score"] * b - (1.0 - races_df["score"])) / b
    ).clip(lower=0)

    # -------------------------------------------------- #
    # D)  Final column drop
    # -------------------------------------------------- #
    races_df = races_df.drop(columns=list(cols_to_drop), errors="ignore")

    return races_df

############################################################################
# 2)  RACE-OBJECT BUILDER  (unchanged interface, cleaner internals)
############################################################################
def build_race_objects(races_pdf: pd.DataFrame) -> List[wc.Race]:
    race_list: List[wc.Race] = []

    for (trk, dt, rno), grp in races_pdf.groupby(
            ["course_cd", "race_date", "race_number"]):

        grp_sorted = grp.sort_values("score", ascending=False)
        first = grp_sorted.iloc[0]

        # ---------- race-level aggregates ----------
        fav_morn = grp["morn_odds"].min(skipna=True)
        avg_morn = grp["morn_odds"].mean(skipna=True)
        max_prob = grp_sorted.iloc[0]["score"]
        second   = grp_sorted.iloc[1]["score"] if len(grp_sorted) > 1 else 0.0
        prob_gap = max_prob - second
        std_prob = grp_sorted["score"].head(4).std(ddof=0)

        # ---------- horse entries ----------
        horses: list[wc.HorseEntry] = []          # ← reset for *each* race
        for _, row in grp.iterrows():
            entry = wc.HorseEntry(
                horse_id    = str(row["horse_id"]),
                program_num = str(row["saddle_cloth_number"]),
                official_fin= row.get("official_fin"),
                prediction  = row.get("score", 0.0),
                rank        = row.get("rank"),
                final_odds  = row.get("dollar_odds"),
            )
            entry.morn_odds = row.get("morn_odds")   
            entry.kelly = row.get("kelly", 0.0)
            entry.edge  = row.get("edge",  0.0)
            horses.append(entry)

        # ---------- race object ----------
        race = wc.Race(
            course_cd         = trk,
            race_date         = dt,
            race_number       = rno,
            horses            = horses,
            distance_meters   = first.get("distance_meters"),
            surface           = first.get("surface"),
            track_condition   = first.get("track_condition"),
            avg_purse_val_calc= first.get("avg_purse_val_calc"),
            race_type         = first.get("race_type"),
        )

        race.fav_morn_odds = fav_morn
        race.avg_morn_odds = avg_morn
        race.max_prob      = max_prob
        race.second_prob   = second
        race.prob_gap      = prob_gap
        race.std_prob      = std_prob

        race_list.append(race)

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

def choose_rule(race):
    """Return the first rule whose condition matches this race, else None."""
    g12 = race.prob_gap                      #   gap top-1  –  top-2
    g23 = race.max_prob - race.second_prob   #   gap top-2  –  top-3

    for r in WAGER_RULES:
        if r.min_gap12  is not None and g12 < r.min_gap12:  continue
        if r.max_gap12  is not None and g12 > r.max_gap12:  continue
        if r.min_gap23  is not None and g23 < r.min_gap23:  continue
        if r.max_gap23  is not None and g23 > r.max_gap23:  continue
        return r
    return None

def log_combo_metrics(
    race: wc.Race,
    combos: list[tuple[str, str]],
    hit_flag: bool,
) -> None:
    """
    One log-line per horse that appears in *any* combo for this race.
      ✔ = first-leg horse, ▶ = second-leg horse.
      Adds gap12  (=prob_gap) and gap23 (=top-2 – top-3) at the end.
    """
    # ------------------------------------------------------------------
    gap12 = race.prob_gap                             # top-1 – top-2
    gap23 = race.max_prob - race.second_prob          # top-2 – top-3
    # ------------------------------------------------------------------

    prog_map = {h.program_num: h for h in race.horses}
    logging.info("combo size %d  |  %s", len(combos), combos)

    for a, b in combos:
        for p, mark in ((a, "✔"), (b, "▶")):
            h = prog_map.get(p)
            if h is None:                # scratched, etc.
                continue

            logging.info(
                "[%s-%d] %s prog=%s rank=%2d  score=%6.4f  kelly=%5.3f "
                "edge=%+6.3f  ML=%5.2f  %s  gap12=%5.3f  gap23=%5.3f",
                race.course_cd,
                race.race_number,
                mark,                    # ✔ (first-leg) or ▶ (second-leg)
                p,
                h.rank,
                h.prediction,
                h.kelly,
                h.edge,
                getattr(h, "morn_odds", float("nan")),
                race.race_date,
                gap12,
                gap23,
            )