"""
Simple two-rule table for Exacta betting
---------------------------------------

    •  gap12  = prob_gap            (top-1 – top-2)
    •  gap23  = max_prob – second_prob
"""
import logging
from collections import namedtuple
from typing import Optional
from wager_config import TRACK_MIN      # only to keep the same import-style
from wager_config import EDGE_MIN   

# --------------------------------------------------------------------- #
# 1)  Rule definition
# --------------------------------------------------------------------- #
# -------------------- rule definition --------------------
# ------------------------------------------------------------------ #
# rule definition
Rule = namedtuple(
    "Rule",
    "name  min_gap12 max_gap12  min_gap23 max_gap23  bet_style pct_bankroll",
)

# --------------------------------------------------------------------- #
# 2)  Hand-tuned rule set  (tweak the four numbers ⬇ as you experiment)
#  	•	gap12 = top-1 − top-2
# 	•	gap23 = top-2 − top-3
# 	•	Rule 1 - if both gaps are “large” → 1-2 straight, 4 % bankroll
# 	•	Rule 2 - if gap12 is small but gap23 is still large → box the same two horses, 3 % bankroll
# 	•	everything that misses those ranges is skipped (no bet)
# --------------------------------------------------------------------- #
# -------------------- rule table -------------------------
# wager rules (use *either* edge or Kelly later for stake sizing)
WAGER_RULES = [

    # 1️⃣   Monsters – both gaps in the top ~10 %
    Rule("Huge gaps straight",
         0.25, None,      0.15, None,      "STRAIGHT", 0.05),

    # 2️⃣   Very clear winner, but 2 ↔ 3 still big enough
    Rule("Big gap key12",
         0.18, 0.25,      0.10, None,      "KEY12",    0.04),

    # 3️⃣   Tight 1 ↔ 2, yet they’re both clear of the rest
    Rule("Small gap12 box12",
         None, 0.18,      0.12, None,      "BOX12",    0.03),

    # 4️⃣   Insurance – skinny races where only gap23 explodes
    #       (let you grab long-shot exactas when #2 is mis-priced)
    Rule("Gap23 only key21",
         None, 0.10,      0.20, None,      "KEY21",    0.02),
]
# --------------------------------------------------------------------- #
# 3)  choose_rule() helper used by implement_ExactaWager
# --------------------------------------------------------------------- #
def choose_rule(race) -> Optional[Rule]:
    best2 = sorted(race.horses, key=lambda h: h.rank)[:2]
    if any(h.edge < 0.02 for h in best2):   # <- 2 % overlay filter
        return None                         # pass race

    g12 = race.prob_gap
    g23 = race.max_prob - race.second_prob

    # ─── probe ───
    logging.debug("HERE IT IS: [%s-%d] gap12=%5.3f  gap23=%5.3f",
                  race.course_cd, race.race_number, g12, g23)

    for r in WAGER_RULES:
        if r.min_gap12 is not None and g12 < r.min_gap12: continue
        if r.max_gap12 is not None and g12 > r.max_gap12: continue
        if r.min_gap23 is not None and g23 < r.min_gap23: continue
        if r.max_gap23 is not None and g23 > r.max_gap23: continue
        return r
    return None