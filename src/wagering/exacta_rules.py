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

    # 1️⃣ Monsters – both gaps huge → straight
    Rule("Huge gaps straight",
         0.30, None,      0.15, None,      "STRAIGHT", 0.05),

    # 2️⃣ Clear winner, but 2 and 3 are close → key 1-2
    Rule("Big gap key12",
         0.40, None,      None, 0.15,      "KEY12",    0.01),

    # # # 3️⃣ Tight 1-2, yet both clear of the rest → box 1-2
    Rule("Box 1-2",
         None, 0.15,      0.25, None,      "BOX12",    0.01),
]

# --------------------------------------------------------------------- #
# 3)  choose_exacta_rule() helper used by implement_ExactaWager
# --------------------------------------------------------------------- #
def choose_exacta_rule(race) -> Optional[Rule]:
    """
    Return the first rule whose gap-filters AND sec_score test match.
    If nothing matches → None  (skip race).
    """
    # ---------- pull the 3 best horses --------------------------------
    best3 = sorted(race.horses, key=lambda h: h.rank)[:3]
    if len(best3) < 3:                             # tiny field → skip
        return None

    sec1, sec2, sec3 = (getattr(h, "sec_score", None) for h in best3)

    # ---------- new condition: sec1  <  sec2  <  sec3 -----------------
    if not (sec1 is not None and sec2 is not None and sec3 is not None
            and sec1 < sec2 < sec3):
        logging.debug("SKIP – sec_score monotonicity fails: "
                      "sec1=%.4f  sec2=%.4f  sec3=%.4f",
                      sec1, sec2, sec3)
        return None            # bail out before testing the gap rules

    # ---------- classic gap-based rule walk --------------------------
    gap12 = best3[1].leader_gap        # (= top-2 − top-1 prob)
    gap23 = max(0.0, best3[1].trailing_gap)

    for r in WAGER_RULES:
        if r.min_gap12 is not None and gap12 < r.min_gap12: continue
        if r.max_gap12 is not None and gap12 > r.max_gap12: continue
        if r.min_gap23 is not None and gap23 < r.min_gap23: continue
        if r.max_gap23 is not None and gap23 > r.max_gap23: continue
        return r                             # ← first rule that fits

    return None                               # nothing matched