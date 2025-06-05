# trifecta_rules.py
# --------------------------------------------------------------------- #
# ❶  Trifecta rule definition & rule-table (with gap34 support)
# --------------------------------------------------------------------- #
from collections import namedtuple
from typing import Optional

TriRule = namedtuple(
    "TriRule",
    (
        "name  "
        "min_gap12 max_gap12  "
        "min_gap23 max_gap23  "
        "min_gap34 max_gap34  "
        "bet_style pct_bankroll"
    )
)

TRIFECTA_RULES: list[TriRule] = [

    # 1️⃣  Monster favourite and the next two are also well clear
    TriRule("Straight 1-2-3",
            0.25, None,        # gap12  ≥ 0.25
            0.15, None,        # gap23  ≥ 0.15
            0.10, None,        # gap34  ≥ 0.10
            "STRAIGHT123", 0.05),

    # 2️⃣  Clear winner, 2 vs 3 reasonably close → key 1 over (2,3)
    TriRule("Key 1 ⟶ 2,3",
            0.18, None,        # gap12  ≥ 0.18
            None, 0.12,        # gap23  ≤ 0.12
            0.08, None,        # still want 3 vs 4 decent
            "KEY1_23",   0.04),

    # 3️⃣  1 and 2 are close, both clear of the rest → key 1-2 over 3
    TriRule("Key 1,2 ⟶ 3",
            None, 0.12,        # gap12  ≤ 0.12
            0.12, None,        # gap23  ≥ 0.12
            0.05, None,        # modest gap34
            "KEY12_3",  0.03),

    # 4️⃣  Tight top-three cluster, bet the box
    TriRule("Box 1-2-3",
            None, 0.12,        # small gap12
            None, 0.12,        # small gap23
            None, 0.12,        # small gap34
            "BOX123",    0.02),
]

# --------------------------------------------------------------------- #
# ❷  Helper that picks the FIRST rule that matches this race
# --------------------------------------------------------------------- #
def choose_tri_rule(race) -> Optional[TriRule]:
    """
    Decide which trifecta rule (if any) fires for *race*.

    • gap12  = leader-gap of horse #2  
    • gap23  = trailing-gap of horse #2  
    • gap34  = trailing-gap of horse #3

    ( sentinel –1.0 from the loader is treated as 0 )
    """
    # ---------- locate horses #2 and #3 --------------------------------
    h2 = next((h for h in race.horses if int(h.rank) == 2), None)
    h3 = next((h for h in race.horses if int(h.rank) == 3), None)
    if h2 is None or h3 is None:          # scratched / short field
        return None

    gap12 = h2.leader_gap
    gap23 = max(0.0, h2.trailing_gap)     # –1  → 0
    gap34 = max(0.0, h3.trailing_gap)

    # ---------- walk the rule-table ------------------------------------
    for r in TRIFECTA_RULES:
        if r.min_gap12 is not None and gap12 < r.min_gap12:   continue
        if r.max_gap12 is not None and gap12 > r.max_gap12:   continue

        if r.min_gap23 is not None and gap23 < r.min_gap23:   continue
        if r.max_gap23 is not None and gap23 > r.max_gap23:   continue

        if r.min_gap34 is not None and gap34 < r.min_gap34:   continue
        if r.max_gap34 is not None and gap34 > r.max_gap34:   continue

        return r                       # ← first match wins
    return None