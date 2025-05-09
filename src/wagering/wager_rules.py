import collections

Rule = collections.namedtuple(
    "Rule",
    "name min_gap12 min_gap23 max_gap12 max_gap23 bet_style pct_bankroll"
)

WAGER_RULES = [
    # name                 min12  min23  max12  max23  style       %BR
    Rule("Straight",        0.20,  0.10, None,  None,  "STRAIGHT", 0.04),
    Rule("Key1 over 2-3",   0.12,   0.00, None, 0.05, "KEY12",     0.03),
    Rule("Box 1-2-3",       0.00,  0.08, 0.08,  None, "BOX123",    0.02),
]

def choose_rule(race) -> Rule | None:
    """
    Decide which Rule (if any) applies to *this* race, based on the two
    gap metrics you already store on each wc.Race:
        • gap12 = prob_gap  (top-1 minus top-2)
        • gap23 = max_prob – second_prob
    The first matching rule in WAGER_RULES wins.
    """
    g12 = race.prob_gap
    g23 = race.max_prob - race.second_prob

    for r in WAGER_RULES:
        if r.min_gap12 is not None and g12 < r.min_gap12:
            continue
        if r.min_gap23 is not None and g23 < r.min_gap23:
            continue
        if r.max_gap12 is not None and g12 > r.max_gap12:
            continue
        if r.max_gap23 is not None and g23 > r.max_gap23:
            continue
        return r                     # <-- first rule that fits
    return None                       # nothing matched → PASS
