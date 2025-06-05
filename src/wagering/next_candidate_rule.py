from copy import deepcopy
from itertools import product
from src.wagering.exacta_rules import WAGER_RULES, choose_rule
from wagers      import implement_ExactaWager   # <- your function
from wager_config import *

def find_grid(spark, all_races, wagers_dict):
    gap_grid   = [0.10, 0.15, 0.20, 0.25, 0.30]
    br_grid    = [0.02, 0.03, 0.04]

    results = []

    for g12, pct in product(gap_grid, br_grid):
        # make a private copy of the table so we don’t corrupt the import
        rules = deepcopy(WAGER_RULES)
        rules[0] = rules[0]._replace(min_gap12=g12, pct_bankroll=pct)
        
        # monkey-patch choose_rule to use *this* copy
        def _choose_rule(r, table=rules):
            g12 = r.prob_gap
            g23 = r.max_prob - r.second_prob
            for rr in table:
                if rr.min_gap12 and g12 < rr.min_gap12:  continue
                if rr.min_gap23 and g23 < rr.min_gap23:  continue
                if rr.max_gap12 and g12 > rr.max_gap12:  continue
                if rr.max_gap23 and g23 > rr.max_gap23:  continue
                return rr
            return None
        import src.wagering.exacta_rules as exacta_rules
        exacta_rules.choose_rule = _choose_rule   # swap in
        
        df = implement_ExactaWager(spark, all_races, wagers_dict,
                                user_cap=2, top_n=3, box=False)
        roi = df.selectExpr("sum(payoff) / sum(cost) - 1").collect()[0][0]
        results.append((g12, pct, roi))

    for g12, pct, roi in sorted(results, key=lambda x: -x[2]):
        print(f"gap≥{g12:.2f}  pctBR {pct:.2%}  ROI {roi:.1%}")