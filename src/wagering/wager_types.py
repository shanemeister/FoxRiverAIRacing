import itertools
from wager_classes import Wager, Race
import logging
import src.wagering.wager_classes as wc
from dataclasses import dataclass, field
from typing import Callable, List, Tuple
    
class ExactaWager(Wager):
    """
    • `generate_combos`  → still supports the legacy *top-n / box* interface  
    • `combos_from_style`→ used by the new rule-table engine
    """
    def __init__(self, stake, top_n=2, box=True):
        super().__init__(stake)
        self.top_n = top_n
        self.box   = box

    # ------------------------------------------------------------------ #
    # ❶  OLD interface – leave untouched so old code keeps working
    # ------------------------------------------------------------------ #
    def generate_combos(self, race: Race):
        sorted_horses = race.get_sorted_by_prediction()
        top = sorted_horses[: self.top_n]
        if self.box:
            return [
                (a.program_num, b.program_num)
                for a, b in itertools.permutations(top, 2)
            ]
        return [
            (top[i].program_num, top[i + 1].program_num)
            for i in range(self.top_n - 1)
        ]

    # ------------------------------------------------------------------ #
    # ❷  NEW helper – called by the rule engine
    # ------------------------------------------------------------------ #
    # ------------------------------------------------------------------ #
    # ❸  helper used by the rule engine
    # ------------------------------------------------------------------ #
    # src/wagering/wagers.py   (ExactaWager)

    @staticmethod
    def combos_from_style(race: Race, style: str, top_n: int = 3):
        """
        Build the (prog_num, prog_num) pairs **by CatBoost rank**.

        * rank 1  = model’s top choice
        * rank 2  = second choice
        * ...
        """
        # ------------------------------------------------------------------
        # ❶  pick the top-N horses by rank (ascending = 1,2,3…)
        #     tie-break on higher calibrated probability if two have same rank
        # ------------------------------------------------------------------
        horses_by_rank = sorted(
            race.horses,
            key=lambda h: (int(h.rank),        # NB: rank is stored as float
                        -h.prediction)      # secondary sort
        )[:top_n]

        if len(horses_by_rank) < 2:            # not enough runners
            return []

        nums = [str(h.program_num) for h in horses_by_rank]

        # ------------------------------------------------------------------
        # ❷  build ticket(s) according to the chosen rule
        # ------------------------------------------------------------------
        if style == "STRAIGHT":
            return [(nums[0], nums[1])]

        if style == "KEY12":
            if top_n < 3:
                return []
            return [(nums[0], nums[1]), (nums[0], nums[2])]

        if style == "BOX123":
            return list(itertools.permutations(nums, 2))

        # 🔽  ← add this block
        if style == "BOX12":                  # box the two favourites
            if len(nums) < 2:                 # ← optional safety net
                return []
            return [(nums[0], nums[1]), (nums[1], nums[0])]

        raise ValueError(f"Unknown exacta style: {style}")
    # ------------------------------------------------------------------ #
    def check_if_win(self, combo, race: Race, official):
        """
        `official` is usually [[WIN],[PLC]].
        Compare after casting to str so 9 == "9".
        """
        if not official or len(official) < 2:
            return False

        actual_1st = str(official[0][0])
        actual_2nd = str(official[1][0])

        return combo == (actual_1st, actual_2nd)
    
class TrifectaWager(Wager):
    """
    A Trifecta wager predicts the exact order of the top 3 finishers.
    If box=True, we generate permutations of the top_n horses taken 3 at a time.
    If box=False, we only generate a single "straight" combo from the top 3.
    """

    def __init__(self, wager_amount, top_n, box):
        super().__init__(wager_amount)
        self.top_n = top_n
        self.box = box

    @staticmethod
    def combos_from_style(race: Race, style: str, top_n: int = 3) -> List[Tuple[str, str, str]]:
        """
        Build a list of (win-prog, place-prog, show-prog) triples.

        Styles supported
        ----------------
        • STRAIGHT123   : only the exact order 1-2-3  
        • KEY1_23       : #1 must win; 2 & 3 can swap underneath  
        • KEY12_3       : 1 or 2 wins, the other is 2nd, #3 must be 3rd  
        • BOX123        : full 3-horse box (6 perms)

        The *rank* used is the model rank (1 = best).  If the field is too
        short the function returns an empty list → caller skips the race.
        """
        # ---------- 1) horses needed for each style -----------------------
        required = {
            "STRAIGHT123": 3,
            "KEY1_23"    : 3,
            "KEY12_3"    : 3,
            "BOX123"     : 3,
        }.get(style)

        if required is None:
            raise ValueError(f"Unknown trifecta style: {style}")

        # ---------- 2) pick the top-`required` horses ---------------------
        horses_by_rank = sorted(
            race.horses,
            key=lambda h: (int(h.rank), -h.prediction)   # rank ascending, prob tiebreak
        )[:required]

        if len(horses_by_rank) < required:              # very short field → no bet
            return []

        nums = [str(h.program_num) for h in horses_by_rank]  # ['1','2','3', …]

        # ---------- 3) build the combos ----------------------------------
        if style == "STRAIGHT123":
            return [(nums[0], nums[1], nums[2])]

        if style == "KEY1_23":               # 1 ⟶ (2,3) boxed underneath
            return [
                (nums[0], nums[1], nums[2]),
                (nums[0], nums[2], nums[1]),
            ]

        if style == "KEY12_3":               # (1,2) ⟶ 3 boxed on top
            return [
                (nums[0], nums[1], nums[2]),
                (nums[1], nums[0], nums[2]),
            ]

        if style == "BOX123":                # full 3-horse box
            return list(itertools.permutations(nums, 3))

        # Should never reach here
        raise ValueError(f"Unhandled style: {style}")

    def check_if_win(self, combo, race: Race, actual_winning_combo):
        """
        actual_winning_combo is typically [[WIN1],[WIN2],[WIN3]] (the first 3 finishers).
        We compare to combo = (prognum1, prognum2, prognum3).
        """
        if not actual_winning_combo or len(actual_winning_combo) < 3:
            return False
        
        actual_1st = actual_winning_combo[0][0]
        actual_2nd = actual_winning_combo[1][0]
        actual_3rd = actual_winning_combo[2][0]

        return combo == (actual_1st, actual_2nd, actual_3rd)

class SuperfectaWager(Wager):
    """
    A Superfecta wager predicts the exact order of the top 4 finishers.
    If box=True, we generate permutations of the top_n horses taken 4 at a time.
    If box=False, we pick only the single "straight" combo from the top 4.
    """

    def __init__(self, wager_amount, top_n, box):
        super().__init__(wager_amount)
        self.top_n = top_n
        self.box = box

    def generate_combos(self, race: Race):
        """
        Generate superfecta combos for 'race'.
          - If box=True and top_n=5, we create permutations of top 5 horses taken 4 at a time.
          - If box=False, just the single combo (top1, top2, top3, top4).
        """
        sorted_horses = race.get_sorted_by_prediction()
        top_horses = sorted_horses[: self.top_n]

        combos = []
        if self.box:
            for perm in itertools.permutations(top_horses, 4):
                combos.append(
                    (perm[0].program_num, perm[1].program_num, perm[2].program_num, perm[3].program_num)
                )
        else:
            if len(top_horses) >= 4:
                combos.append((
                    top_horses[0].program_num,
                    top_horses[1].program_num,
                    top_horses[2].program_num,
                    top_horses[3].program_num
                ))
        return combos

    def check_if_win(self, combo, race: Race, actual_winning_combo):
        """
        actual_winning_combo is typically [[WIN1],[WIN2],[WIN3],[WIN4]].
        We compare to combo = (p1, p2, p3, p4).
        """
        if not actual_winning_combo or len(actual_winning_combo) < 4:
            return False

        actual_1st = actual_winning_combo[0][0]
        actual_2nd = actual_winning_combo[1][0]
        actual_3rd = actual_winning_combo[2][0]
        actual_4th = actual_winning_combo[3][0]

        return combo == (actual_1st, actual_2nd, actual_3rd, actual_4th)
    
class MultiRaceWager(Wager):
    """
    A general multi-race wager class for multi-leg bets:
      e.g. Daily Double (num_legs=2), Pick 3 (num_legs=3), etc.

    It uses program_num for combos, so we can compare them to official 
    winners that the track lists in program_num form.
    """
    def __init__(self, wager_amount, num_legs, top_n, box):
        super().__init__(wager_amount)
        self.num_legs = num_legs
        self.top_n = top_n
        self.box = box

    def generate_combos(self, list_of_races):
        """
        For each Race in list_of_races:
          - Take the top_n horses by .get_sorted_by_prediction()
          - Gather their .program_num
        If box=True => cartesian product across the legs
        If box=False => single “straight” combo from each leg’s first pick
        """
        if len(list_of_races) != self.num_legs:
            logging.warning(f"[MultiRaceWager] generate_combos: expected {self.num_legs} races, got {len(list_of_races)}")
            return []

        all_legs_picks = []
        for idx, race in enumerate(list_of_races, start=1):
            sorted_horses = race.get_sorted_by_prediction()
            top_horses = sorted_horses[: self.top_n]
            picks_for_leg = [h.program_num for h in top_horses]
            logging.info(f"[Leg {idx}] top picks => {picks_for_leg}")
            all_legs_picks.append(picks_for_leg)

        if self.box:
            combos = list(itertools.product(*all_legs_picks))
        else:
            # only one “straight” combo
            combos = []
            if all(len(p) >= 1 for p in all_legs_picks):
                one_combo = tuple(p[0] for p in all_legs_picks)
                combos.append(one_combo)

        logging.info(f"Generated {len(combos)} combos => {combos}")
        return combos

    def check_if_win(self, combo, list_of_races, actual_winning_combo):
        """
        combo: e.g. ("7","1","6") if you have 3 legs
        actual_winning_combo: e.g. [ ["7"], ["1"], ["6"] ]
        We require combo[i] in actual_winning_combo[i].
        """
        if len(list_of_races) != self.num_legs:
            return False
        if len(actual_winning_combo) < self.num_legs:
            return False

        for i in range(self.num_legs):
            if combo[i] not in actual_winning_combo[i]:
                return False
        return True
    
    def get_top_n_for_leg(self, leg_index, race):
        """
        Return the program_nums of the top_n horses in the given race.
        This matches the logic in generate_combos() but for logging/debug.
        """
        sorted_horses = race.get_sorted_by_prediction()
        top_horses = sorted_horses[: self.top_n]
        picks_for_leg = [h.program_num for h in top_horses]
        return picks_for_leg
