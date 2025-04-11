import itertools
from .wagering_classes import Wager, Race
import logging

class ExactaWager(Wager):
    def __init__(self, base_amount=2.0, top_n=2, box=False):
        """
        :param base_amount: e.g. $1 base
        :param top_n: number of top predicted horses used to form combos
        :param box: if True, generate all permutations for those top_n horses 
                    in 1st/2nd. If False, only the single "top1->top2" order.
        """
        super().__init__(base_amount)
        self.top_n = top_n
        self.box = box

    def generate_combos(self, race: Race):
        """
        If box=True and top_n=3, you'd get permutations of the top 3 horses taken 2 at a time.
        If box=False, you only pair top_horse_1 as first, top_horse_2 as second, etc.
        """
        sorted_horses = race.get_sorted_by_prediction()
        top_horses = sorted_horses[:self.top_n]

        combos = []
        if self.box:
            # permutations of the top_n horses, taking 2
            for perm in itertools.permutations(top_horses, 2):
                combos.append((perm[0].program_num, perm[1].program_num))
        else:
            # just a single combo from #1 to #2, #2 to #3, etc. 
            # but often we only do (top_horses[0], top_horses[1]) if top_n>=2
            for i in range(self.top_n - 1):
                combo = (top_horses[i].program_num, top_horses[i+1].program_num)
                combos.append(combo)
        return combos

    def check_if_win(self, combo, race: Race, actual_winning_combo):
        """
        For an exacta, actual_winning_combo is typically: [[WINNER],[SECOND]].
        We just see if combo == (WINNER, SECOND).
        """
        if not actual_winning_combo or len(actual_winning_combo) < 2:
            return False
        
        actual_1st = actual_winning_combo[0][0]
        actual_2nd = actual_winning_combo[1][0]
        
        return combo == (actual_1st, actual_2nd)

import logging
import itertools
# from src.wagering.wager_types import Wager  # or wherever your Wager base class is located

class MultiRaceWager(Wager):
    """
    A general multi-race wager class for multi-leg bets:
      e.g. Daily Double (num_legs=2), Pick 3 (num_legs=3), etc.

    It uses program_num for combos, so we can compare them to official 
    winners that the track lists in program_num form.
    """
    def __init__(self, base_amount=2.0, num_legs=3, top_n=2, box=True):
        super().__init__(base_amount=base_amount)
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