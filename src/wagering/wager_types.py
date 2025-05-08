import itertools
from .wagering_classes import Wager, Race
import logging
import src.wagering.wagering_classes as wc
from dataclasses import dataclass, field
from typing import Callable, List, Tuple

@dataclass
class WagerStrategy:
    name: str
    wager_type: str                       # "Exacta", "Trifecta", ...
    should_bet: Callable[[wc.Race], bool]
    build_combos: Callable[[wc.Race], List[Tuple[str,...]]]
    base_amount: float = 1.0

    bets: int  = field(default=0, init=False)
    cost: float = field(default=0.0, init=False)
    payoff: float = field(default=0.0, init=False)

    @property
    def roi(self):
        return (self.payoff - self.cost) / self.cost if self.cost else 0.0
    
@dataclass
class ExactaStrategy:
    name: str
    should_bet: Callable[[wc.Race], bool]              # race → True/False
    build_combos: Callable[[wc.Race], List[Tuple[str,str]]]
    base_amount: float = 1.0                           # $ per combo

    # running totals (filled during back-test)
    bets: int  = field(default=0, init=False)
    cost: float = field(default=0.0, init=False)
    payoff: float = field(default=0.0, init=False)
    
class ExactaWager(Wager):
    def __init__(self, wager_amount, top_n, box):
        """
        :param wager_amount: e.g. $1 base
        :param top_n: number of top predicted horses used to form combos
        :param box: if True, generate all permutations for those top_n horses 
                    in 1st/2nd. If False, only the single "top1->top2" order.
        """
        super().__init__(wager_amount)
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

    def generate_combos(self, race: Race):
        """
        Generate trifecta combos for 'race'.
          - If box=True and top_n=4, we create all permutations of the top 4 horses taken 3 at a time.
            E.g. (horseA,horseB,horseC), (horseA,horseC,horseB), ...
          - If box=False, we pick only (top_horse[0], top_horse[1], top_horse[2]) (assuming top_n>=3).
        """
        sorted_horses = race.get_sorted_by_prediction()
        top_horses = sorted_horses[: self.top_n]

        combos = []
        if self.box:
            # All permutations of the chosen top_n, taken 3 at a time
            for perm in itertools.permutations(top_horses, 3):
                combos.append(
                    (perm[0].program_num, perm[1].program_num, perm[2].program_num)
                )
        else:
            # A single "straight" trifecta from the top 3
            if len(top_horses) >= 3:
                combos.append((
                    top_horses[0].program_num,
                    top_horses[1].program_num,
                    top_horses[2].program_num
                ))
        return combos

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