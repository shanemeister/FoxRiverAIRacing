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

class Pick3Wager(Wager):
    """
    A Pick 3 class that uses 'rank' (instead of program_num) for combos.
    If top_n=2, we pick the two horses with the highest or lowest 'prediction' 
    and store their 'rank' attributes in combos.
    """
    def __init__(self, base_amount=2.0, top_n=2, box=True):
        super().__init__(base_amount)
        self.top_n = top_n
        self.box = box

    def generate_combos(self, three_races):
        """
        For each of the 3 races, gather top_n horses by prediction, 
        but store the 'rank' attribute in combos.
        """
        if len(three_races) != 3:
            logging.warning("generate_combos received a set of len != 3.")
            return []

        leg_top_ranks = []
        for idx, race in enumerate(three_races, start=1):
            sorted_horses = race.get_sorted_by_prediction()  
            top_horses = sorted_horses[:self.top_n]   # e.g. 2 horses

            # LOG: how many total horses and which ranks we're taking
            logging.info(f"Race {idx}: Found {len(sorted_horses)} horses in sorted list.")
            ranks_str = [str(h.rank) for h in top_horses]
            ranks_str = [str(h.rank) for h in top_horses]
        
            logging.info(f"Race {idx}: Found {len(sorted_horses)} horses in sorted list.")
            logging.info(f"Race {idx}: Taking top {self.top_n} => {ranks_str}")

            leg_top_ranks.append(ranks_str)

        # Combine them across the 3 legs
        combos = []
        if self.box:
            # If top_n=2 => 2 x 2 x 2 = 8 combos
            for r1 in leg_top_ranks[0]:
                for r2 in leg_top_ranks[1]:
                    for r3 in leg_top_ranks[2]:
                        combos.append((r1, r2, r3))
        else:
            # single combo if box=False
            if all(leg_top_ranks) and len(leg_top_ranks[0])>0 and len(leg_top_ranks[1])>0 and len(leg_top_ranks[2])>0:
                combos.append((
                    leg_top_ranks[0][0],
                    leg_top_ranks[1][0],
                    leg_top_ranks[2][0]
                ))

        logging.info(f"Generated {len(combos)} combos: {combos}")
        return combos

    def check_if_win(self, combo, three_races, actual_winning_combo):
        """
        'combo' might be ('1','2','1') if you're referencing rank=1 or rank=2. 
        'actual_winning_combo' might also be e.g. [ ['1'], ['2'], ['1'] ] 
        if the official 'rank' winners for each leg is rank=1 or rank=2, etc.
        Return True if each leg in combo is found in the sub-list for that leg's winners.
        """
        if len(three_races) != 3:
            logging.info("Error in check_if_win method: not 3 races.")
            return False
        if not actual_winning_combo or len(actual_winning_combo) < 3:
            logging.info("No actual winning combo found or not enough legs in actual combo.")
            return False

        # If there's a possibility of multiple 'rank' winners (dead heats?), 
        # actual_winning_combo[leg] might be e.g. ['1','2']. 
        # We'll check if combo[i] is in that sub-list.
        for i in range(3):
            if combo[i] not in actual_winning_combo[i]:
                return False
        return True