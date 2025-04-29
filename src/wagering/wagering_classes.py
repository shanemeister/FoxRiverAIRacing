from abc import ABC, abstractmethod
from typing import List, Optional
import itertools
import src.wagering.wager_types as wt

class Race:
    """
    Represents a single race, containing multiple HorseEntry objects.
    """
    def __init__(
        self,
        course_cd: str,
        race_date,
        race_number: int,
        horses: List['HorseEntry'],
        distance_meters: Optional[float] = None,
        surface: Optional[str] = None,
        track_condition: Optional[str] = None,
        avg_purse_val_calc: Optional[float] = None,
        race_type: Optional[str] = None,
        rank: Optional[float] = None
    ):
        """
        :param course_cd:           Track code (e.g. 'LRL', 'TAM')
        :param race_date:           The date of the race (datetime.date)
        :param race_number:         The race number on that date at the track
        :param horses:              A list of HorseEntry objects
        :param distance_meters:     Optional distance in meters
        :param surface:             E.g., 'Dirt', 'Turf', 'Synthetic'
        :param track_condition:     E.g., 'Fast', 'Good', 'Sloppy', 'Firm'
        :param avg_purse_val_calc:  Optional numeric measure of purse/class
        :param race_type:           Allowance, Stake, Claiming, etc.
        :param rank                 Predicted finish rank (e.g., 1st, 2nd, etc.)
        """
        self.course_cd = course_cd
        self.race_date = race_date
        self.race_number = race_number

        self.horses = horses  # list of HorseEntry objects

        self.distance_meters = distance_meters
        self.surface = surface
        self.track_condition = track_condition
        self.avg_purse_val_calc = avg_purse_val_calc
        self.race_type = race_type
        self.rank = rank

    def get_sorted_by_prediction(self) -> List['HorseEntry']:
        """
        Return the list of horses sorted by model prediction in descending order.
        (e.g., top predicted horse first)
        """
        return sorted(self.horses, key=lambda h: h.rank, reverse=False)

    def get_sorted_by_finish(self) -> List['HorseEntry']:
        """
        Return the horses sorted by official finishing position in ascending order
        (i.e. 1, 2, 3...).
        Horses without an official_fin (None) are placed at the end.
        """
        # Sort with None finishing positions at the end
        return sorted(self.horses, key=lambda h: (h.official_fin is None, h.official_fin))

    def __repr__(self):
        return (f"Race(course_cd={self.course_cd}, race_date={self.race_date}, "
                f"race_number={self.race_number}, horses={self.horses}, "
                f"distance_meters={self.distance_meters}, surface={self.surface}, "
                f"track_condition={self.track_condition}, "
                f"avg_purse_val_calc={self.avg_purse_val_calc})")
        
class Wager(ABC):
    """
    Abstract base class for different types of wagers (Exacta, Trifecta, Pick3, etc.).
    Subclasses should implement how combos are generated and how a winning combo is checked.
    """
    def __init__(self, base_amount: float = 1.0):
        """
        :param base_amount: The base cost of one combination (e.g., $1)
        """
        self.base_amount = base_amount

    @abstractmethod
    def generate_combos(self, race: Race):
        """
        Return a list of combos (each combo can be a tuple or list representing
        the horses/positions) based on the chosen strategy (e.g., box, key, top picks, etc.).
        """
        pass

    @abstractmethod
    def check_if_win(self, combo, race: Race, actual_winning_combo):
        """
        Compare one of our combos to the actual winning combo (from exotic_wagers).
        Return True if it hits, False if not.
        """
        pass

    def calculate_cost(self, combos):
        """
        By default: cost = number_of_combos * base_amount.
        Subclasses can override if needed (e.g., partial wheels, etc.).
        """
        return len(combos) * self.base_amount
    
    def calculate_payoff(self, posted_payoff: float, posted_base: float = 2.0) -> float:
        
        scaling_factor = self.base_amount / posted_base
        return posted_payoff * scaling_factor
        
    def __repr__(self):
        return f"{self.__class__.__name__}(base_amount={self.base_amount})"
    
class HorseEntry:
    """
    Represents a single horse (or entry) in a race.
    """
    def __init__(
        self,
        horse_id: str,
        program_num: str,
        official_fin: Optional[int],
        prediction: float,
        rank: float,
        final_odds: Optional[float] = None
    ):
        """
        :param horse_id:      A unique identifier for the horse
        :param program_num:   The saddle_cloth_number or program number
        :param official_fin:  The final finishing position (1 = winner, 2 = second, etc.) 
                              If None, possibly means not applicable or data missing
        :param prediction:    The model's predicted probability or score (higher = better)
        :param final_odds:    The final post-time odds (optional). 
                              Could be None if not available or for historical partial data
        """
        self.horse_id = horse_id
        self.program_num = program_num
        self.official_fin = official_fin
        self.prediction = prediction
        self.rank = rank
        self.final_odds = final_odds

    def __repr__(self):
        return (f"HorseEntry("
                f"horse_id={self.horse_id}, program_num={self.program_num}, "
                f"official_fin={self.official_fin}, prediction={self.prediction}, rank={self.rank}, "
                f"final_odds={self.final_odds})")

class BettingSimulation:
    def __init__(self, wagers_data):
        """
        wagers_data: a dict or df that has the actual exotics payoff info keyed by:
          (course_cd, race_date, race_number, 'Exacta') => { 'winning_combo': [...], 'payoff': XX.XX }
        or something similar
        """
        self.wagers_data = wagers_data
    
    def simulate_race(self, race: Race, wager: Wager):
        combos = wager.generate_combos(race)
        cost = wager.calculate_cost(combos)
        
        # look up the actual winning combo from self.wagers_data
        wager_key = (race.course_cd, race.race_date, race.race_number, "Exacta")  # for example
        # see if data is present
        actual_info = self.wagers_data.get(wager_key)
        
        payoff = 0.0
        if actual_info:
            actual_combo = actual_info['winning_combo']  # e.g. [["7"], ["6"]]
            pay = float(actual_info['payoff'])
            # if ANY combo in combos matches actual, you collect that payoff
            for combo in combos:
                if wager.check_if_win(combo, race, actual_combo):
                    payoff = pay
                    break  # you only collect once presumably
            
        return cost, payoff
    
    def simulate_all_races(self, races, wager):
        total_cost, total_payoff = 0.0, 0.0
        for r in races:
            c, p = self.simulate_race(r, wager)
            total_cost += c
            total_payoff += p
        return total_cost, total_payoff