import src.wagering.wagering_classes as wc
import pandas as pd
from typing import Dict, Any, Tuple, List 


def build_race_objects(races_pdf: pd.DataFrame) -> List[wc.Race]:
    """
    Groups the DataFrame by (course_cd, race_date, race_number),
    then for each group creates:
      - A Race object (with distance, surface, etc. from the group's first row)
      - Multiple HorseEntry objects (one per row in the group).
    Returns a list of Race objects.
    """

    race_list = []
    group_cols = ["course_cd", "race_date", "race_number"]

    for (course_cd, race_date, race_number), group_df in races_pdf.groupby(group_cols):
        # Pull race-level attributes (e.g., distance_meters, surface) from the first row
        first_row = group_df.iloc[0]
        distance_meters = first_row.get("distance_meters")
        surface = first_row.get("surface")
        track_condition = first_row.get("trk_cond")  # or "track_condition" in your DF
        avg_purse_val = first_row.get("avg_purse_val_calc")
        race_type = first_row.get("race_type")
        rank = first_row.get("rank")      

        # Build HorseEntry objects
        horses = []
        for _, row in group_df.iterrows():
            entry = wc.HorseEntry(
                horse_id = str(row.get("horse_id", "")),  # or any unique ID column
                program_num = str(row["saddle_cloth_number"]),
                official_fin = row.get("official_fin"),
                prediction = row.get("prediction", 0.0),
                rank = row.get("rank"),
                final_odds = row.get("dollar_odds")
            )
            horses.append(entry)

        # Create the Race object
        race_obj = wc.Race(
            course_cd=course_cd,
            race_date=race_date,
            race_number=race_number,
            horses=horses,
            distance_meters=distance_meters,
            surface=surface,
            track_condition=track_condition,
            avg_purse_val_calc=avg_purse_val,
            race_type=race_type,
            rank=rank
        )

        race_list.append(race_obj)

    return race_list

def parse_winners_str(winners_str: str):
    """
    Simple parser that splits by '-' into "positions/legs",
    then splits each part by '/' for multiple horses.
    Example: "7-6" => [['7'], ['6']]
             "6-1/4/7/9-5" => [['6'], ['1','4','7','9'], ['5']]
    """
    if not winners_str:
        return []
    parts = winners_str.split('-')
    parsed = [p.split('/') for p in parts]
    return parsed

def build_wagers_dict(wagers_pdf: pd.DataFrame) -> Dict[Tuple[Any, Any, Any, str], Dict[str, Any]]:
    """
    Creates a dictionary keyed by (course_cd, race_date, race_number, wager_type)
    with a value dict of { 'winning_combo': list_of_lists, 'payoff': float }.
    
    Example Result:
    {
      ('TAM', '2022-01-21', 3, 'Exacta'): {
         'winning_combo': [['7'], ['6']],
         'payoff': 38.20
      },
      ...
    }
    """
    wagers_dict = {}
    
    for _, row in wagers_pdf.iterrows():
        key = (
            row['course_cd'],
            row['race_date'],
            row['race_number'],
            row['wager_type']
        )
        
        winners_str = str(row.get('winners', ''))
        parsed_winners = parse_winners_str(winners_str)

        # Convert payoff to float (some rows might be None or NaN)
        payoff_val = row.get('payoff')
        payoff_val = float(payoff_val) if payoff_val is not None else 0.0

        # You could also store 'pool_total' or 'post_time' if needed
        wagers_dict[key] = {
            'winning_combo': parsed_winners,
            'wager_id': row['wager_id'],
            'num_tickets': row['num_tickets'],
            'payoff': payoff_val
        }
            
    return wagers_dict