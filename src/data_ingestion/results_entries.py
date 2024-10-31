from lxml import etree
import json
import logging
from ingestion_utils import (
    validate_xml, get_text, parse_time, parse_date, safe_int, safe_float,
    gen_race_identifier, log_rejected_record, convert_last_pp_to_json, convert_point_of_call_to_json
)
from datetime import datetime

def process_results_entries_file(xml_file, conn, cursor, xsd_schema_path):
    """
    Process individual XML race results data file and insert into the results_entries table.
    Validates the XML against the provided XSD schema.
    """
    if not validate_xml(xml_file, xsd_schema_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        return "error"

    has_rejections = False
    
    try:
        tree = etree.parse(xml_file)
        root = tree.getroot()

        course_cd_nodes = root.xpath('.//TRACK/CODE/text()')
        course_cd = course_cd_nodes[0] if course_cd_nodes else None
        if course_cd is None:
            raise ValueError("TRACK/CODE element not found or empty")

        race_date = parse_date(root.get("RACE_DATE"))

        for race_elem in root.xpath('.//RACE'):
            race_number = safe_int(race_elem.get("NUMBER"))
            race_identifier = gen_race_identifier(course_cd, race_date, race_number)
            
            for entry_elem in race_elem.xpath('.//ENTRY'):
                try:
                    horse_name = entry_elem.xpath('./NAME/text()')[0] if entry_elem.xpath('./NAME/text()') else None
                    breed = entry_elem.xpath('./BREED/text()')[0] if entry_elem.xpath('./BREED/text()') else None
                    last_pp_elem = entry_elem.xpath('./LAST_PP')[0] if entry_elem.xpath('./LAST_PP') else None
                    last_pp_json = convert_last_pp_to_json(last_pp_elem) if last_pp_elem else None
                    weight = safe_int(entry_elem.xpath('./WEIGHT/text()')[0]) if entry_elem.xpath('./WEIGHT/text()') else None
                    age = safe_int(entry_elem.xpath('./AGE/text()')[0]) if entry_elem.xpath('./AGE/text()') else None
                    sex = entry_elem.xpath('./SEX/CODE/text()')[0] if entry_elem.xpath('./SEX/CODE/text()') else None
                    meds = entry_elem.xpath('./MEDS/text()')[0] if entry_elem.xpath('./MEDS/text()') else None
                    equip = entry_elem.xpath('./EQUIP/text()')[0] if entry_elem.xpath('./EQUIP/text()') else None
                    dollar_odds = safe_float(entry_elem.xpath('./DOLLAR_ODDS/text()')[0]) if entry_elem.xpath('./DOLLAR_ODDS/text()') else None
                    program_num = entry_elem.xpath('./PROGRAM_NUM/text()')[0] if entry_elem.xpath('./PROGRAM_NUM/text()') else None
                    post_pos = safe_int(entry_elem.xpath('./POST_POS/text()')[0]) if entry_elem.xpath('./POST_POS/text()') else None
                    claim_price = safe_float(entry_elem.xpath('./CLAIM_PRICE/text()')[0]) if entry_elem.xpath('./CLAIM_PRICE/text()') else None
                    start_position = safe_int(entry_elem.xpath('./START_POSITION/text()')[0]) if entry_elem.xpath('./START_POSITION/text()') else None
                    official_fin = safe_int(entry_elem.xpath('./OFFICIAL_FIN/text()')[0]) if entry_elem.xpath('./OFFICIAL_FIN/text()') else None
                    finish_time = entry_elem.xpath('./FINISH_TIME/text()')[0] if entry_elem.xpath('./FINISH_TIME/text()') else None
                    speed_rating = safe_int(entry_elem.xpath('./SPEED_RATING/text()')[0]) if entry_elem.xpath('./SPEED_RATING/text()') else None
                    owner = entry_elem.xpath('./OWNER/text()')[0] if entry_elem.xpath('./OWNER/text()') else None
                    comment = entry_elem.xpath('./COMMENT/text()')[0] if entry_elem.xpath('./COMMENT/text()') else None
                    winners_details = entry_elem.xpath('./WINNERS_DETAILS/text()')[0] if entry_elem.xpath('./WINNERS_DETAILS/text()') else None
                    win_payoff = safe_float(entry_elem.xpath('./WIN_PAYOFF/text()')[0]) if entry_elem.xpath('./WIN_PAYOFF/text()') else None
                    place_payoff = safe_float(entry_elem.xpath('./PLACE_PAYOFF/text()')[0]) if entry_elem.xpath('./PLACE_PAYOFF/text()') else None
                    show_payoff = safe_float(entry_elem.xpath('./SHOW_PAYOFF/text()')[0]) if entry_elem.xpath('./SHOW_PAYOFF/text()') else None
                    show_payoff2 = safe_float(entry_elem.xpath('./SHOW_PAYOFF2/text()')[0]) if entry_elem.xpath('./SHOW_PAYOFF2/text()') else None
                    axciskey = entry_elem.xpath('./AXCISKEY/text()')[0] if entry_elem.xpath('./AXCISKEY/text()') else None
                    point_of_call_elems = entry_elem.xpath('./POINT_OF_CALL')
                    point_of_call_json = convert_point_of_call_to_json(point_of_call_elems)

                    jock_key = entry_elem.xpath('./JOCKEY/KEY/text()')[0] if entry_elem.xpath('./JOCKEY/KEY/text()') else None
                    train_key = entry_elem.xpath('./TRAINER/KEY/text()')[0] if entry_elem.xpath('./TRAINER/KEY/text()') else None

                    # Insert the entry data into the database
                    insert_results_entries_query = """
                        INSERT INTO results_entries (
                            race_identifier, course_cd, horse_name, breed, last_pp, weight, age, sex, meds, equip,
                            dollar_odds, program_num, post_pos, claim_price, start_position, official_fin, finish_time,
                            speed_rating, owner, comment, winners_details, win_payoff, place_payoff,
                            show_payoff, show_payoff2, axciskey, point_of_call, jock_key, train_key
                        ) VALUES (%s, %s, %s, %s, %s,
                                  %s, %s, %s, %s, %s, 
                                  %s, %s, %s, %s, %s, 
                                  %s, %s, %s, %s, %s, 
                                  %s, %s, %s, %s, %s, 
                                  %s, %s, %s, %s)
                        ON CONFLICT (race_identifier, program_num ) DO UPDATE 
                    SET course_cd = EXCLUDED.course_cd, 
                        horse_name = EXCLUDED.horse_name,
                        breed = EXCLUDED.breed, 
                        last_pp = EXCLUDED.last_pp,
                        weight = EXCLUDED.weight, 
                        age = EXCLUDED.age, 
                        sex = EXCLUDED.sex, 
                        meds = EXCLUDED.meds, 
                        equip = EXCLUDED.equip,
                        dollar_odds = EXCLUDED.dollar_odds, 
                        post_pos = EXCLUDED.post_pos,
                        claim_price = EXCLUDED.claim_price, 
                        start_position = EXCLUDED.start_position, 
                        official_fin = EXCLUDED.official_fin, 
                        finish_time = EXCLUDED.finish_time,
                        speed_rating = EXCLUDED.speed_rating, 
                        owner = EXCLUDED.owner, 
                        comment = EXCLUDED.comment, 
                        winners_details = EXCLUDED.winners_details, 
                        win_payoff = EXCLUDED.win_payoff, 
                        place_payoff = EXCLUDED.place_payoff,
                        show_payoff = EXCLUDED.show_payoff, 
                        show_payoff2 = EXCLUDED.show_payoff2, 
                        axciskey = EXCLUDED.axciskey, 
                        point_of_call = EXCLUDED.point_of_call, 
                        jock_key = EXCLUDED.jock_key, 
                        train_key = EXCLUDED.train_key
                    """
                    cursor.execute(insert_results_entries_query, (
                        race_identifier, course_cd, horse_name, breed, last_pp_json, weight, age, sex, meds, equip,
                        dollar_odds, program_num, post_pos, claim_price, start_position, official_fin, finish_time,
                        speed_rating, owner, comment, winners_details, win_payoff, place_payoff,
                        show_payoff, show_payoff2, axciskey, point_of_call_json, jock_key, train_key
                    ))
                except Exception as results_entries_error:
                    has_rejections = True
                    logging.error(f"Error processing file: {xml_file} race {race_number}: {results_entries_error}")
                    # Prepare and log rejected record
                    rejected_record = {
                        "race_identifier": race_identifier,
                        "course_cd": course_cd,
                        "horse_name": horse_name,
                        "breed": breed,
                        "last_pp_json": last_pp_json,
                        "weight": weight,
                        "age": age,
                        "sex": sex,
                        "meds": meds,
                        "equip": equip,
                        "dollar_odds": dollar_odds,
                        "program_num": program_num,
                        "post_pos": post_pos,
                        "claim_price": claim_price,
                        "start_position": start_position,
                        "official_fin": official_fin,
                        "finish_time": finish_time,
                        "speed_rating": speed_rating,
                        "owner": owner,
                        "comment": comment,
                        "winners_details": winners_details,
                        "win_payoff": win_payoff,
                        "place_payoff": place_payoff,
                        "show_payoff": show_payoff,
                        "show_payoff2": show_payoff2,
                        "axciskey": axciskey,
                        "point_of_call_json": point_of_call_json,
                        "jock_key": jock_key,
                        "train_key": train_key
                    }
                    conn.rollback()  # Rollback the transaction before logging the rejected record
                    log_rejected_record(conn, 'results_entries', rejected_record, str(race_error))
                    continue  # Skip to the next race record

            conn.commit()
            return "processed_with_rejections" if has_rejections else "processed"

    except Exception as e:
        logging.error(f"Error processing results entries data in file {xml_file}: {e}")
        conn.rollback()
        return "error"