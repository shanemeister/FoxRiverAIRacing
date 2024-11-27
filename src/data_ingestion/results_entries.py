import xml.etree.ElementTree as ET
import json
import logging
from src.data_ingestion.ingestion_utils import (
    validate_xml, get_text, parse_time, parse_date, safe_numeric_int, safe_numeric_float,
    log_rejected_record, update_ingestion_status, convert_last_pp_to_json, convert_point_of_call_to_json, parse_finish_time
)
from datetime import datetime
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd

def process_results_entries_file(xml_file, conn, cursor, xsd_schema_path):
    """
    Process individual XML race results data file and insert into the results_entries table.
    Validates the XML against the provided XSD schema.
    """
    # Validate the XML file first
    if not validate_xml(xml_file, xsd_schema_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        update_ingestion_status(conn, xml_file, "error", "validation error on results_entries")  # Record error status
        return False

    has_rejections = False  # Track if any records were rejected
    rejected_record = {}  # Store rejected records for logging
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        course_cd = eqb_tpd_codes_to_course_cd.get(root.find('./TRACK/CODE').text, 'EQE')
        course_name = root.find('./TRACK/NAME').text
        race_date = parse_date(root.get("RACE_DATE"))  # Get race date directly from attribute

        # Iterate over each RACE element
        for race_elem in root.findall('RACE'):
                try:
                    race_number = safe_numeric_int(race_elem.get("NUMBER"), "race_number")
                    post_time = parse_time(get_text(race_elem.find('POST_TIME'))) or datetime.strptime("00:00", "%H:%M").time()

                    # Iterate over each ENTRY element within this RACE element
                    for entry_elem in race_elem.findall('ENTRY'):
                        try:
                            horse_name = get_text(entry_elem.find('NAME')) if entry_elem.find('NAME') is not None else None
                            breed = get_text(entry_elem.find('BREED')) if entry_elem.find('BREED') is not None else None
                            last_pp_elem = entry_elem.find('LAST_PP')
                            last_pp = convert_last_pp_to_json(last_pp_elem) if last_pp_elem is not None else None
                            weight = safe_numeric_int(get_text(entry_elem.find('WEIGHT')), 'weight') if entry_elem.find('WEIGHT') is not None else None
                            age = safe_numeric_int(get_text(entry_elem.find('AGE')), 'age') if entry_elem.find('AGE') is not None else None
                            sex_code = get_text(entry_elem.find('SEX/CODE')) if entry_elem.find('SEX/CODE') is not None else None
                            sex_description = get_text(entry_elem.find('SEX/DESCRIPTION')) if entry_elem.find('SEX/DESCRIPTION') is not None else None
                            sex = sex_code
                            meds = get_text(entry_elem.find('MEDS')) if entry_elem.find('MEDS') is not None else None
                            equip = get_text(entry_elem.find('EQUIP')) if entry_elem.find('EQUIP') is not None else None
                            dollar_odds = safe_numeric_float(get_text(entry_elem.find('DOLLAR_ODDS')), 'dollar_odds') if entry_elem.find('DOLLAR_ODDS') is not None else None

                            # Assign a program number, incrementing if the original is missing
                            program_num = get_text(entry_elem.find('PROGRAM_NUM'))
                                
                            post_pos = safe_numeric_int(get_text(entry_elem.find('POST_POS')), 'post_pos') if entry_elem.find('POST_POS') is not None else None
                            claim_price = safe_numeric_float(get_text(entry_elem.find('CLAIM_PRICE')), 'claim_price') if entry_elem.find('CLAIM_PRICE') is not None else None,
                            start_position = safe_numeric_int(get_text(entry_elem.find('START_POSITION')), 'start_position') if entry_elem.find('START_POSITION') is not None else None,
                            official_fin = safe_numeric_int(get_text(entry_elem.find('OFFICIAL_FIN')), 'official_fin') if entry_elem.find('OFFICIAL_FIN') is not None else None,
                            finish_time_str = get_text(entry_elem.find('FINISH_TIME'))
                            finish_time = parse_finish_time(finish_time_str) if finish_time_str else None
                            speed_rating = safe_numeric_int(get_text(entry_elem.find('SPEED_RATING')), 'speed_rating') if entry_elem.find('SPEED_RATING') is not None else None,
                            owner = get_text(entry_elem.find('OWNER')) if entry_elem.find('OWNER') is not None else None,
                            comment = get_text(entry_elem.find('COMMENT')) if entry_elem.find('COMMENT') is not None else None,
                            winners_details = get_text(entry_elem.find('WINNERS_DETAILS')) if entry_elem.find('WINNERS_DETAILS') is not None else None,
                            win_payoff = safe_numeric_float(get_text(entry_elem.find('WIN_PAYOFF')), 'win_payoff') if entry_elem.find('WIN_PAYOFF') is not None else None,
                            place_payoff = safe_numeric_float(get_text(entry_elem.find('PLACE_PAYOFF')), 'place_payoff') if entry_elem.find('PLACE_PAYOFF') is not None else None,
                            show_payoff = safe_numeric_float(get_text(entry_elem.find('SHOW_PAYOFF')), 'show_payoff') if entry_elem.find('SHOW_PAYOFF') is not None else None,
                            show_payoff2 = safe_numeric_float(get_text(entry_elem.find('SHOW_PAYOFF2')), 'show_payoff2') if entry_elem.find('SHOW_PAYOFF2') is not None else None,
                            axciskey = get_text(entry_elem.find('AXCISKEY')) if entry_elem.find('AXCISKEY') is not None else None,
                            point_of_call_elem = entry_elem.find('POINT_OF_CALL')
                            point_of_call = convert_point_of_call_to_json(point_of_call_elem) if point_of_call_elem is not None else None
                            # Extract jock_key from JOCKEY tag
                            jockey_elem = entry_elem.find('JOCKEY')
                            jock_key = jockey_elem.find('KEY').text if jockey_elem is not None and jockey_elem.find('KEY') is not None else None

                            # Extract train_key from TRAINER tag (assuming similar structure)
                            trainer_elem = entry_elem.find('TRAINER')
                            train_key = trainer_elem.find('KEY').text if trainer_elem is not None and trainer_elem.find('KEY') is not None else None
                            # Insert entry data into the table
                            insert_entry_query = """
                                INSERT INTO public.results_entries (
                                    course_cd, race_date, post_time, race_number, program_num, 
                                    horse_name, breed, last_pp, weight, age, 
                                    sex_code, sex_description, meds, equip, dollar_odds, 
                                    post_pos, claim_price, start_position, official_fin, finish_time, 
                                    speed_rating, owner, comment, winners_details, win_payoff,
                                    place_payoff, show_payoff, show_payoff2, axciskey, point_of_call, 
                                    jock_key, train_key
                                ) VALUES (%s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s, %s, 
                                        %s, %s)
                                ON CONFLICT (course_cd, race_date, race_number, program_num) DO UPDATE 
                                    SET post_time = EXCLUDED.post_time,
                                        horse_name = EXCLUDED.horse_name,
                                        breed = EXCLUDED.breed,
                                        last_pp = EXCLUDED.last_pp,
                                        weight = EXCLUDED.weight,
                                        age = EXCLUDED.age,
                                        sex_code = EXCLUDED.sex_code,
                                        sex_description = EXCLUDED.sex_description,
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
                            cursor.execute(insert_entry_query, (
                                course_cd, race_date, post_time, race_number, program_num, 
                                horse_name, breed, last_pp, weight, age, 
                                sex_code, sex_description, meds, equip, dollar_odds, 
                                post_pos, claim_price, start_position, official_fin, finish_time, 
                                speed_rating, owner, comment, winners_details, win_payoff,
                                place_payoff, show_payoff, show_payoff2, axciskey, point_of_call, 
                                jock_key, train_key
                                ))
                            conn.commit()  # Commit after successful insertion
                            logging.info(f"Inserted entry for result_entries: {course_cd}")    
                        except Exception as entries_error:
                            has_rejections = True
                            logging.error(f"Error processing race {race_number}: {entries_error}")
                            # Prepare rejected record
                            rejected_record = {
                                "course_cd": course_cd,
                                "race_date": race_date.isoformat() if race_date else None,
                                "post_time": post_time.isoformat() if post_time else None,
                                "race_number": race_number,
                                "program_num": program_num,
                                "horse_name": horse_name,
                                "breed": breed,
                                "last_pp": last_pp,
                                "weight": weight,
                                "age": age,
                                "sex_code": sex_code,
                                "sex_description": sex_description,
                                "meds": meds,
                                "equip": equip,
                                "dollar_odds": dollar_odds,
                                "post_pos": post_pos,
                                "claim_price": claim_price,
                                "start_position": start_position,
                                "official_fin": official_fin,
                                "finish_time": finish_time.isoformat() if finish_time else None, 
                                "speed_rating": speed_rating,
                                "owner": owner,
                                "comment": comment,
                                "winners_details": winners_details,
                                "win_payoff": win_payoff,
                                "place_payoff": place_payoff,
                                "show_payoff": show_payoff,
                                "show_payoff2": show_payoff2,
                                "axciskey": axciskey,
                                "point_of_call": point_of_call,
                                "jock_key": jock_key,
                                "train_key": train_key
                            }
                            conn.rollback()
                            log_rejected_record(conn, 'results_entries', rejected_record, str(entries_error))
                            # continue  # Skip to the next race entry after logging the error
                        try:
                            insert_horse_query = """
                            INSERT INTO public.horse(
                                axciskey, horse_name, sex
                            ) VALUES (%s, %s, %s)
                            ON CONFLICT (axciskey) DO UPDATE 
                                SET horse_name = EXCLUDED.horse_name,
                                    sex = EXCLUDED.sex
                            """     
                            cursor.execute(insert_horse_query, (
                                axciskey, horse_name, sex
                                ))
                            conn.commit()  # Commit after successful insertion
                            logging.info(f"Inserted entry for horse: {axciskey}")
                        except Exception as horse_error:
                            has_rejections = True
                            logging.error(f"Error processing horse {axciskey}: {horse_error}")
                            # Prepare rejected record
                            rejected_record = {
                                "axciskey": axciskey,
                                "horse_name": horse_name}
                            conn.rollback()
                            log_rejected_record(conn, 'horse', rejected_record, str(horse_error))
                            continue  # Skip to the next race entry after logging the error
                except Exception as e:
                    has_rejections = True
                    logging.error(f"Error processing results_entries before cursor insert: file: {xml_file}, error: {e}")

        return not has_rejections  # Returns True if no rejections, otherwise False

    except Exception as e:
        logging.error(f"Critical error processing horse data file {xml_file}: {e}")
        return False