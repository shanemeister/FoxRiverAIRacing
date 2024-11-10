import xml.etree.ElementTree as ET
import json
import logging
from ingestion_utils import (
    validate_xml, get_text, parse_time, parse_date,
    log_rejected_record, parse_claims, update_ingestion_status, safe_numeric_int, safe_numeric_float
)
from datetime import datetime
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd

def process_raceresults_file(xml_file, conn, cursor, xsd_schema_path):
    """
    Process individual XML race results data file and insert into the race results table.
    Validates the XML against the provided XSD schema and tracks ingestion status.
    """
    # Validate the XML file first
    if not validate_xml(xml_file, xsd_schema_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        update_ingestion_status(conn, xml_file, "error")  # Record error status
        return False

    has_rejections = False  # Track if any records were rejected
    rejected_record = {}  # Store rejected records for logging

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        # Initialize variables for logging in case of an exception
        course_cd = course_name = race_date = race_number = None

        # Extract course_cd and course_name
        course_cd = eqb_tpd_codes_to_course_cd.get(root.find('./TRACK/CODE').text, 'EQE')
        course_name = root.find('./TRACK/NAME').text
        race_date = parse_date(root.get("RACE_DATE"))  # Get race date directly from attribute
        # Iterate over each RACE element to extract race-specific details
        for race_elem in root.findall('RACE'):
            try:
                # Corrected call with field name specified
                race_number = safe_numeric_int(race_elem.get("NUMBER"), "race_number")  # Get race number from attribute        
                
                 # Initialize claimed to None at the start of each race processing iteration
                claimed = None
                
                # Convert claims to JSON format if claims exist
                claims = race_elem.find('CLAIMED')
                claimed_json = parse_claims(claims) if claims is not None and len(claims) > 0 else None
                claimed = json.dumps(claimed_json) if claimed_json is not None else None
                
                # Extract race data fields
                card_id = get_text(race_elem.find('CARD_ID'))
                type = get_text(race_elem.find('TYPE'))
                purse = safe_numeric_int(get_text(race_elem.find('PURSE')), 'Purse')
                race_text = get_text(race_elem.find('race_text'))
                age_restr_cd = get_text(race_elem.find('AGE_RESTRICTIONS'))
                distance = safe_numeric_int(get_text(race_elem.find('DISTANCE')), 'Distance')
                dist_unit = get_text(race_elem.find('DIST_UNIT'))
                about_dist_flag = get_text(race_elem.find('ABOUT_DIST_FLAG'))
                course_id = get_text(race_elem.find('COURSE_ID'))
                course_desc = get_text(race_elem.find('COURSE_DESC'))
                surface = get_text(race_elem.find('SURFACE'))
                class_rating = safe_numeric_int(get_text(race_elem.find('CLASS_RATING')), 'Class Rating')
                trk_cond = get_text(race_elem.find('TRK_COND'))
                weather = get_text(race_elem.find('WEATHER'))
                strt_desc = get_text(race_elem.find('STRT_DESC'))
                post_time = parse_time(get_text(race_elem.find('POST_TIME'))) or datetime.strptime("00:00", "%H:%M").time()
                dtv = safe_numeric_int(get_text(race_elem.find('DTV')), 'DTV')
                fraction_1 = get_text(race_elem.find('FRACTION_1'))
                fraction_2 = get_text(race_elem.find('FRACTION_2'))
                fraction_3 = get_text(race_elem.find('FRACTION_3'))
                fraction_4 = get_text(race_elem.find('FRACTION_4'))
                fraction_5 = get_text(race_elem.find('FRACTION_5'))
                win_time = safe_numeric_float(get_text(race_elem.find('WIN_TIME')), 'Win Time')
                pace_call1 = safe_numeric_int(get_text(race_elem.find('PACE_CALL1')), 'Pace Call 1')
                pace_call2 = safe_numeric_int(get_text(race_elem.find('PACE_CALL2')), 'Pace Call 2')
                pace_final = safe_numeric_int(get_text(race_elem.find('PACE_FINAL')), 'Pace Final')
                par_time = safe_numeric_float(get_text(race_elem.find('PAR_TIME')), 'Par Time')
                # Parse earning_splits
                earning_splits_temp = {
                    f"SPLIT_{i}": safe_numeric_float(get_text(split), f"SPLIT_{i}")
                    for i, split in enumerate(race_elem.findall('EARNING_SPLITS/SPLIT'), start=1)
                    if get_text(split)  # Ensure there's a value to process
                }
                # Convert earning_splits to JSON if there are valid entries, otherwise set to None
                earning_splits = json.dumps(earning_splits_temp) if earning_splits_temp else None

                # Remaining fields
                voided_claims = get_text(race_elem.find('VOIDED_CLAIMS'))
                wind_direction = get_text(race_elem.find('WIND_DIRECTION'))
                wind_speed = safe_numeric_int(get_text(race_elem.find('WIND_SPEED')), 'Wind Speed')
                runupdist = safe_numeric_int(get_text(race_elem.find('RUNUPDIST')), 'Runup Distance')
                raildist = safe_numeric_int(get_text(race_elem.find('RAILDIST')), 'Rail Distance')
                sealed = get_text(race_elem.find('SEALED'))
                wps_pool = safe_numeric_float(get_text(race_elem.find('WPS_POOL')), 'WPS Pool')
                footnotes = get_text(race_elem.find('FOOTNOTES'))

                # Insert query for race results
                insert_race_results_query = """
                    INSERT INTO public.race_results (
                        course_cd, race_date, post_time, race_number, course_name,
                        card_id, type, purse, race_text, age_restr_cd, 
                        distance, dist_unit, about_dist_flag, course_id, course_desc,
                        surface, class_rating, trk_cond, weather, strt_desc, 
                        dtv, fraction_1, fraction_2, fraction_3, fraction_4, 
                        fraction_5, win_time, pace_call1, pace_call2, pace_final, 
                        par_time, earning_splits, claimed, voided_claims, wind_direction, wind_speed, 
                        runupdist, raildist, sealed, wps_pool, footnotes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (course_cd, race_date, post_time, race_number) DO UPDATE 
                    SET course_name = EXCLUDED.course_name,
                        card_id = EXCLUDED.card_id,
                        type = EXCLUDED.type,
                        purse = EXCLUDED.purse,
                        race_text = EXCLUDED.race_text,
                        age_restr_cd = EXCLUDED.age_restr_cd,
                        distance = EXCLUDED.distance,
                        dist_unit = EXCLUDED.dist_unit,
                        about_dist_flag = EXCLUDED.about_dist_flag,
                        course_id = EXCLUDED.course_id,
                        course_desc = EXCLUDED.course_desc,
                        surface = EXCLUDED.surface,
                        class_rating = EXCLUDED.class_rating,
                        trk_cond = EXCLUDED.trk_cond,
                        weather = EXCLUDED.weather,
                        strt_desc = EXCLUDED.strt_desc,
                        dtv = EXCLUDED.dtv,
                        fraction_1 = EXCLUDED.fraction_1,
                        fraction_2 = EXCLUDED.fraction_2,
                        fraction_3 = EXCLUDED.fraction_3,
                        fraction_4 = EXCLUDED.fraction_4,
                        fraction_5 = EXCLUDED.fraction_5,
                        win_time = EXCLUDED.win_time,
                        pace_call1 = EXCLUDED.pace_call1,
                        pace_call2 = EXCLUDED.pace_call2,
                        pace_final = EXCLUDED.pace_final,
                        par_time = EXCLUDED.par_time,
                        earning_splits = EXCLUDED.earning_splits,
                        claimed = EXCLUDED.claimed,
                        voided_claims = EXCLUDED.voided_claims,
                        wind_direction = EXCLUDED.wind_direction,
                        wind_speed = EXCLUDED.wind_speed,
                        runupdist = EXCLUDED.runupdist,
                        raildist = EXCLUDED.raildist,
                        sealed = EXCLUDED.sealed,
                        wps_pool = EXCLUDED.wps_pool,
                        footnotes = EXCLUDED.footnotes
                """
                try:
                        
                    cursor.execute(insert_race_results_query, (
                            course_cd, race_date, post_time, race_number, course_name,
                            card_id, type, purse, race_text, age_restr_cd, 
                            distance, dist_unit, about_dist_flag, course_id, course_desc,
                            surface, class_rating, trk_cond, weather, strt_desc, 
                            dtv, fraction_1, fraction_2, fraction_3, fraction_4, 
                            fraction_5, win_time, pace_call1, pace_call2, pace_final, 
                            par_time, earning_splits, claimed, voided_claims, wind_direction, wind_speed, 
                            runupdist, raildist, sealed, wps_pool, footnotes
                            ))
                    conn.commit()  # Commit after successful insertion
                except Exception as race_error:
                    has_rejections = True
                    logging.error(f"Error processing race {race_number}: {race_error}")
                    # Prepare rejected record
                    rejected_record = {
                        "course_cd": course_cd,
                        "race_date": race_date.isoformat() if race_date else None,
                        "post_time": post_time.isoformat() if post_time else None,
                        "race_number": race_number,
                        "course_name": course_name,
                        "card_id": card_id,
                        "type": type,
                        "purse": purse,
                        "race_text": race_text,
                        "age_restr_cd": age_restr_cd,
                        "distance": distance,
                        "dist_unit": dist_unit,
                        "about_dist_flag": about_dist_flag,
                        "course_id": course_id,
                        "course_desc": course_desc,
                        "surface": surface,
                        "class_rating": class_rating,
                        "trk_cond": trk_cond,
                        "weather": weather,
                        "strt_desc": strt_desc,
                        "dtv": dtv,
                        "fraction_1": fraction_1,
                        "fraction_2": fraction_2,
                        "fraction_3": fraction_3,
                        "fraction_4": fraction_4,
                        "fraction_5": fraction_5,
                        "win_time": win_time,
                        "pace_call1": pace_call1,
                        "pace_call2": pace_call2,
                        "pace_final": pace_final,
                        "par_time": par_time,
                        "earning_splits": earning_splits,
                        "claimed": claimed,
                        "voided_claims": voided_claims,
                        "wind_direction": wind_direction,
                        "wind_speed": wind_speed,
                        "runupdist": runupdist,
                        "raildist": raildist,
                        "sealed": sealed,
                        "wps_pool": wps_pool,
                        "footnotes": footnotes
                    }
                    conn.rollback()  # Rollback after error
                    log_rejected_record(conn, 'race_results', rejected_record, str(race_error))
                    continue  # Skip to the next race after logging the error

            except Exception as e:
                has_rejections = True
                logging.error(f"Critical error processing race_results before cursor insert: file: {xml_file}, error: {e}")
                continue  # Skip to the next race record
        
        return not has_rejections  # Returns True if no rejections, otherwise False

    except Exception as e:
        logging.error(f"Critical error processing horse data file {xml_file}: {e}")
        return False