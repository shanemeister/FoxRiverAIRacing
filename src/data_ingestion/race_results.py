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
    Process individual XML race results data file and insert into the race_results table.
    Validates the XML against the provided XSD schema and tracks ingestion status.
    """
    logging.info(f"Processing xml file: {xml_file}, conn: {conn}, cursor: {cursor}, xsd_schema_path: {xsd_schema_path}")
    # Validate the XML file first
    if not validate_xml(xml_file, xsd_schema_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        update_ingestion_status(conn, cursor, xml_file, "Failed Validation", 'ResultsCharts')  # Record error status
        return False

    has_rejections = False  # Track if any records were rejected

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        # Initialize variables for logging in case of an exception
        course_cd = course_name = race_date = race_number = None

        # Extract course_cd and course_name
        course_code = get_text(root.find('./TRACK/CODE'), 'Unknown')
        course_cd = eqb_tpd_codes_to_course_cd.get(course_code, 'UNK')
        course_name = get_text(root.find('./TRACK/NAME'), 'Unknown')
        race_date = parse_date(root.get("RACE_DATE"))  # Get race date directly from attribute

        # Log the starting of file processing
        logging.info(f"Processing race results file: {xml_file}, Course: {course_cd}, Date: {race_date}")

        # Iterate over each RACE element to extract race-specific details
        races = root.findall('RACE')
        logging.info(f"Found {len(races)} races in file {xml_file}")

        for race_elem in races:
            try:
                race_number = safe_numeric_int(race_elem.get("NUMBER"), "race_number")  # Get race number from attribute
                logging.info(f"Processing race number: {race_number}")

                # Initialize claimed to None at the start of each race processing iteration
                claimed = None

                # Convert claims to JSON format if claims exist
                claims_elem = race_elem.find('CLAIMED')
                if claims_elem is not None:
                    claimed_json = parse_claims(claims_elem)
                    claimed = json.dumps(claimed_json) if claimed_json else None

                # Extract race data fields
                card_id = get_text(race_elem.find('CARD_ID'))
                race_type = get_text(race_elem.find('TYPE'))
                purse = safe_numeric_int(get_text(race_elem.find('PURSE')), 'Purse')
                race_text = get_text(race_elem.find('RACE_TEXT'))
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
                earning_splits_elem = race_elem.find('EARNING_SPLITS')
                earning_splits_temp = {}
                if earning_splits_elem is not None:
                    for split_elem in earning_splits_elem:
                        split_num_str = split_elem.tag.replace('SPLIT_', '')
                        split_num = safe_numeric_int(split_num_str, 'Split Number')
                        earnings = safe_numeric_int(get_text(split_elem), f"Earnings Split {split_num}")
                        if split_num is not None and earnings is not None:
                            earning_splits_temp[f"SPLIT_{split_num}"] = earnings
                # Convert earning_splits to JSON if there are valid entries, otherwise set to None
                earning_splits = json.dumps(earning_splits_temp) if earning_splits_temp else None

                # Remaining fields
                voided_claims_elem = race_elem.find('VOIDED_CLAIMS')
                voided_claims = parse_claims(voided_claims_elem) if voided_claims_elem is not None else None
                voided_claims = json.dumps(voided_claims) if voided_claims else None

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
                        par_time, earning_splits, claimed, voided_claims, wind_direction, 
                        wind_speed, runupdist, raildist, sealed, wps_pool, 
                        footnotes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (course_cd, race_date, race_number) DO UPDATE 
                    SET course_name = EXCLUDED.course_name,
                        post_time = EXCLUDED.post_time,
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
                        card_id, race_type, purse, race_text, age_restr_cd, 
                        distance, dist_unit, about_dist_flag, course_id, course_desc,
                        surface, class_rating, trk_cond, weather, strt_desc, 
                        dtv, fraction_1, fraction_2, fraction_3, fraction_4, 
                        fraction_5, win_time, pace_call1, pace_call2, pace_final, 
                        par_time, earning_splits, claimed, voided_claims, wind_direction, 
                        wind_speed, runupdist, raildist, sealed, wps_pool, 
                        footnotes
                    ))
                    conn.commit()  # Commit after successful insertion
                    logging.info(f"Inserted race result for course {course_cd}, date {race_date}, race number {race_number}")
                except Exception as race_error:
                    has_rejections = True
                    logging.error(f"Error inserting race {race_number}: {race_error}")
                    # Prepare rejected record
                    rejected_record = {
                        "course_cd": course_cd,
                        "race_date": race_date.isoformat() if race_date else None,
                        "post_time": post_time.isoformat() if post_time else None,
                        "race_number": race_number,
                        "course_name": course_name,
                        "card_id": card_id,
                        "type": race_type,
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
                    log_rejected_record(conn, cursor, 'race_results', rejected_record, str(race_error))
                    continue  # Skip to the next race after logging the error

            except Exception as e:
                has_rejections = True
                logging.error(f"Critical error processing race {race_number}: {e}")
                rejected_record = {
                    "course_cd": course_cd,
                    "race_date": race_date.isoformat() if race_date else None,
                    "race_number": race_number
                }
                log_rejected_record(conn, cursor, 'race_results', rejected_record, str(e))
                conn.rollback()
                continue  # Skip to the next race record

        return not has_rejections  # Returns True if no rejections, otherwise False

    except Exception as e:
        logging.error(f"Critical error processing race results file {xml_file}: {e}")
        update_ingestion_status(conn, cursor, xml_file, "Error", 'ResultsCharts')
        conn.rollback()
        return False