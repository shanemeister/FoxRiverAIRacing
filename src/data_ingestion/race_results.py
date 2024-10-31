import xml.etree.ElementTree as ET
import json
import logging
from ingestion_utils import (
    validate_xml, get_text, parse_time, parse_date, safe_int, safe_float,
    gen_race_identifier, log_rejected_record, parse_claims
)
from datetime import datetime

def process_raceresults_file(xml_file, conn, cursor, xsd_schema_path):
    """
    Process individual XML race results data file and insert into the race results table.
    Validates the XML against the provided XSD schema.
    """
    # Validate the XML file first
    if not validate_xml(xml_file, xsd_schema_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        return "error"  # Skip processing this file
    
    has_rejections = False  # Track if any records were rejected
    
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Extract course_cd and course_name
        course_cd = root.find('./TRACK/CODE').text
        course_name = root.find('./TRACK/NAME').text
        race_date = parse_date(root.get("RACE_DATE"))  # Get race date directly from attribute

        # Iterate over each RACE element to extract race-specific details
        for race_elem in root.findall('RACE'):
            race_number = safe_int(race_elem.get("NUMBER"))  # Get race number from attribute
            race_identifier = gen_race_identifier(course_cd, race_date, race_number)

            # Extract race data fields
            card_id = get_text(race_elem.find('CARD_ID'))
            type = get_text(race_elem.find('TYPE'))
            purse = safe_int(get_text(race_elem.find('PURSE')))
            race_text = get_text(race_elem.find('race_text'))
            age_restr_cd = get_text(race_elem.find('AGE_RESTRICTIONS'))
            distance = safe_int(get_text(race_elem.find('DISTANCE')))
            dist_unit = get_text(race_elem.find('DIST_UNIT'))
            about_dist_flag = get_text(race_elem.find('ABOUT_DIST_FLAG'))
            course_id = get_text(race_elem.find('COURSE_ID'))
            course_desc = get_text(race_elem.find('COURSE_DESC'))
            surface = get_text(race_elem.find('SURFACE'))
            class_rating = safe_int(get_text(race_elem.find('CLASS_RATING')))
            trk_cond = get_text(race_elem.find('TRK_COND'))
            weather = get_text(race_elem.find('WEATHER'))
            strt_desc = get_text(race_elem.find('STRT_DESC'))
            post_time = parse_time(get_text(race_elem.find('POST_TIME')))
            dtv = safe_int(get_text(race_elem.find('DTV')))
            fraction_1 = get_text(race_elem.find('FRACTION_1'))
            fraction_2 = get_text(race_elem.find('FRACTION_2'))
            fraction_3 = get_text(race_elem.find('FRACTION_3'))
            fraction_4 = get_text(race_elem.find('FRACTION_4'))
            fraction_5 = get_text(race_elem.find('FRACTION_5'))
            win_time = safe_float(get_text(race_elem.find('WIN_TIME')))
            pace_call1 = safe_int(get_text(race_elem.find('PACE_CALL1')))
            pace_call2 = safe_int(get_text(race_elem.find('PACE_CALL2')))
            pace_final = safe_int(get_text(race_elem.find('PACE_FINAL')))
            par_time = safe_float(get_text(race_elem.find('PAR_TIME')))

            # Convert claims to JSON format
            claims = race_elem.find('CLAIMED')
            claimed = parse_claims(claims) if claims is not None and len(claims) > 0 else None
            claimed_json = json.dumps(claimed) if claimed is not None else None  # Convert to JSON for DB insertion


            voided_claims = get_text(race_elem.find('VOIDED_CLAIMS'))
            wind_direction = get_text(race_elem.find('WIND_DIRECTION'))
            wind_speed = safe_int(get_text(race_elem.find('WIND_SPEED')))
            runupdist = safe_int(get_text(race_elem.find('RUNUPDIST')))
            raildist = safe_int(get_text(race_elem.find('RAILDIST')))
            sealed = get_text(race_elem.find('SEALED'))
            wps_pool = safe_float(get_text(race_elem.find('WPS_POOL')))
            footnotes = get_text(race_elem.find('FOOTNOTES'))

            # Insert query for race results
            insert_race_results_query = """
                INSERT INTO race_results (
                    race_identifier, course_cd, race_date, race_number, course_name,
                    card_id, type, purse, race_text, age_restr_cd, 
                    distance, dist_unit, about_dist_flag, course_id, course_desc,
                    surface, class_rating, trk_cond, weather, strt_desc, 
                    post_time, dtv, fraction_1, fraction_2, fraction_3, 
                    fraction_4, fraction_5, win_time, pace_call1, pace_call2,
                    pace_final, par_time, claimed, voided_claims, wind_direction, 
                    wind_speed, runupdist, raildist, sealed, wps_pool, 
                    footnotes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s)
                ON CONFLICT (race_identifier) DO UPDATE 
                SET course_cd = EXCLUDED.course_cd,
                    race_date = EXCLUDED.race_date,
                    race_number = EXCLUDED.race_number,
                    course_name = EXCLUDED.course_name,
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
                    post_time = EXCLUDED.post_time,
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
                    race_identifier, course_cd, race_date, race_number, course_name,
                    card_id, type, purse, race_text, age_restr_cd, 
                    distance, dist_unit, about_dist_flag, course_id, course_desc,
                    surface, class_rating, trk_cond, weather, strt_desc, 
                    post_time, dtv, fraction_1, fraction_2, fraction_3, 
                    fraction_4, fraction_5, win_time, pace_call1, pace_call2,
                    pace_final, par_time, claimed_json, voided_claims, wind_direction, 
                    wind_speed, runupdist, raildist, sealed, wps_pool, 
                    footnotes
                ))
            except Exception as race_error:
                has_rejections = True
                logging.error(f"Error processing race {race_number}: {race_error}")
                # Prepare and log rejected record
                rejected_record = {
                    "race_identifier": race_identifier,
                    "course_cd": course_cd,
                    "race_date": race_date.isoformat(),
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
                    "post_time": post_time.isoformat() if post_time else None,
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
                    "claimed": json.loads(claimed_json) if claimed_json else None,  # Load JSON for rejected record logging
                    "voided_claims": voided_claims,
                    "wind_direction": wind_direction,
                    "wind_speed": wind_speed,
                    "runupdist": runupdist,
                    "raildist": raildist,
                    "sealed": sealed,
                    "wps_pool": wps_pool,
                    "footnotes": footnotes
                }
                conn.rollback()  # Rollback the transaction before logging the rejected record
                log_rejected_record(conn, 'race_results', rejected_record, str(race_error))
                continue  # Skip to the next race record

        conn.commit()
        return "processed_with_rejections" if has_rejections else "processed"

    except Exception as e:
        logging.error(f"Error processing race results data in file {xml_file}: {e}")
        conn.rollback()
        return "error"