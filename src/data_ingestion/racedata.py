import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd
from src.data_ingestion.ingestion_utils import (
    validate_xml, safe_float, log_rejected_record, clean_attribute, normalize_tags,
    parse_date, parse_time, get_text, safe_int, update_ingestion_status
)

def parse_time_with_am_pm(time_text):
    """
    Parses a time string into a Python time object, handling AM/PM format.
    
    Args:
        time_text (str): Time string to parse (e.g., '6:05PM').
        
    Returns:
        datetime.time: Parsed time object, or None if parsing fails.
    """
    try:
        return datetime.strptime(time_text.strip().upper(), "%I:%M%p").time()
    except ValueError:
        logging.error(f"Invalid time format: {time_text}")
        return None

def process_racedata_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the racedata table.
    Validates the XML against the provided XSD schema and updates ingestion status.
    
    Args:
        xml_file (str): Path to the XML race data file.
        xsd_file_path (str): Path to the XSD schema file for validation.
        conn (psycopg2.connection): Database connection object.
        cursor (psycopg2.cursor): Database cursor object.
        
    Returns:
        str: Status of the processing - "processed", "processed_with_rejections", or "error".
    """

    if not validate_xml(xml_file, xsd_file_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        update_ingestion_status(conn, xml_file, "error", "racedata")
        return "error"

    has_rejections = False  # Track if any records were rejected

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        logging.info(f"Parsed XML file: {xml_file}")
        normalized_root = normalize_tags(root)
        logging.info("Normalized XML tags to lowercase.")

        for racedata_elem in normalized_root.findall('racedata'):
            try:
                race_date_text = get_text(racedata_elem.find('race_date'))
                race_date = parse_date(race_date_text) if race_date_text else None

                if not race_date:
                    logging.error(f"Invalid or missing race_date in file {xml_file}.")
                    update_ingestion_status(conn, xml_file, "error", "racedata")
                    return "error"

                track_code_text = get_text(racedata_elem.find('track'))
                track_code = track_code_text.lower() if track_code_text else 'unk'
                course_cd = eqb_tpd_codes_to_course_cd.get(track_code.upper(), 'UNK')

                if track_code.upper() in eqb_tpd_codes_to_course_cd:
                    course_cd = eqb_tpd_codes_to_course_cd[track_code.upper()]
                    if len(course_cd) != 3:
                        logging.warning(f"Mapped course_cd '{course_cd}' for track_code '{track_code}' is not three characters. Defaulting to 'UNK'.")
                        course_cd = 'UNK'
                else:
                    logging.warning(f"Course code '{track_code}' not found in mapping dictionary. Defaulting to 'UNK'.")
                    continue

                logging.info(f"Determined course_cd: {course_cd} for track_code: {track_code}")

                race_number = safe_int(get_text(racedata_elem.find('race')))
                breed_cd = get_text(racedata_elem.find('breed_type'))
                post_time_text = get_text(racedata_elem.find('post_time'))
                post_time = parse_time_with_am_pm(post_time_text) if post_time_text else datetime.strptime("00:00", "%H:%M").time()

                todays_cls = get_text(racedata_elem.find('todays_cls'))
                distance = safe_float(get_text(racedata_elem.find('distance')))
                dist_unit = get_text(racedata_elem.find('dist_unit'))
                surface_type_code = get_text(racedata_elem.find('course_id'))
                surface = get_text(racedata_elem.find('surface'))
                stkorclm = get_text(racedata_elem.find('stkorclm'))
                purse = safe_float(get_text(racedata_elem.find('purse')))
                claimant = safe_float(get_text(racedata_elem.find('claimamt')))
                age_restr = get_text(racedata_elem.find('age_restr'))
                bet_opt = get_text(racedata_elem.find('bet_opt'))
                raceord = safe_int(get_text(racedata_elem.find('raceord')))
                partim = safe_float(get_text(racedata_elem.find('partim')))
                dist_disp = get_text(racedata_elem.find('dist_disp'))
                race_text = get_text(racedata_elem.find('race_text'))
                stk_clm_md = get_text(racedata_elem.find('stk_clm_md'))
                country = get_text(racedata_elem.find('country')) or 'USA'

                insert_query = """
                    INSERT INTO racedata (
                        country, post_time, course_cd, race_number, todays_cls, 
                        distance, dist_unit, surface_type_code, surface, 
                        stkorclm, purse, claimant, age_restr, 
                        race_date, race_text, bet_opt, 
                        raceord, partim, dist_disp, breed_cd, 
                        stk_clm_md
                    ) VALUES (%s, %s, %s, %s, %s, 
                              %s, %s, %s, %s, %s, 
                              %s, %s, %s, %s, %s, 
                              %s, %s, %s, %s, %s, 
                              %s)
                    ON CONFLICT (course_cd, race_date, race_number) DO UPDATE 
                    SET post_time = EXCLUDED.post_time,
                        country = EXCLUDED.country,
                        todays_cls = EXCLUDED.todays_cls,
                        distance = EXCLUDED.distance,
                        dist_unit = EXCLUDED.dist_unit,
                        surface_type_code = EXCLUDED.surface_type_code,
                        surface = EXCLUDED.surface,
                        stkorclm = EXCLUDED.stkorclm,
                        purse = EXCLUDED.purse,
                        claimant = EXCLUDED.claimant,
                        age_restr = EXCLUDED.age_restr,
                        bet_opt = EXCLUDED.bet_opt,                        
                        raceord = EXCLUDED.raceord,
                        partim = EXCLUDED.partim,
                        dist_disp = EXCLUDED.dist_disp,
                        breed_cd = EXCLUDED.breed_cd,
                        race_text = EXCLUDED.race_text,
                        stk_clm_md = EXCLUDED.stk_clm_md
                """
                
                cursor.execute(insert_query, (
                    country, post_time, course_cd, race_number, todays_cls, 
                    distance, dist_unit, surface_type_code, surface, 
                    stkorclm, purse, claimant, age_restr, 
                    race_date, race_text, bet_opt, 
                    raceord, partim, dist_disp, breed_cd, 
                    stk_clm_md
                ))
                conn.commit()
                logging.info(f"Successfully inserted/updated race number {race_number} on {race_date}.")
            
            except Exception as race_error:
                has_rejections = True
                logging.error(f"Error processing race number {race_number} in file {xml_file}: {race_error}")
                conn.rollback()
                log_rejected_record(conn, 'racedata', {"race_number": race_number}, str(race_error))
                update_ingestion_status(conn, xml_file, "error", "racedata")
                continue

    except Exception as e:
        logging.error(f"Critical error processing racedata file {xml_file}: {e}")
        update_ingestion_status(conn, xml_file, "error", "racedata")
        return "error"

    return "processed_with_rejections" if has_rejections else "processed"