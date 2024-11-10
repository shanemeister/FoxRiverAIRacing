import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from ingestion_utils import (
    validate_xml, safe_float, log_rejected_record, clean_attribute, 
    parse_date, parse_time, gen_race_identifier, safe_int, update_ingestion_status
)
from mappings_dictionaries import eqb_tpd_codes_to_course_cd

def process_racedata_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the racedata table.
    Validates the XML against the provided XSD schema and updates ingestion status.
    """
    # Validate the XML file first
    if not validate_xml(xml_file, xsd_file_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        update_ingestion_status(conn, cursor, xml_file, "error")  # Record error status
        return "error"

    has_rejections = False  # Track if any records were rejected
    rejected_record = {}
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        for race in root.findall('racedata'):
            try:
                # Extract key details for logging in case of rejection
                country = race.find('country').text
                course_cd = eqb_tpd_codes_to_course_cd.get(race.find('track').text)                
                race_date = parse_date(race.find('race_date').text)
                default_time = datetime(1970, 1, 1).time()

                # Parse post_time or use default
                post_time_element = race.find('post_time')
                post_time = parse_time(post_time_element.text) if post_time_element is not None and post_time_element.text else default_time

                # Extract race-specific attributes
                race_number = safe_int(race.find('race').text)
                todays_cls = race.find('todays_cls').text
                distance = safe_float(race.find('distance').text)
                dist_unit = race.find('dist_unit').text
                surface_type_code = clean_attribute(race.find('course_id').text)
                surface = race.find('surface').text
                stkorclm = race.find('stkorclm').text
                purse = safe_float(race.find('purse').text)
                claimamt = safe_float(race.find('claimamt').text)
                age_restr = race.find('age_restr').text
                bet_opt = race.find('bet_opt').text
                raceord = safe_int(race.find('raceord').text)
                partim = safe_float(race.find('partim').text)
                dist_disp = race.find('dist_disp').text
                breed_cd = race.find('breed_type').text
                race_text = race.find('race_text').text
                stk_clm_md = race.find('stk_clm_md').text

                # SQL insert query for the racedata table
                insert_query = """
                    INSERT INTO racedata (country, post_time, course_cd, race_number, todays_cls, 
                                        distance, dist_unit, surface_type_code, surface, 
                                        stkorclm, purse, claimamt, age_restr, 
                                        race_date, race_text, bet_opt, 
                                        raceord, partim, dist_disp, breed_cd, 
                                        stk_clm_md)
                    VALUES (%s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, 
                            %s)
                    ON CONFLICT (course_cd, race_date, post_time, race_number) DO UPDATE 
                    SET country = EXCLUDED.country,
                        todays_cls = EXCLUDED.todays_cls,
                        distance = EXCLUDED.distance,
                        dist_unit = EXCLUDED.dist_unit,
                        surface_type_code = EXCLUDED.surface_type_code,
                        surface = EXCLUDED.surface,
                        stkorclm = EXCLUDED.stkorclm,
                        purse = EXCLUDED.purse,
                        claimamt = EXCLUDED.claimamt,
                        age_restr = EXCLUDED.age_restr,
                        bet_opt = EXCLUDED.bet_opt,                        
                        raceord = EXCLUDED.raceord,
                        partim = EXCLUDED.partim,
                        dist_disp = EXCLUDED.dist_disp,
                        breed_cd = EXCLUDED.breed_cd,
                        race_text = EXCLUDED.race_text,
                        stk_clm_md = EXCLUDED.stk_clm_md
                """
                try:
                    cursor.execute(insert_query, (
                    country, post_time, course_cd, race_number, todays_cls, 
                    distance, dist_unit, surface_type_code, surface, 
                    stkorclm, purse, claimamt, age_restr, 
                    race_date, race_text, bet_opt, 
                    raceord, partim, dist_disp, breed_cd, 
                    stk_clm_md
                    ))

                except Exception as race_error:
                    has_rejections = True
                    logging.error(f"Error processing race: {race_number}, error: {race_error}")
                    # Prepare rejected record data
                    rejected_record = {
                        "country": country,
                        "course_cd": course_cd,
                        "race_date": race_date,
                        "race_number": race_number,
                        "post_time": post_time,
                        "todays_cls": todays_cls,
                        "distance": distance,
                        "dist_unit": dist_unit,
                        "surface_type_code": surface_type_code,
                        "surface": surface,
                        "stkorclm": stkorclm,
                        "purse": purse,
                        "claimamt": claimamt,
                        "age_restr": age_restr,
                        "bet_opt": bet_opt,
                        "raceord": raceord,
                        "partim": partim,
                        "dist_disp": dist_disp,
                        "breed_cd": breed_cd,
                        "race_text": race_text,
                        "stk_clm_md": stk_clm_md
                    }
                    conn.rollback()
                    log_rejected_record(conn, 'racedata', rejected_record, str(race_error))
                    continue  # Skip to the next race after logging the error

            except Exception as e:
                has_rejections = True
                log_rejected_record(conn, 'horse_data', rejected_record, str(e))
                continue  # Skip to the next race record

        return not has_rejections  # Returns True if no rejections, otherwise False

    except Exception as e:
        logging.error(f"Critical error processing horse data file {xml_file}: {e}")
        return False