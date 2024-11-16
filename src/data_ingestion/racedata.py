import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from src.data_ingestion.ingestion_utils import (
    validate_xml, safe_float, log_rejected_record, clean_attribute, 
    parse_date, parse_time, get_text, safe_int, update_ingestion_status
)
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd

def process_racedata_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the racedata table.
    Validates the XML against the provided XSD schema and updates ingestion status.
    """
    # Validate the XML file first
    if not validate_xml(xml_file, xsd_file_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        update_ingestion_status(conn, xml_file, "error", "racedata")  # Record error status
        return "error"
    
    #logging.info(f"################## Processing racedata file: {xml_file} ##################")
    has_rejections = False  # Track if any records were rejected
    rejected_record = {}
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        for race in root.findall('racedata'):
            try:
                # Extract key details safely with default values if missing
                country = get_text(race.find('country'))
                course_cd = eqb_tpd_codes_to_course_cd.get(get_text(race.find('track'), 'Unknown'))
                race_date = parse_date(get_text(race.find('race_date')))
                default_time = datetime(1970, 1, 1).time()

                # Parse post_time or use default
                post_time_element = race.find('post_time')
                post_time = parse_time(get_text(post_time_element)) if post_time_element is not None else default_time

                # Extract race-specific attributes
                race_number = safe_int(get_text(race.find('race')))
                todays_cls = get_text(race.find('todays_cls'))
                distance = safe_float(get_text(race.find('distance')))
                dist_unit = get_text(race.find('dist_unit'))
                surface_type_code = clean_attribute(get_text(race.find('course_id')))
                surface = get_text(race.find('surface'))
                stkorclm = get_text(race.find('stkorclm'))
                purse = safe_float(get_text(race.find('purse')))
                claimant = safe_float(get_text(race.find('claimant')))
                age_restr = get_text(race.find('age_restr'))
                bet_opt = get_text(race.find('bet_opt'))
                raceord = safe_int(get_text(race.find('raceord')))
                partim = safe_float(get_text(race.find('partim')))
                dist_disp = get_text(race.find('dist_disp'))
                breed_cd = get_text(race.find('breed_type'))
                race_text = get_text(race.find('race_text'))
                stk_clm_md = get_text(race.find('stk_clm_md'))

                # SQL insert query for the racedata table
                insert_query = """
                    INSERT INTO racedata (country, post_time, course_cd, race_number, todays_cls, 
                                        distance, dist_unit, surface_type_code, surface, 
                                        stkorclm, purse, claimant, age_restr, 
                                        race_date, race_text, bet_opt, 
                                        raceord, partim, dist_disp, breed_cd, 
                                        stk_clm_md)
                    VALUES (%s, %s, %s, %s, %s, 
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
                try:
                    cursor.execute(insert_query, (
                        country, post_time, course_cd, race_number, todays_cls, 
                        distance, dist_unit, surface_type_code, surface, 
                        stkorclm, purse, claimant, age_restr, 
                        race_date, race_text, bet_opt, 
                        raceord, partim, dist_disp, breed_cd, 
                        stk_clm_md
                    ))
                    conn.commit()
                    #logging.info(f"******************* Inserted racedata *******************")
                    #update_ingestion_status(conn, xml_file, "processed", "racedata")  # Record processed status
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
                        "claimant": claimant,
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
                    update_ingestion_status(conn, xml_file, "error", "racedata")
                    logging.error(f"*********************** Error processing racedata on file {xml_file}: {race_error} ***********************")
                    continue  # Skip to the next race after logging the error

            except Exception as e:
                has_rejections = True
                logging.error(f"*************** Critical error processing racedata file {xml_file}: {e} ***************")
                log_rejected_record(conn, 'horse_data', rejected_record, str(e))
                continue  # Skip to the next race record

        return not has_rejections  # Returns True if no rejections, otherwise False

    except Exception as e:
        logging.error(f"******************* Critical error processing racedata file {xml_file}: {e} *******************")
        logging.error(f"Critical error processing horse data file {xml_file}: {e}")
        return False