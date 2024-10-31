import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from ingestion_utils import validate_xml, safe_float, log_rejected_record, clean_attribute, parse_date, parse_time, gen_race_identifier, safe_int

def process_racedata_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the racedata table.
    Validates the XML against the provided XSD schema.
    """
    
    # Validate the XML file first
    if not validate_xml(xml_file, xsd_file_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        return  # Skip processing this file

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        #logging.info(f"Parsed XML successfully -- Randall Shane: {xml_file}")
        
        races = root.findall('racedata')
        #logging.info(f"Found {len(races)} races in the file: {xml_file}")

        for race in races:
            try:
                country = race.find('country').text
                course_cd = race.find('track').text
                race_date = parse_date(race.find('race_date').text)

                #logging.info("Start of race_number:")

                # Safely handle race_number conversion
                try:
                    race_number = safe_int(race.find('race').text)
                except ValueError as e:
                    logging.error(f"Error converting race_number '{race_number}' to integer: {e}")
                    continue  # Skip this race if the race_number is invalid

                # Generate the race identifier
                try:
                    race_identifier = gen_race_identifier(course_cd, race_date, race_number)
                    #logging.info(f"Race Identifier: {race_identifier} -- Randall Shane")
                except Exception as e:
                    logging.error(f"Error generating race identifier: {e}")
                    continue  # Skip this race if the race identifier cannot be generated
                post_time = parse_time(race.find('post_time').text)
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
                race_text = race.find('race_text').text  # Corrected the tag name here
                stk_clm_md = race.find('stk_clm_md').text

                # SQL insert query for the racedata table
                insert_query = """
                    INSERT INTO racedata (race_identifier, country, post_time, course_cd, race_number, todays_cls, 
                                        distance, dist_unit, surface_type_code, surface, 
                                        stkorclm, purse, claimamt, age_restr, 
                                        race_date, race_text, bet_opt, 
                                        raceord, partim, dist_disp, breed_cd, 
                                        stk_clm_md)
                    VALUES (%s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, 
                            %s, %s)
                    ON CONFLICT (race_identifier) DO UPDATE 
                    SET country = EXCLUDED.country,
                        course_cd = EXCLUDED.course_cd,
                        race_date = EXCLUDED.race_date,
                        race_number = EXCLUDED.race_number,               
                        post_time = EXCLUDED.post_time,
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
                cursor.execute(insert_query, (
                                        race_identifier, country, post_time, course_cd, race_number, todays_cls, 
                                        distance, dist_unit, surface_type_code, surface, 
                                        stkorclm, purse, claimamt, age_restr, 
                                        race_date, race_text, bet_opt, 
                                        raceord, partim, dist_disp, breed_cd, 
                                        stk_clm_md
                ))

            except Exception as race_error:
                logging.error(f"Error processing race: {race_number}, error: {race_error}")
                rejected_record = {
                    "race_identifier": race_identifier,
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
                    "race_text": race_text,
                    "stk_clm_md": stk_clm_md
                }
                log_rejected_record('racedata', rejected_record)
                continue  # Skip to the next race after logging the error

        conn.commit()
        #logging.info("Racedata records processed successfully.")

    except Exception as e:
        logging.error(f"Error processing XML file {xml_file}: {e}")
        conn.rollback()  # Rollback the transaction in case of an error