
import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from mapping_dictionaries import eqb_to_course_cd


def process_racedata_file(xml_file, conn, cursor):
    """
    Process individual XML race data file and insert into the racedata table.
    """
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        logging.info(f"Parsed XML successfully: {xml_file}")
        
        # Assuming the root has race data, adapt the following to match your XML structure
        for race in root.findall('Race'):
            # Extract and format the necessary fields from XML
            eqb_track_cd = race.find('track').text  # This is the EQB track code like 'BEL'
            # Translate the EQB track code to the corresponding course_cd in TDP
            course_cd = eqb_to_course_cd.get(eqb_track_cd, eqb_track_cd)  # Default to eqb_track_cd if no mapping exists
            race_date = datetime.strptime(race.find('race_date').text, '%Y%m%d').date()
            race_number = race.find('race').text
            post_time = datetime.strptime(race.find('post_time').text, '%I:%M%p').time()
            country = race.find('country').text
            logging.info(f"Processing race: {eqb_track_cd} on {race_date}")

            # Skip if country is Canada (CAN) and the track is EP
            if country == 'CAN' and eqb_track_cd == 'EP':
                logging.info(f"Skipping race from EP in Canada: {race.find('race').text}")
                continue

            
            # Other race fields from the XML
            
            todays_cls = race.find('todays_cls').text
            distance = race.find('distance').text
            dist_unit = race.find('dist_unit').text
            surface_type_code = race.find('course_id').text
            surface = race.find('surface').text
            stkorclm = race.find('stkorclm').text
            purse = race.find('purse').text
            claimamt = race.find('claimamt').text
            age_restr = race.find('age_restr').text
            bet_opt = race.find('bet_opt').text
            send_track = race.find('send_track').text
            race_ord = race.find('race_ord').text
            partim = race.find('partim').text
            dist_disp = race.find('dist_disp').text
            breed_type = race.find('breed_type').text
            race_text = race.find('race_text').text
            stk_clm_md = race.find('stk_clm_md').text

            # SQL insert query for the racedata table
            insert_query = """
                INSERT INTO racedata (race_id, post_time, course_cd, race_number, todays_cls, distance, dist_unit,
                                      surface_type_code, surface, stkorclm, purse, claimamt, age_restr, race_date, race_text)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (race_id) DO UPDATE 
                SET post_time = EXCLUDED.post_time,
                    race_date = EXCLUDED.race_date,
                    race_text = EXCLUDED.race_text
            """
            # Execute the query
            cursor.execute(insert_query, (
                race.find('race_id').text, post_time, course_cd, race_number, todays_cls,
                distance, dist_unit, surface_type_code, surface, stkorclm, purse, claimamt,
                age_restr, race_date, race_text
            ))
        
        # Commit the transaction
        conn.commit()
        logging.info("Race data processed successfully.")

    except Exception as e:
        # Handle exceptions and rollback in case of error
        logging.error(f"Error processing file {xml_file}: {e}")
        conn.rollback()  # Rollback the transaction in case of an error