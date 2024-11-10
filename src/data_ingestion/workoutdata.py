import xml.etree.ElementTree as ET
import logging
from ingestion_utils import validate_xml, get_text, safe_int, parse_date, gen_race_identifier, log_rejected_record, parse_time, update_ingestion_status
import json
from datetime import datetime
from mappings_dictionaries import eqb_tpd_codes_to_course_cd

def process_workoutdata_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the workoutdata table.
    Validates the XML against the provided XSD schema and updates ingestion status.
    """
    # Validate the XML file first
    if not validate_xml(xml_file, xsd_file_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        update_ingestion_status(conn, cursor, xml_file, "error")  # Update status in ingestion_files
        return  # Skip processing this file
    has_rejections = False  # Track if any records were rejected
    rejected_record = {}  # Store rejected records for logging

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        has_rejections = False  # Track if any records were rejected
        
        # Iterate over each race data
        for race in root.findall('racedata'):
            try:
                course_cd = eqb_tpd_codes_to_course_cd.get(race.find('track').text)                
                race_date = parse_date(race.find('race_date').text)
                default_time = datetime(1970, 1, 1).time()

                # Parse the post_time or use the default
                post_time_element = race.find('post_time')
                post_time = parse_time(post_time_element.text) if post_time_element is not None and post_time_element.text else default_time
                race_number = safe_int(get_text(race.find('race')))

                for horse in race.findall('horsedata'):
                    # Directly retrieve program_number as string to allow for alphanumeric values
                    axciskey = get_text(horse.find('axciskey'))
                    saddle_cloth_number = get_text(horse.find('program'))
                    if saddle_cloth_number is None:
                        logging.warning(f"Missing program_number for horse in race {race_number} from file {xml_file}. Assigning default saddle_cloth_number of 0.")
                        saddle_cloth_number = 0  # Assign default value

                    # Initialize variables for logging in case of rejection
                    worknum = days_back = worktext = ranking = rank_group = None

                    for workout_data in horse.findall('workoutdata'):
                        if workout_data is not None:
                            # Extract workout information
                            worknum = safe_int(get_text(workout_data.find('worknum')))
                            days_back = safe_int(get_text(workout_data.find('days_back')))
                            worktext = get_text(workout_data.find('worktext'))
                            ranking = safe_int(get_text(workout_data.find('ranking')))
                            rank_group = safe_int(get_text(workout_data.find('rank_group')))

                            # Prepare SQL insert query for the workoutdata table
                            insert_workout_query = """
                                INSERT INTO workoutdata (
                                    course_cd, race_date, post_time, race_number, saddle_cloth_number, 
                                    worknum, days_back, worktext, ranking, rank_group
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (course_cd, race_date, post_time, race_number, saddle_cloth_number, worknum) DO UPDATE 
                                SET days_back = EXCLUDED.days_back,
                                    worktext = EXCLUDED.worktext,
                                    ranking = EXCLUDED.ranking,
                                    rank_group = EXCLUDED.rank_group
                            """
                            try:
                                    
                                # Execute the query for workout data
                                cursor.execute(insert_workout_query, (
                                    course_cd, race_date, post_time, race_number, saddle_cloth_number, 
                                    worknum, days_back, worktext, ranking, rank_group
                                ))
                            except Exception as workout_error:
                                has_rejections = True
                                logging.error(f"Error processing workoutdata for race {race_number}: {workout_error}")
                                rejected_record_data = {
                                    "course_cd": course_cd,
                                    "race_date": race_date,
                                    "post_time": post_time,
                                    "race_number": race_number,
                                    "saddle_cloth_number": saddle_cloth_number,
                                    "worknum": worknum,
                                    "days_back": days_back,
                                    "worktext": worktext,
                                    "ranking": ranking,
                                    "rank_group": rank_group
                                }
                                # Convert dictionary to JSON for logging
                                rejected_record_json = json.dumps(rejected_record_data)

                                log_rejected_record(conn, 'workoutdata', rejected_record_json, str(workout_error))
                                continue  # Skip to the next workoutdata item

                        else:
                            logging.warning(f"No workoutdata found for horse {axciskey} in file {xml_file}.")
                            continue  # Skip to the next horse if no workout data is found
 
            except Exception as e:
                has_rejections = True
                conn.rollback()  # Rollback the transaction before logging the rejected record
                log_rejected_record(conn, 'horse_data', rejected_record, str(e))
                continue  # Skip to the next race record
        
        return not has_rejections  # Returns True if no rejections, otherwise False

    except Exception as e:
        logging.error(f"Critical error processing horse data file {xml_file}: {e}")
        conn.rollback()  # Rollback transaction if an error occurred
        return False