import xml.etree.ElementTree as ET
import logging
from src.data_ingestion.ingestion_utils import (
    validate_xml, get_text, safe_int, parse_date,
    log_rejected_record, parse_time, update_ingestion_status
)
from datetime import datetime
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd

def process_workoutdata_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the workoutdata table.
    Validates the XML against the provided XSD schema and updates ingestion status.
    """
    # Validate the XML file first
    if not validate_xml(xml_file, xsd_file_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        update_ingestion_status(conn, xml_file, "error", "workoutdata")  # Update status in ingestion_files
        return  # Skip processing this file
    has_rejections = False  # Track if any records were rejected

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        # Iterate over each race data
        for race in root.findall('racedata'):
            try:
                # Extract course code
                course_code = get_text(race.find('track'), 'Unknown')

                if course_code and course_code != 'Unknown':
                    mapped_course_cd = eqb_tpd_codes_to_course_cd.get(course_code)
                    if mapped_course_cd and len(mapped_course_cd) == 3:
                        course_cd = mapped_course_cd
                    else:
                        logging.error(f"Course code '{course_code}' not found in mapping dictionary or mapped course code is not three characters -- defaulting to 'UNK'.")
                        continue  # Skip
                else:
                    logging.error("Course code not found in XML or is 'Unknown' -- defaulting to 'UNK'.")
                    continue  # Skip

                race_date = parse_date(get_text(race.find('race_date')))
                default_time = datetime(1970, 1, 1).time()

                # Parse the post_time or use the default
                post_time_text = get_text(race.find('post_time'))
                post_time = parse_time(post_time_text) if post_time_text else default_time
                race_number = safe_int(get_text(race.find('race')))

                for horse in race.findall('horsedata'):
                    # Directly retrieve program_number as string to allow for alphanumeric values
                    axciskey = get_text(horse.find('axciskey'))
                    saddle_cloth_number = get_text(horse.find('program'))
                    if saddle_cloth_number is None:
                        logging.warning(f"Missing program_number for horse in race {race_number} from file {xml_file}. Assigning default saddle_cloth_number of '0'.")
                        saddle_cloth_number = '0'  # Assign default value

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
                                ON CONFLICT (course_cd, race_date, race_number, saddle_cloth_number, worknum) DO UPDATE 
                                SET post_time = EXCLUDED.post_time,
                                    days_back = EXCLUDED.days_back,
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
                                conn.commit()  # Commit the transaction
                            except Exception as workout_error:
                                has_rejections = True
                                logging.error(f"Error processing workoutdata for race {race_number}: {workout_error}")
                                # Prepare rejected record data
                                rejected_record_data = {
                                    "course_cd": course_cd,
                                    "race_date": race_date.isoformat() if race_date else None,
                                    "post_time": post_time.isoformat() if post_time else None,
                                    "race_number": race_number,
                                    "saddle_cloth_number": saddle_cloth_number,
                                    "worknum": worknum,
                                    "days_back": days_back,
                                    "worktext": worktext,
                                    "ranking": ranking,
                                    "rank_group": rank_group
                                }

                                log_rejected_record(conn, 'workoutdata', rejected_record_data, str(workout_error))
                                continue  # Skip to the next workoutdata item

                        else:
                            logging.warning(f"No workoutdata found for horse {axciskey} in file {xml_file}.")
                            continue  # Skip to the next horse if no workout data is found

            except Exception as e:
                has_rejections = True
                conn.rollback()  # Rollback the transaction before logging the rejected record
                logging.error(f"Error processing race {race_number} in file {xml_file}: {e}")
                rejected_record = {
                    "course_cd": course_cd,
                    "race_date": race_date.isoformat() if race_date else None,
                    "post_time": post_time.isoformat() if post_time else None,
                    "race_number": race_number,
                }
                log_rejected_record(conn, 'workoutdata', rejected_record, str(e))
                continue  # Skip to the next race record
        
        return not has_rejections  # Returns True if no rejections, otherwise False

    except Exception as e:
        logging.error(f"Critical error processing workout data file {xml_file}: {e}")
        conn.rollback()  # Rollback transaction if an error occurred
        update_ingestion_status(conn, xml_file, "error", "workoutdata")
        return False