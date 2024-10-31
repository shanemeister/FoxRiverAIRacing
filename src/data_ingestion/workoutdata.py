import xml.etree.ElementTree as ET
import logging
from ingestion_utils import validate_xml, get_text, safe_int, parse_date, gen_race_identifier, log_rejected_record
import json

def process_workoutdata_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the workout table.
    Validates the XML against the provided XSD schema.
    """
    # Validate the XML file first
    if not validate_xml(xml_file, xsd_file_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        return  # Skip processing this file

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        # Iterate over each race data
        for race in root.findall('racedata'):
            # Extract course_cd, race_date, and race_number to generate race_identifier
            course_cd = race.find('track').text.strip()  # Clean extra spaces
            race_date = parse_date(get_text(race.find('race_date')))
            race_number = safe_int(get_text(race.find('race')))
            race_identifier = gen_race_identifier(course_cd, race_date, race_number)

            for horse in race.findall('horsedata'):
                try:
                    # Extract horse-related data
                    # Directly retrieve `program_number` to handle alphanumeric cases
                    program_number = get_text(horse.find('program'))
                    if program_number is None:
                        logging.warning(f"Skipping horse with missing program_number in race {race_number}")
                        continue  # Skip to next horse if `program_number` is missing
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
                                INSERT INTO workoutdata (worknum, race_identifier, program_number, 
                                            days_back, worktext, ranking, rank_group)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (worknum, race_identifier, program_number) DO UPDATE 
                                SET days_back = EXCLUDED.days_back,
                                    worktext = EXCLUDED.worktext,
                                    ranking = EXCLUDED.ranking,
                                    rank_group = EXCLUDED.rank_group
                            """

                            # Execute the query for workout data
                            cursor.execute(insert_workout_query, (
                                worknum, race_identifier, program_number, 
                                            days_back, worktext, ranking, rank_group
                            ))
                        else:
                            logging.warning(f"No workoutdata found for horse {axciskey} in file {xml_file}.")
                except Exception as horse_error:
                    # If there’s an error, log rejection and set `has_rejections` flag
                    has_rejections = True
                    logging.error(f"Error processing workoutdata for race {race_number}: {horse_error}")
                    rejected_record_data = {
                        "worknum": worknum,
                        "race_identifier": race_identifier,
                        "program_number": program_number,
                        "days_back": days_back,
                        "worktext": worktext,
                        "ranking": ranking,
                        "rank_group": rank_group
                    }
                
                    # Convert dictionary to JSON for logging
                    rejected_record_json = json.dumps(rejected_record_data)

                    log_rejected_record(conn, 'workoutdata', rejected_record_json, str(race_error))
                    conn.rollback()
                    continue  # Skip to the next workoutdata item

                    conn.commit()
                    return "processed_with_rejections" if has_rejections else "processed"

    except Exception as e:
        logging.error(f"Error processing workout data in file {xml_file}: {e}")
        conn.rollback()
        return "error"