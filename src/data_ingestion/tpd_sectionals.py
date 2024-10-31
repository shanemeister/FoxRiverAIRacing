import os
import json
import logging
from datetime import datetime
import psycopg2
from ingestion_utils import extract_race_date, extract_course_code, extract_post_time, log_file_status, gen_race_identifier

def parse_filename(filename):
    """
    Parses TPD sectional filename to extract course code, race date, and post time.
    Example filename format: 70202405291605 where:
    - "70" is the course code
    - "20240529" is the race date
    - "1605" is the post time
    """
    course_cd = filename[:2]
    race_date = filename[2:10]
    post_time = filename[10:14]
    return course_cd, race_date, post_time

def format_sectional_record(sectional_info):
    """Helper function to format sectional_info as a CSV string for logging."""
    return ','.join([
        sectional_info.get('I', ''),
        str(sectional_info.get('G', '')),
        sectional_info.get('L', ''),
        str(sectional_info.get('S', '')),
        sectional_info.get('R', ''),
        sectional_info.get('B', ''),
        sectional_info.get('D', ''),
        sectional_info.get('N', ''),
    ])
    
def process_tpd_sectional_data(conn, directory_path, error_log_file, processed_files):
    cursor = conn.cursor()

    for filename in os.listdir(directory_path):
        if filename in processed_files:
            logging.info(f"Skipping already processed file: {filename}")
            continue

        # Parse information from the filename
        course_cd, race_date_str, post_time_str = parse_filename(filename)
        race_date = datetime.strptime(race_date_str, '%Y%m%d').date()
        
        # Initialize race number sequencing for ordering within the date
        # This may be adjusted based on available data in the JSON file
        race_number = 1  # This could increment based on distinct times

        filepath = os.path.join(directory_path, filename)
        logging.info(f"Processing file: {filepath}")
        
        with open(filepath, 'r') as f:
            try:
                sectional_data = json.load(f)

                # Loop through each sectional data entry in the file
                for sec_id, sec_info in sectional_data.items():
                    try:
                        # Extract and prepare sectional data fields
                        saddle_cloth_num = sec_id[-2:]  # Last two characters represent the horse's saddle cloth number
                        race_identifier = gen_race_identifier(course_cd, race_date, race_number)
                        gate_name = sec_info.get('G')
                        length_to_finish = sec_info.get('L')
                        sectional_time = sec_info.get('S')
                        running_time = sec_info.get('R')
                        distance_back = sec_info.get('B')
                        distance_ran = sec_info.get('D')
                        number_of_strides = sec_info.get('N', None)  # Optional field

                        insert_query = """
                            INSERT INTO sectionals (race_identifier, saddle_cloth_num, course_cd, race_date, race_number, 
                                                    gate_name, length_to_finish, sectional_time, running_time, distance_back, 
                                                    distance_ran, number_of_strides)
                            VALUES (%s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s, %s,
                                    %s, %s)
                            ON CONFLICT (race_identifier, saddle_cloth_num, gate_name)
                            DO UPDATE SET
                                length_to_finish = EXCLUDED.length_to_finish,
                                sectional_time = EXCLUDED.sectional_time,
                                running_time = EXCLUDED.running_time,
                                distance_back = EXCLUDED.distance_back,
                                distance_ran = EXCLUDED.distance_ran,
                                number_of_strides = EXCLUDED.number_of_strides;
                        """
                        try:
                            cursor.execute(insert_query, (
                                race_identifier, saddle_cloth_num, course_cd, race_date, race_number, 
                                gate_name, length_to_finish, sectional_time, running_time, distance_back, 
                                distance_ran, number_of_strides
                            ))
                            conn.commit()
                        except psycopg2.Error as e:
                            logging.error(f"Error inserting sectional data ID {sec_id} in file {filename}: {e}")
                            formatted_record = format_sectional_record(sec_info)
                            logging.error(f"Failed record: {formatted_record}")
                            conn.rollback()
                            log_file_status(conn, filename, datetime.now(), "error", str(e))

                    except KeyError as e:
                        logging.error(f"Missing key {e} in sectional ID {sec_id} from file {filename}")
                        log_file_status(conn, filename, datetime.now(), "error", f"Missing key {e}")

                processed_files.add(filename)
                log_file_status(conn, filename, datetime.now(), "processed")

            except json.JSONDecodeError as e:
                logging.error(f"JSON decode error in file {filename}: {e}")
                log_file_status(conn, filename, datetime.now(), "error", "JSON decode error")
            except Exception as e:
                logging.error(f"Unexpected error in file {filename}: {e}")
                log_file_status(conn, filename, datetime.now(), "error", str(e))

    cursor.close()