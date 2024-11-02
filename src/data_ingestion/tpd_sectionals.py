import os
import json
import logging
from datetime import datetime
import psycopg2
from src.data_ingestion.ingestion_utils import extract_race_date, extract_course_code, log_file_status, gen_race_identifier

def parse_filename(filename):
    """
    Parses the filename to extract course code, race date, and post time.
    Assumes the filename format is 'coursecodeYYYYMMDDHHMM.ext'.
    """
    try:
        course_cd = filename[:2]
        race_date_str = filename[2:10]
        post_time_str = filename[10:14]
        return course_cd, race_date_str, post_time_str
    except Exception as e:
        raise ValueError(f"Error parsing filename {filename}: {e}")

def process_tpd_sectionals(conn, data, course_cd, race_date, race_number, race_identifier, directory_path, processed_files, course_mapping):
    cursor = conn.cursor()

    for filename in os.listdir(directory_path):
        if filename in processed_files:
            logging.info(f"Skipping already processed file: {filename}")
            continue

        # Skip non-data files
        if not filename.endswith('.json'):
            logging.info(f"Skipping non-data file: {filename}")
            continue

        # Parse information from the filename
        try:
            course_cd, race_date_str, post_time_str = parse_filename(filename)
            race_date = datetime.strptime(race_date_str, '%Y%m%d').date()
            logging.info(f"Extracted course code: {course_cd} from identifier: {filename}")
        except ValueError as e:
            logging.error(f"Error parsing filename {filename}: {e}")
            log_file_status(conn, filename, datetime.now(), "error", f"Filename parsing error: {e}")
            continue

        # Check if course code is in the mapping dictionary
        if course_cd not in course_mapping or len(course_mapping[course_cd]) != 3:
            logging.error(f"Course code '{course_cd}' not found in mapping dictionary or mapped course code is not three characters. Filename: {filename}")
            log_file_status(conn, filename, datetime.now(), "error", f"Course code '{course_cd}' not found in mapping dictionary or mapped course code is not three characters.")
            continue

        filepath = os.path.join(directory_path, filename)
        logging.info(f"Processing file: {filepath}")

        with open(filepath, 'r') as f:
            try:
                sectional_data = json.load(f)
                logging.info(f"Loaded JSON data from file: {filename}")

                # Check if the loaded JSON data is a list or a dictionary
                if isinstance(sectional_data, list):
                    for index, sec_info in enumerate(sectional_data):
                        logging.info(f"Processing list entry {index + 1} of {len(sectional_data)}")
                        process_sectional_entry(conn, cursor, sec_info, race_identifier, course_cd, race_date, race_number, filename, index + 1)
                elif isinstance(sectional_data, dict):
                    for index, (sec_id, sec_info) in enumerate(sectional_data.items()):
                        logging.info(f"Processing dictionary entry {index + 1} of {len(sectional_data)}")
                        process_sectional_entry(conn, cursor, sec_info, race_identifier, course_cd, race_date, race_number, filename, index + 1)
                else:
                    raise ValueError("Unexpected JSON format")

                processed_files.add(filename)
                log_file_status(conn, filename, datetime.now(), "processed")

            except json.JSONDecodeError as e:
                logging.error(f"JSON decode error in file {filename}: {e}")
                log_file_status(conn, filename, datetime.now(), "error", "JSON decode error")
            except Exception as e:
                logging.error(f"Unexpected error in file {filename}: {e}")
                log_file_status(conn, filename, datetime.now(), "error", str(e))

    cursor.close()

def process_sectional_entry(conn, cursor, sec_info, race_identifier, course_cd, race_date, race_number, filename, index):
    try:
        # Extract and prepare sectional data fields
        program_number = str(sec_info.get('I', '')[-2:])  # Last two characters represent the program number
        gate_name = str(sec_info.get('G', ''))
        length_to_finish = str(sec_info.get('L', ''))
        sectional_time = str(sec_info.get('S', ''))
        running_time = str(sec_info.get('R', ''))
        distance_back = str(sec_info.get('B', ''))
        distance_ran = str(sec_info.get('D', ''))
        number_of_strides = str(sec_info.get('N', '')) if sec_info.get('N') is not None else None  # Optional field

        # Use index to make each entry unique within the file
        unique_identifier = index

        # Insert or update race_list record
        insert_race_list_query = """
            INSERT INTO race_list (race_identifier, course_cd, race_date, race_number)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (race_identifier)
            DO UPDATE SET
                course_cd = EXCLUDED.course_cd,
                race_date = EXCLUDED.race_date,
                race_number = EXCLUDED.race_number;
        """
        cursor.execute(insert_race_list_query, (race_identifier, course_cd, race_date, race_number))

        # Insert or update sectional entry
        insert_query = """
            INSERT INTO sectionals (program_number, course_cd, race_date, race_number, 
                                    gate_name, length_to_finish, sectional_time, running_time, distance_back, 
                                    distance_ran, number_of_strides, race_identifier, unique_identifier)
            VALUES (%s, %s, %s, %s, 
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s)
            ON CONFLICT (race_identifier, program_number, unique_identifier)
            DO UPDATE SET
                course_cd = EXCLUDED.course_cd,
                race_date = EXCLUDED.race_date,
                race_number = EXCLUDED.race_number,
                gate_name = EXCLUDED.gate_name,
                length_to_finish = EXCLUDED.length_to_finish,
                sectional_time = EXCLUDED.sectional_time,
                running_time = EXCLUDED.running_time,
                distance_back = EXCLUDED.distance_back,
                distance_ran = EXCLUDED.distance_ran,
                number_of_strides = EXCLUDED.number_of_strides;
        """
        try:
            cursor.execute(insert_query, (
                program_number, course_cd, race_date, race_number, 
                gate_name, length_to_finish, sectional_time, running_time, distance_back, 
                distance_ran, number_of_strides, race_identifier, unique_identifier
            ))
            conn.commit()
            logging.info(f"Inserted sectional entry with unique_identifier: {unique_identifier} in file {filename}")
        except psycopg2.Error as e:
            logging.error(f"Error inserting sectional data in file {filename}: {e}")
            formatted_record = format_sectional_record(sec_info)
            logging.error(f"Failed record: {formatted_record}")
            conn.rollback()
            log_file_status(conn, filename, datetime.now(), "error", str(e))

    except KeyError as e:
        logging.error(f"Missing key {e} in sectional data from file {filename}")
        log_file_status(conn, filename, datetime.now(), "error", f"Missing key {e}")
    except TypeError as e:
        logging.error(f"Type error in sectional data from file {filename}: {e}")
        log_file_status(conn, filename, datetime.now(), "error", f"Type error: {e}")