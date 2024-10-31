import os
import json
import logging
from datetime import datetime
import psycopg2
from ingestion_utils import extract_race_date, extract_course_code, extract_post_time, log_file_status

def format_race_record(race_info):
    """Helper function to format race_info as a CSV string for logging."""
    return ','.join([
        race_info.get('I', ''),
        race_info.get('Country', ''),
        race_info.get('Racecourse', ''),
        str(race_info.get('RaceNo', '')),
        race_info.get('PostTime', ''),
        race_info.get('RaceType', ''),
        str(race_info.get('RaceLength', '')),
        str(race_info.get('Published', '')),
        race_info.get('EQBRacecourse', '')
    ])

def process_tpd_racelist_data(conn, directory_path, error_log_file, processed_files):
    cursor = conn.cursor()

    for filename in os.listdir(directory_path):
        if filename in processed_files:
            logging.info(f"Skipping already processed file: {filename}")
            continue

        filepath = os.path.join(directory_path, filename)
        logging.info(f"Processing file: {filepath}")
        
        with open(filepath, 'r') as f:
            try:
                race_data = json.load(f)
                for race_id, race_info in race_data.items():
                    try:
                        # Use the extraction functions
                        course_cd = extract_course_code(race_info['I'])
                        race_date = extract_race_date(race_info['I'])  # From the 'I' field (YYYY-MM-DD)
                        race_number = extract_post_time(race_info['PostTime'])  # Combine race_date with PostTime
                        post_time = race_info['PostTime']
                        country = race_info['Country']
                        race_course = race_info['Racecourse']
                        race_number = race_info['RaceNo']
                        race_type = race_info['RaceType']
                        race_length = race_info['RaceLength']
                        published = race_info['Published']
                        eqb_race_course = race_info.get('EQBRacecourse') or None

                        insert_query = """
                            INSERT INTO race_list (course_cd, post_time, country, race_course, race_number, 
                                                   race_type, race_length, published, eqb_race_course, race_date)
                            VALUES (%s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s, %s)
                            ON CONFLICT (course_cd, race_date, race_number)
                            DO UPDATE SET
                                post_time = EXCLUDED.post_time,
                                country = EXCLUDED.country,
                                race_course = EXCLUDED.race_course,
                                race_type = EXCLUDED.race_type,
                                race_length = EXCLUDED.race_length,
                                published = EXCLUDED.published,
                                eqb_race_course = EXCLUDED.eqb_race_course;
                        """
                        try:
                            cursor.execute(insert_query, (
                                course_cd, post_time, country, race_course, race_number, race_type, race_length,
                                published, eqb_race_course, race_date
                            ))
                            conn.commit()
                        except psycopg2.Error as e:
                            logging.error(f"Error inserting race ID {race_id} in file {filename}: {e}")
                            formatted_record = format_race_record(race_info)
                            logging.error(f"Failed record: {formatted_record}")
                            conn.rollback()
                            log_file_status(conn, filename, datetime.now(), "error", str(e))

                    except KeyError as e:
                        logging.error(f"Missing key {e} in race ID {race_id} from file {filename}")
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