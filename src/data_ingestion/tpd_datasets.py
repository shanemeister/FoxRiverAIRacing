
import os
import json
import logging
from datetime import datetime
from src.data_ingestion.ingestion_utils import (
    parse_time, gen_race_identifier, extract_course_code, extract_race_date, log_file_status)
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd 
from src.data_ingestion.tpd_sectionals import process_tpd_sectionals  # Similar handler for sectionals
from src.data_ingestion.tpd_gpsdata import process_tpd_gpsdata  # Similar handler for gpsData
import psycopg2

def process_tpd_data(conn, directory_path, error_log_file, processed_files, data_type):
    cursor = conn.cursor()
    try:
        with open(error_log_file, 'a') as error_log:
            files_by_course_date = {}
            for filename in os.listdir(directory_path):
                filepath = os.path.join(directory_path, filename)
                if os.path.isdir(filepath):
                    continue
                if filename not in processed_files:
                    try:
                        course_cd = extract_course_code(filename)
                        race_date = extract_race_date(filename)
                        key = (course_cd, race_date)
                        if key not in files_by_course_date:
                            files_by_course_date[key] = []
                        files_by_course_date[key].append(filename)
                    except ValueError as e:
                        error_message = f"Error extracting metadata from filename {filename}: {e}"
                        logging.error(error_message)
                        error_log.write(f"{datetime.now()} - {error_message}\n")
                        log_file_status(conn, filename, datetime.now(), "error", str(e))  # Ensure log_file_status is called
                        continue

            for (course_cd, race_date), files in files_by_course_date.items():
                try:
                    sorted_files = sorted(files, key=lambda x: int(x[-8:]))
                except ValueError as e:
                    for filename in files:
                        error_message = f"Error sorting files for course_cd {course_cd} and race_date {race_date}: {e}"
                        logging.error(error_message)
                        error_log.write(f"{datetime.now()} - {error_message}\n")
                        log_file_status(conn, filename, datetime.now(), "error", str(e))
                    continue

                for race_number, filename in enumerate(sorted_files, start=1):
                    try:
                        race_identifier = gen_race_identifier(course_cd, race_date, race_number)
                        filepath = os.path.join(directory_path, filename)
                        logging.info(f"Processing {data_type} file: {filepath}")

                        with open(filepath, 'r') as f:
                            try:
                                data = json.load(f)

                                if data_type == "sectionals":
                                    process_tpd_sectionals(conn, data, course_cd, race_date, race_number, race_identifier, directory_path, processed_files, eqb_tpd_codes_to_course_cd)
                                elif data_type == "gpsData":
                                    process_tpd_gpsdata(conn, data, course_cd, race_date, race_number, race_identifier, processed_files)
                                else:
                                    logging.error(f"Unknown data type: {data_type}")
                                    continue

                                log_file_status(conn, filename, datetime.now(), "processed")
                                processed_files.add(filename)

                            except json.JSONDecodeError as e:
                                error_message = f"JSON decode error in file {filename}: {e}"
                                logging.error(error_message)
                                error_log.write(f"{datetime.now()} - {error_message}\n")
                                log_file_status(conn, filename, datetime.now(), "error", "JSON decode error")
                            except Exception as e:
                                error_message = f"Unexpected error in file {filename}: {e}"
                                logging.error(error_message)
                                error_log.write(f"{datetime.now()} - {error_message}\n")
                                log_file_status(conn, filename, datetime.now(), "error", str(e))
                    except Exception as e:
                        error_message = f"Error processing file {filename}: {e}"
                        logging.error(error_message)
                        error_log.write(f"{datetime.now()} - {error_message}\n")
                        continue

            conn.commit()
    finally:
        cursor.close()
        
def process_tpd_racelist_data(conn, directory_path, error_log_file, processed_files):
    """
    Processes the TPD Racelist data separately due to unique file structure.
    Each file represents multiple races organized by a common share code.
    """
    cursor = conn.cursor()
    try:
        for filename in os.listdir(directory_path):
            if filename in processed_files:
                logging.info(f"Skipping already processed file: {filename}")
                continue

            filepath = os.path.join(directory_path, filename)
            logging.info(f"Processing racelist file: {filepath}")

            with open(filepath, 'r') as f:
                try:
                    race_data = json.load(f)

                    # Each race within a file is treated individually
                    for race_id, race_info in race_data.items():
                        logging.info(f"Input for I: {race_info['I']}")
                        course_cd = extract_course_code(race_info['I'])
                        #print(f"Course_cd: {course_cd}")
                        #print(f"Identifier I: {race_info['I']}")
                        logging.info(f"Course_cd: {course_cd}")
                        race_date = extract_race_date(race_info['I'])
                        race_number = race_info['RaceNo']
                        print(f"Race number: {race_number}")
                        race_identifier = gen_race_identifier(course_cd, race_date, race_number)
                        logging.info(f"Input to race_identifier: {course_cd}, {race_date}, {race_number}")
                        post_time = parse_time(race_info['PostTime'])
                        country = race_info['Country']
                        race_course = race_info['Racecourse']
                        race_type = race_info['RaceType']
                        race_length = race_info['RaceLength']
                        published = race_info['Published']
                        eqb_race_course = race_info.get('EQBRacecourse') or None

                        insert_query = """
                            INSERT INTO race_list (race_identifier, course_cd, race_date, race_number, post_time, country, race_course, 
                                                   race_type, race_length, published, eqb_race_course)
                            VALUES (%s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (race_identifier)
                            DO UPDATE SET
                                course_cd = EXCLUDED.course_cd,
                                race_date = EXCLUDED.race_date,
                                race_number = EXCLUDED.race_number,
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
                                race_identifier, course_cd, race_date, race_number, post_time, country, 
                                race_course, race_type, race_length, published, eqb_race_course
                            ))
                            conn.commit()
                            logging.info(f"Successfully inserted/updated race ID {race_identifier} from file {filename}")
                        except psycopg2.Error as e:
                            logging.error(f"Error inserting race ID {race_identifier} in file {filename}: {e}")
                            formatted_record = format_race_record(race_info)
                            logging.error(f"Failed record: {formatted_record}")
                            conn.rollback()
                            log_file_status(conn, filename, datetime.now(), "error", str(e))

                    log_file_status(conn, filename, datetime.now(), "processed")
                    processed_files.add(filename)

                except json.JSONDecodeError as e:
                    logging.error(f"JSON decode error in file {filename}: {e}")
                    log_file_status(conn, filename, datetime.now(), "error", "JSON decode error")
                except Exception as e:
                    logging.error(f"Unexpected error in file {filename}: {e}")
                    log_file_status(conn, filename, datetime.now(), "error", str(e))
                    
        conn.commit()  # Commit all changes for processed files
    finally:
        cursor.close()

# Wrapper functions for ingestion controller to call
def process_tpd_sectionals_data(conn, directory_path, error_log_file, processed_files):
    process_tpd_data(conn, directory_path, error_log_file, processed_files, data_type="sectionals")

def process_tpd_gpsdata_data(conn, directory_path, error_log_file, processed_files):
    process_tpd_data(conn, directory_path, error_log_file, processed_files, data_type="gpsData")