import os
import json
import logging
from datetime import datetime, date
from src.data_ingestion.ingestion_utils import update_ingestion_status
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd
import psycopg2

def process_tpd_racelist(conn, directory_path, error_log_file, processed_files):
    has_rejections = False  # Track if any records were rejected
    cursor = conn.cursor()
    data_type = "Racelist"  # Specify the data type for processed_files check

    # Define cutoff date for filtering files (adjust as needed)
    cutoff_date = date(2017, 1, 1)

    try:
        with open(error_log_file, 'a') as error_log:
            logging.info(f"Checking directory: {directory_path}")
            
            for filename in os.listdir(directory_path):
                filepath = os.path.join(directory_path, filename)

                # Check if this file has already been processed for Racelist
                if (filename, 'processed', data_type) in processed_files:
                    logging.info(f"Skipping already processed TPD {data_type} file: {filename}")
                    continue
                
                logging.info(f"Processing file: {filename}")
                try:
                    with open(filepath, 'r') as f:
                        data = json.load(f)

                    for race_id, race_data in data.items():
                        # Extract information from the "I" field
                        identifier = race_data["I"]

                        # Determine course_cd dynamically by checking first two or three characters
                        course_cd_key = identifier[:3]
                        if course_cd_key not in eqb_tpd_codes_to_course_cd:
                            course_cd_key = identifier[:2]
                        course_cd = eqb_tpd_codes_to_course_cd.get(course_cd_key, None)
                        
                        if not course_cd or course_cd == 'XXX':
                            logging.info(f"Skipping file {filename} due to invalid course_cd: {course_cd}")
                            continue
                        # Extract race_date (next 8 characters)
                        race_date_str = identifier[len(course_cd_key):len(course_cd_key) + 8]
                        race_date = datetime.strptime(race_date_str, "%Y%m%d").date()

                        # Filter out files with race_date earlier than cutoff_date
                        if race_date < cutoff_date:
                            logging.info(f"Skipping file {filename} due to race_date before cutoff: {race_date}")
                            continue
                        # Extract other data fields
                        race_number = race_data.get("RaceNo")
                        post_time = race_data.get("PostTime")
                        country = race_data.get("Country")
                        race_course = race_data.get("Racecourse")
                        race_type = race_data.get("RaceType")
                        race_length = race_data.get("RaceLength")
                        published = race_data.get("Published")
                        eqb_race_course = race_data.get("EQBRacecourse")
                               
                            # Insert data into the race_list table
                        insert_query = """
                            INSERT INTO race_list (
                                course_cd, race_date, race_number, country, post_time,
                                race_course, race_type, race_length, published, eqb_race_course
                                )VALUES (%s, %s, %s, %s, %s,
                                         %s, %s, %s, %s, %s)
                            ON CONFLICT (course_cd, race_date, race_number)
                            DO UPDATE SET   post_time = EXCLUDED.post_time,
                                            country = EXCLUDED.country,
                                            race_course = EXCLUDED.race_course,
                                            race_type = EXCLUDED.race_type,
                                            race_length = EXCLUDED.race_length,
                                            published = EXCLUDED.published,
                                            eqb_race_course = EXCLUDED.eqb_race_course;
                        """
                        try:
                            cursor.execute(insert_query, (
                                course_cd, race_date, race_number, country, post_time,
                                race_course, race_type, race_length, published, eqb_race_course
                            ))
                            conn.commit()
                            #logging.info(f"Successfully inserted record for saddle_cloth_number: {saddle_cloth_number}, time_stamp: {time_stamp}")
                            logging.info(f"Successfully inserted record for race_list: {course_cd}, race_date: {race_date})")
                        except psycopg2.Error as e:
                            has_rejections = True  # Track if any records were rejected
                            logging.error(f"Error inserting RACE_LIST data in file {filename}: {e}")
                            conn.rollback()
                            update_ingestion_status(conn, filename, str(e), "Racelist")               
                    # Mark file as processed
                    processed_files.add((filename, 'processed', data_type))
                    logging.info(f"Successfully processed Racelist data from file {filename}")
                    if not has_rejections:
                        conn. commit()
                        update_ingestion_status(conn, filename, "processed", "Racelist")
                    else:
                        conn.commit()
                        update_ingestion_status(conn, filename, "partial error", "Racelist")
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    has_rejections = True
                    error_message = f"Error parsing file {filename}: {e}"
                    logging.error(error_message)
                    error_log.write(f"{datetime.now()} - {error_message}\n")
                    update_ingestion_status(conn, filename, str(e), 'Racelist')
                except Exception as e:
                    error_message = f"Unexpected error in file {filename}: {e}"
                    logging.error(error_message)
                    error_log.write(f"{datetime.now()} - {error_message}\n")
                    update_ingestion_status(conn, filename, str(e), 'Racelist')
                
        conn.commit()
    finally:
        cursor.close()