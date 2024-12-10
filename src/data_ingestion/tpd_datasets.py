import os
import re
import json
import logging
from datetime import datetime, date
import psycopg2
from src.data_ingestion.ingestion_utils import update_ingestion_status, parse_time, parse_date, extract_course_code, extract_race_date
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd
from src.data_ingestion.tpd_sectionals import process_tpd_sectionals
from src.data_ingestion.tpd_gpsdata import process_tpd_gpsdata

def process_tpd_data(conn, directory_path, error_log_file, processed_files, data_type):
    cursor = conn.cursor()
    try:
        cutoff_num = 0
        skipped = 0
        bad_course_cd = 0
        with open(error_log_file, 'a') as error_log:
            # Collect and group files by course, date, and time
            files_by_course_date = {}
            
            # Define cutoff date for filtering files
            cutoff_date = date(2022, 1, 1)

            # Get the list of all files in the directory
            all_files = os.listdir(directory_path)
            logging.info(f"Total number of files in the directory: {len(all_files)}")

            # Filter files that match the expected format
            filename_pattern = re.compile(r"^[A-Z0-9]{2}\d{12}$")
            valid_files = [filename for filename in all_files if filename_pattern.match(filename)]
            logging.info(f"Number of valid files matching the pattern: {len(valid_files)}")

            for filename in valid_files:
                filepath = os.path.join(directory_path, filename)
                
                # Check if the file is a directory or already processed
                if os.path.isdir(filepath):
                    #logging.info(f"Skipping directory: {filename}")
                    continue  # Skip directories
                
                if (filename, 'processed', data_type) in processed_files:
                    skipped += 1
                    #logging.info(f"Skipping already processed TPD {data_type} file: {filename}")
                    continue  # Skip already processed files

                try:
                    course_cd = extract_course_code(filename)
                    if course_cd is None or len(course_cd) != 3 or course_cd == 'XXX' or course_cd == 'UNK':
                        bad_course_cd += 1
                        #logging.info(f"Skipping file {filename} due to invalid course_cd: {course_cd}")
                        continue

                    race_date = extract_race_date(filename)
                    post_time_str = filename[-4:]
                    post_time = f"{post_time_str[:2]}:{post_time_str[2:]}:00"

                    # Filter out files with race_date earlier than cutoff_date
                    if race_date < cutoff_date:
                        #logging.info(f"Skipping file {filename} due to race_date before cutoff: {race_date}")
                        cutoff_num += 1
                        continue

                    # Group files by (course_cd, race_date) for sorting and race number assignment
                    key = (course_cd, race_date)
                    if key not in files_by_course_date:
                        files_by_course_date[key] = []
                    files_by_course_date[key].append((filename, post_time))

                except ValueError as e:
                    error_message = f"Error extracting metadata from filename {filename}: {e}"
                    logging.error(error_message)
                    error_log.write(f"{datetime.now()} - {error_message}\n")
                    update_ingestion_status(conn, filename, str(e), data_type)
                    continue

            logging.info(f"Total number of files skipped: {skipped}")
            logging.info(f"Total number of files skipped due to cutoff date: {cutoff_num}")
            logging.info(f"Total number of files skipped due to bad course_cd: {bad_course_cd}")

            # Process files sorted by post_time for each group
            for (course_cd, race_date), file_info_list in files_by_course_date.items():
                sorted_files = sorted(file_info_list, key=lambda x: x[1])  # Sort by post_time
                course_cd = extract_course_code(filename)
                if course_cd is None or len(course_cd) != 3 or course_cd == 'XXX' or course_cd == 'UNK':
                    #logging.info(f"Skipping file {filename} due to invalid course_cd: {course_cd}")
                    continue

                for race_number, (filename, post_time) in enumerate(sorted_files, start=1):
                    filepath = os.path.join(directory_path, filename)
                    try:
                        logging.info(f"Processing {data_type} data from file {filename}")

                        with open(filepath, 'r') as f:
                            data = json.load(f)

                            # Delegate to appropriate processing function based on data_type
                            if data_type == "Sectionals":
                                results = process_tpd_sectionals(conn, data, course_cd, race_date, race_number, filename, post_time)
                                try:
                                    if results:
                                        update_ingestion_status(conn, filename, "processed", "Sectionals")
                                        processed_files.add((filename, "processed", data_type))
                                    else:
                                        update_ingestion_status(conn, filename, "error", "Sectionals")
                                except Exception as section_error:
                                    logging.error(f"Error processing: filename: {filename} course_cd: {course_cd}")
                                    update_ingestion_status(conn, filename, str(section_error), "Sectionals")
                            elif data_type == "GPSData":
                                results = process_tpd_gpsdata(conn, data, course_cd, race_date, post_time, race_number, filename)
                                try:
                                    if results:
                                        update_ingestion_status(conn, filename, "processed", "GPSData")
                                        processed_files.add((filename, "processed", data_type))
                                    else:
                                        update_ingestion_status(conn, filename, "error", "GPSData")
                                except Exception as section_error:
                                    logging.error(f"Error processing: filename: {filename} course_cd: {course_cd}")
                                    update_ingestion_status(conn, filename, str(section_error), "GPSData")
                            else:
                                logging.error(f"Unknown data type: {data_type}")
                                continue

                    except json.JSONDecodeError as e:
                        error_message = f"JSON decode error in file {filename}: {e}"
                        logging.error(error_message)
                        error_log.write(f"{datetime.now()} - {error_message}\n")
                        update_ingestion_status(conn, filename, "JSON decode error", data_type)
                    except Exception as e:
                        error_message = f"Unexpected error in file {filename}: {e}"
                        logging.error(error_message)
                        error_log.write(f"{datetime.now()} - {error_message}\n")
                        update_ingestion_status(conn, filename, str(e), data_type)

            conn.commit()  # Commit all changes
    finally:
        cursor.close()
        
# Wrapper functions for each data type
def process_tpd_sectionals_data(conn, directory_path, error_log_file, processed_files):
    process_tpd_data(conn, directory_path, error_log_file, processed_files, data_type="Sectionals")

def process_tpd_gpsdata_data(conn, directory_path, error_log_file, processed_files):
    process_tpd_data(conn, directory_path, error_log_file, processed_files, data_type="GPSData")