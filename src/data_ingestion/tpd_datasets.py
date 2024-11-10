import os
import re
import json
import logging
from datetime import datetime, date
import psycopg2
from src.data_ingestion.ingestion_utils import update_ingestion_status, parse_time, parse_date, extract_course_code, extract_race_date
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd
from tpd_sectionals import process_tpd_sectionals
from src.data_ingestion.tpd_gpsdata import process_tpd_gpsdata

def process_tpd_data(conn, directory_path, error_log_file, processed_files, data_type):
    cursor = conn.cursor()
    try:
        with open(error_log_file, 'a') as error_log:
            # Collect and group files by course, date, and time
            files_by_course_date = {}
            
            # Define cutoff date for filtering files
            cutoff_date = date(2022, 1, 1)

            for filename in os.listdir(directory_path):
                # Filter to ignore files that don't match the expected format
                # Define the regex pattern to match filenames like '90' or 'CG' followed by 12 digits
                filename_pattern = re.compile(r"^[A-Z0-9]{2}\d{12}$")

                # Adjust the file filtering condition
                if not filename_pattern.match(filename):
                    logging.warning(f"Skipping non-standard file: {filename}")
                    continue
                filepath = os.path.join(directory_path, filename)
                if os.path.isdir(filepath) or filename in processed_files:
                    continue  # Skip directories and already processed files

                try:
                    course_cd = extract_course_code(filename)
                    if course_cd is None or len(course_cd) != 3:
                        raise ValueError("Course code is either missing or not three characters.")

                    race_date = extract_race_date(filename)
                    post_time_str = filename[-4:]
                    post_time = f"{post_time_str[:2]}:{post_time_str[2:]}:00"

                    # Filter out files with race_date earlier than cutoff_date
                    if race_date < cutoff_date:
                        # logging.info(f"Skipping file {filename} due to race_date before cutoff: {race_date}")
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

            # Process files sorted by post_time for each group
            for (course_cd, race_date), file_info_list in files_by_course_date.items():
                sorted_files = sorted(file_info_list, key=lambda x: x[1])  # Sort by post_time

                for race_number, (filename, post_time) in enumerate(sorted_files, start=1):
                    filepath = os.path.join(directory_path, filename)
                    try:
                        #logging.info(f"Processing {data_type} file: {filepath}")

                        with open(filepath, 'r') as f:
                            data = json.load(f)

                            # Delegate to appropriate processing function based on data_type
                            if data_type == "sectionals":
                                #logging.info(f"Processing sectionals data for course_cd: {course_cd}, race_date: {race_date}, post_time: {post_time}, race_number: {race_number}")
                                #logging.info(f"Processing filename: {filename} for course code: {course_cd}")
                                results = process_tpd_sectionals(conn, data, course_cd, race_date, race_number, post_time, filename)
                                try:
                                    if results:
                                        update_ingestion_status(conn, filename, "processed", "Sectionals")
                                        processed_files.add(filename)
                                    else:
                                        update_ingestion_status(conn, filename, "error", "Sectionals")
                                except Exception as section_error:
                                    logging.error(f"Error processing: filename: {filename} course_cd: {course_cd}")
                                    update_ingestion_status(conn, filename, str(section_error, "Sectionals"))
                            elif data_type == "gpsData":
                                #logging.info(f"Processing sectionals data for course_cd: {course_cd}, race_date: {race_date}, post_time: {post_time}, race_number: {race_number}")
                                #logging.info(f"Processing filename: {filename} for course code: {course_cd}")
                                results = process_tpd_gpsdata(conn, data, course_cd, race_date, post_time,race_number,  filename)
                                try:
                                    if results:
                                        update_ingestion_status(conn, filename, "processed", "GPSData")
                                        processed_files.add(filename)
                                    else:
                                        update_ingestion_status(conn, filename, "error", "GPSData")
                                except Exception as section_error:
                                    logging.error(f"Error processing: filename: {filename} course_cd: {course_cd}")
                                    update_ingestion_status(conn, filename, str(section_error), "GPSData")
                            else:
                                logging.error(f"Unknown data type: {data_type}")
                                continue

                            # Mark file as processed
                            processed_files.add(filename)

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
    process_tpd_data(conn, directory_path, error_log_file, processed_files, data_type="sectionals")

def process_tpd_gpsdata_data(conn, directory_path, error_log_file, processed_files):
    process_tpd_data(conn, directory_path, error_log_file, processed_files, data_type="gpsData")