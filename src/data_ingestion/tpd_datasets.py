import os
import json
import logging
from datetime import datetime
from ingestion_utils import (
    get_db_connection, load_processed_files, log_file_status,
    extract_race_date, extract_course_code, extract_post_time,
    gen_race_identifier, translate_course_code
)
from tpd_racelist import process_tpd_racelist
from tpd_sectionals import process_tpd_sectionals
from tpd_gpsdata import process_tpd_gpsdata  # Similar handler for gpsData


def process_tpd_data(conn, directory_path, error_log_file, processed_files, data_type):
    """
    Main function to process TPD datasets, e.g., Sectionals, gpsData.
    
    Parameters:
    - conn: Database connection.
    - directory_path: Directory containing TPD data files.
    - error_log_file: Path to the error log file.
    - processed_files: Set of already processed files.
    - data_type: Type of data to process, e.g., 'sectionals' or 'gpsData'.
    """
    cursor = conn.cursor()
    try:
        # Sort files to maintain race order based on time within each date
        files_to_process = sorted(
            [f for f in os.listdir(directory_path) if f not in processed_files],
            key=lambda x: int(x[-8:])  # Sorting by the share code time portion in filename
        )

        for filename in files_to_process:
            # Extract metadata from filename (course_cd, race_date, race_number)
            course_cd = translate_course_code(extract_course_code(filename[:2]))
            race_date = extract_race_date(filename[2:10])  # Parse date from filename
            race_number = int(filename[-4:])  # Determine race number by file order/time

            filepath = os.path.join(directory_path, filename)
            logging.info(f"Processing {data_type} file: {filepath}")
            
            with open(filepath, 'r') as f:
                try:
                    data = json.load(f)

                    # Process based on data_type and call specific functions
                    if data_type == "sectionals":
                        process_tpd_sectionals(conn, data, course_cd, race_date, race_number, processed_files)
                    elif data_type == "gpsData":
                        process_tpd_gpsdata(conn, data, course_cd, race_date, race_number, processed_files)
                    else:
                        logging.error(f"Unknown data type: {data_type}")
                        continue

                    # Log success for this file
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
                        course_cd = translate_course_code(extract_course_code(race_info['I']))
                        race_date = extract_race_date(race_info['I'])
                        post_time = extract_post_time(race_info['PostTime'])
                        race_identifier = gen_race_identifier(course_cd, race_date, race_id)

                        # Call specific racelist processing function
                        process_tpd_racelist(conn, race_info, course_cd, race_date, race_identifier, processed_files)

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