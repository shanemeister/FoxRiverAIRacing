import os
import logging
from datetime import datetime
from ingestion_utils import validate_xml, log_file_status
from race_results import process_raceresults_file
from results_entries import process_results_entries_file
from exotic_wagers import process_exotic_wagers_file
from jockey_current import process_jockey_current_file
from trainer_current import process_trainer_current_file
from results_scratches import process_results_scratches_file
from results_earnings import process_results_earnings_file
from exotic_wagers import process_exotic_wagers_file


def process_xml_files(directory, conn, xsd_schema_path, processed_files):
    cursor = conn.cursor()
    
    # Define process functions without direct logging calls in the list
    process_functions = [
        ("race_results", process_raceresults_file),
        ("jockey_current", process_jockey_current_file),
        ("trainer_current", process_trainer_current_file),
        ("results_entries", process_results_entries_file),
        ("exotic_wagers", process_exotic_wagers_file),
        ("results_scratches", process_results_scratches_file),
        ("results_earnings", process_results_earnings_file),
    ]

    total_files, files_processed, files_skipped = 0, 0, 0

    # Start directory walk
    for root, dirs, files in os.walk(directory):
        # Filter only .xml files
        tch_files = [f for f in files if f.endswith("tch.xml")]
        total_files += len(tch_files)
        print(f"Found {len(tch_files)} files ending with 'tch.xml' in directory: {root}")  # Debugging directory content

        for file in tch_files:
            # Check if file was already processed
            if file not in processed_files:
                xml_path = os.path.join(root, file)

                try:
                    # Mark and log as "in-progress"
                    log_file_status(conn, file, datetime.now(), "in-progress")
                    
                    # Validate XML and process if valid
                    if validate_xml(xml_path, xsd_schema_path):
                        status = "processed"
                        
                        # Process each section with error handling
                        for func_name, process_func in process_functions:
                            try:
                                logging.info(f"Processing ResultsCharts data {func_name} ...")
                                result = process_func(xml_path, conn, cursor, xsd_schema_path)
                                if result == "processed_with_rejections":
                                    status = "processed_with_rejections"
                            except Exception as func_err:
                                logging.error(f"Error in {func_name} for {xml_path}: {func_err}")
                                status = "error"
                                break
                        
                        # Final log and update counts
                        log_file_status(conn, file, datetime.now(), status)
                        processed_files.add(file)
                        if status == "processed":
                            files_processed += 1
                        else:
                            files_skipped += 1
                    else:
                        # Validation failed
                        logging.warning(f"Validation failed for XML file: {xml_path}")
                        log_file_status(conn, file, datetime.now(), "error", "Validation failed")
                        files_skipped += 1

                except Exception as xml_err:
                    # General exception in processing
                    logging.error(f"Error processing {file}: {xml_err}")
                    log_file_status(conn, file, datetime.now(), "error", str(xml_err))
                    files_skipped += 1

    # Print final summary
    print(f"Total files found: {total_files}")
    print(f"Files successfully processed: {files_processed}")
    print(f"Files skipped or errored: {files_skipped}")

    cursor.close()
        
def process_resultscharts_data(conn, resultscharts_dir, xsd_schema_rc, error_log, processed_files):
    """
    Main function to process ResultsCharts data.
    This will handle the year-specific subdirectories.
    """
    try:
        # Specify the year or directory to process (e.g., 2022R)
        year_dirs = ['2022R', '2023R', '2024R', 'Daily']  # Extend this list for more years or other directories
        
        for year_dir in year_dirs:
            rc_data_path = os.path.join(resultscharts_dir, year_dir)
            
            if os.path.exists(rc_data_path):
                logging.info(f"Processing ResultsCharts data for {year_dir}")
                process_xml_files(rc_data_path, conn, xsd_schema_rc, processed_files)
            else:
                logging.warning(f"Directory {rc_data_path} not found for {year_dir}")

        logging.info("ResultsCharts data ingestion completed successfully.")
    
    except Exception as e:
        logging.error(f"Error during ResultsCharts data ingestion: {e}")
        with open(error_log, 'a') as error_file:
            error_file.write(f"ResultsCharts data ingestion error: {e}\n")
