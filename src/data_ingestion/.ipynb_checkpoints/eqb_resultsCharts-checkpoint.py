import os
import logging
from datetime import datetime
from src.data_ingestion.ingestion_utils import validate_xml, update_ingestion_status
from src.data_ingestion.race_results import process_raceresults_file
from src.data_ingestion.results_entries import process_results_entries_file
from src.data_ingestion.exotic_wagers import process_exotic_wagers_file
from src.data_ingestion.jockey_current import process_jockey_current_file
from src.data_ingestion.trainer_current import process_trainer_current_file
from src.data_ingestion.results_scratches import process_results_scratches_file
from src.data_ingestion.results_earnings import process_results_earnings_file
from src.data_ingestion.exotic_wagers import process_exotic_wagers_file

def process_xml_files(directory, conn, xsd_schema_path, processed_files):
    cursor = conn.cursor()

    # Define the sections and functions to process
    sections = [
        ("race_results", process_raceresults_file),
        ("jockey_current", process_jockey_current_file),
        ("trainer_current", process_trainer_current_file),
        ("results_entries", process_results_entries_file),
        ("exotic_wagers", process_exotic_wagers_file),
        # Add more sections as needed
    ]

    # Loop through all XML files in the directory structure
    for root, _, files in os.walk(directory):
        tch_files = [f for f in files if f.endswith("tch.xml")]
        for file in tch_files:
            xml_base_name = os.path.basename(file)  # Get the file name without the path
            xml_path = os.path.join(root, file)  # Full file path

            if os.path.isfile(xml_path) and (xml_base_name, 'processed', 'ResultsCharts') not in processed_files:                
                try:
                    if not validate_xml(xml_path, xsd_schema_path):
                        logging.error(f"Validation failed for XML file: {xml_path}")
                        update_ingestion_status(conn, xml_base_name, "Validation failed", "ResultsCharts")
                        continue

                    section_results = {}  # Track the result of each section

                    # Process each section sequentially
                    for section_name, process_func in sections:
                        try:
                            result = process_func(xml_path, conn, cursor, xsd_schema_path)
                            section_results[section_name] = "processed" if result else "error"
                        except Exception as section_error:
                            logging.error(f"Error in {section_name} for {xml_path}: {section_error}")
                            section_results[section_name] = "error"

                    # Determine the overall status based on section results
                    if all(status == "processed" for status in section_results.values()):
                        conn.commit()
                        update_ingestion_status(conn, xml_base_name, "processed", "ResultsCharts")
                        processed_files.add(xml_base_name)
                    else:
                        conn.rollback()
                        update_ingestion_status(conn, xml_base_name, "processed_with_errors", "ResultsCharts")

                except Exception as xml_err:
                    logging.error(f"Error processing {file}: {xml_err}")
                    conn.rollback()
                    update_ingestion_status(conn, xml_base_name, "error", "ResultsCharts")
            else:
                 logging.info(f"########################### Skipping already processed ResultsCharts file: {xml_base_name} ###########################")

    cursor.close()
        
def process_resultscharts_data(conn, resultscharts_dir, xsd_schema_rc, error_log, processed_files):
    try:
        year_dirs = ['Daily'] #'2022R', '2023R', '2024R', 'Daily']
        
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
            error_file.write(f"{datetime.now()} - ResultsCharts error: {e}\n")