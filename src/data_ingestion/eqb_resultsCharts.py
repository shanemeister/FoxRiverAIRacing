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

def process_xml_file(filepath, conn, xsd_schema_path, processed_files):
    cursor = conn.cursor()
    xml_base_name = os.path.basename(filepath)
    data_type = "ResultsCharts"
    section_results = {}

    # print(f"\n--- Processing file: {xml_base_name} ---")
    
    try:
        if (xml_base_name, 'processed', data_type) in processed_files:
            # print(f"File already processed: {xml_base_name}. Skipping.")
            return

        #print(f"Validating XML file: {xml_base_name}")
        if not validate_xml(filepath, xsd_schema_path):
            logging.error(f"Validation failed for XML file: {xml_base_name}")
            update_ingestion_status(conn, xml_base_name, "validation_failed", data_type)
            return

        sections = [
            ("race_results", process_raceresults_file),
            ("jockey_current", process_jockey_current_file),
            ("trainer_current", process_trainer_current_file),
            ("results_entries", process_results_entries_file),
            ("exotic_wagers", process_exotic_wagers_file),
        ]

        for section_name, process_function in sections:
            # print(f"Processing section: {section_name} in file: {xml_base_name}")
            try:
                result = process_function(filepath, conn, cursor, xsd_schema_path)
                if result:
                    conn.commit()
                    update_ingestion_status(conn, xml_base_name, "processed", data_type)
                    processed_files.add((xml_base_name, 'processed', data_type))
                    logging.info(f"Successfully processed {section_name} in file {xml_base_name}")
                else:
                    conn.rollback()
                    update_ingestion_status(conn, xml_base_name, "error", data_type)

            except Exception as section_error:
                logging.error(f"Error processing {section_name} in file {xml_base_name}: {section_error}")
                section_results[section_name] = "error"
                
    except Exception as e:
        logging.error(f"Unexpected error in file {xml_base_name}: {e}")
        conn.rollback()
        update_ingestion_status(conn, xml_base_name, str(e), data_type)
    finally:
        cursor.close()

def process_resultscharts_data(conn, resultscharts_dir, xsd_schema_rc, error_log, processed_files):
    """
    Iterate over yearly directories and process each XML file with the 'tch.xml' suffix.
    
    Parameters:
    - conn: Database connection.
    - resultscharts_dir (str): Base path for ResultsCharts data directories.
    - xsd_schema_rc (str): Path to the ResultsCharts XSD schema.
    - error_log (str): Log file for errors.
    - processed_files (set): Set of processed files to avoid reprocessing.
    """
    year_dirs = ['Daily'] # ['2022R', '2023R', '2024R', 'Daily']
    
    for year_dir in year_dirs:
        rc_data_path = os.path.join(resultscharts_dir, year_dir)
        
        if not os.path.exists(rc_data_path):
            logging.warning(f"Directory {rc_data_path} does not exist for {year_dir}")
            continue
        
        #print(f"\n--- Accessing directory: {rc_data_path} ---")
        all_files = os.listdir(rc_data_path)
        #print(f"Total files in {year_dir}: {len(all_files)}")

        # Display a sample of filenames for quick verification
        #print("Sample filenames:", all_files[:5])

        # Filter files ending with 'tch.xml'
        valid_files = [filename for filename in all_files if filename.endswith("tch.xml")]
        #print(f"Valid files in {year_dir}: {len(valid_files)}")

        for filename in valid_files:
            filepath = os.path.join(rc_data_path, filename)
            #print(f"Checking file: {filename}")

            try:
                # Process the file directly without further parsing filename
                process_xml_file(filepath, conn, xsd_schema_rc, processed_files)
                
            except Exception as e:
                logging.error(f"Error processing file {filename}: {e}")
                with open(error_log, 'a') as error_file:
                    error_file.write(f"{datetime.now()} - Error processing file {filename}: {e}\n")

    logging.info("Completed ResultsCharts data ingestion.")