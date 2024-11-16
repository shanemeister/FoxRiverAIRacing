import os
import zipfile
import logging
import tempfile
from src.data_ingestion.ingestion_utils import validate_xml
from src.data_ingestion.racedata import process_racedata_file
from src.data_ingestion.horse_data import process_horsedata_file 
from src.data_ingestion.dam import process_dam_file
from src.data_ingestion.sire import process_sire_file
from src.data_ingestion.stat_horse import process_stathorse_file
from src.data_ingestion.stat_dam import process_stat_dam_file
from src.data_ingestion.stat_sire import process_stat_sire_file
from src.data_ingestion.jockey import process_jockey_file
from src.data_ingestion.stat_jockey import process_stat_jockey_file
from src.data_ingestion.trainer import process_trainer_file
from src.data_ingestion.stat_trainer import process_stat_trainer_file
from src.data_ingestion.runners import process_runners_file
from src.data_ingestion.runners_stats import process_runners_stats_file
from src.data_ingestion.workoutdata import process_workoutdata_file
from src.data_ingestion.ppdata import process_ppData_file
from src.data_ingestion.ingestion_utils import update_ingestion_status
from datetime import datetime

import os
import zipfile
import logging
from datetime import datetime

import os
import zipfile
import logging
from datetime import datetime

def process_zip_files(directory, conn, xsd_schema_path, processed_files):
    cursor = conn.cursor()
    total_zips = 0
    zips_processed = 0
    zips_failed = 0
    x = 0
    for root, dirs, files in os.walk(directory):
        total_zips += len([f for f in files if f.endswith("plusxml.zip")])
    
        for file in files:
            if file.endswith("plusxml.zip"):
                zip_path = os.path.join(root, file)
                
                try:
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        # Extract the XML file name inside the ZIP
                        xml_file = zip_ref.namelist()[0]
                        xml_base_name = os.path.basename(xml_file)  # Get the base name without path
                        xml_path = os.path.join(directory, xml_file)

                        # Check if the XML file has already been processed by its base name
                        if os.path.isdir(xml_base_name) or (xml_base_name, 'processed', 'PlusPro') in processed_files:
                            logging.info(f"########################### Skipping already processed PluPro file: {xml_base_name} ###########################")
                            continue  # Skip directories and already processed files
                            
                        # Extract and validate XML
                        zip_ref.extract(xml_file, path=directory)
                        if validate_xml(xml_path, xsd_schema_path):
                            # Process the XML file and set the final status based on the outcome
                            x+=1
                            if process_single_xml_file(xml_path, xml_base_name, conn, cursor, xsd_schema_path, processed_files, x):
                                zips_processed += 1
                            else:
                                zips_failed += 1
                        else:
                            zips_failed += 1

                except zipfile.BadZipFile:
                    logging.error(f"Bad ZIP file: {file} in {directory}")
                    zips_failed += 1

                except Exception as xml_err:
                    logging.error(f"Error processing XML in ZIP {zip_path}: {xml_err}")
                    zips_failed += 1

    cursor.close()
    print(f"Total ZIP files found: {total_zips}")
    print(f"ZIP files processed successfully: {zips_processed}")
    print(f"ZIP files failed or skipped: {zips_failed}")
                   
def process_single_xml_file(xml_file, xml_base_name, conn, cursor, xsd_schema_path, processed_files, x):
    """
    Processes each XML file, logging success only after all sections are loaded.
    Returns True if all sections succeed; False otherwise.
    """
    print(f"Processing XML file number : {x}")

    section_results = {}  # Track the result of each section
    try:
        if not validate_xml(xml_file, xsd_schema_path):
            logging.error(f"Validation failed for XML file: {xml_file}")
            return False  # Skip processing if validation fails

        # Sequentially process each part of the data and store the result for each section
        sections = [
            ("racedata", process_racedata_file),
            ("horse_data", process_horsedata_file),
            ("stat_horse", process_stathorse_file),
            ("dam", process_dam_file),
            ("stat_dam", process_stat_dam_file),
            ("sire", process_sire_file),
            ("stat_sire", process_stat_sire_file),
            ("jockey", process_jockey_file),
            ("stat_jockey", process_stat_jockey_file),
            ("trainer", process_trainer_file),
            ("stat_trainer", process_stat_trainer_file),
            ("runners", process_runners_file),
            ("runners_stats", process_runners_stats_file),
            ("workout", process_workoutdata_file),
            ("ppData", process_ppData_file)
        ]

        for section_name, process_function in sections:
            # logging.info(f"Processing {section_name} data file: {xml_file}")
            try:
                result = process_function(xml_file, xsd_schema_path, conn, cursor)
                if result:
                    section_results[section_name] = "processed"
                    update_ingestion_status(conn, xml_base_name, "processed", section_name)
                else:
                    section_results[section_name] = "error"
                    update_ingestion_status(conn, xml_base_name, "processed_with_errors", section_name)
            except Exception as section_error:
                logging.error(f"Error processing {section_name} in file {xml_file}: {section_error}")
                section_results[section_name] = "error"

        # Determine overall success based on individual section results
        if all(status == "processed" for status in section_results.values()):
            conn.commit()
            update_ingestion_status(conn, xml_base_name, "processed", "PlusPro")
            processed_files.add(xml_base_name)
            return True  # All sections succeeded
        else:
            conn.commit()
            update_ingestion_status(conn, xml_base_name, "processed_with_errors", "PlusPro")
            return False  # At least one section had an error

    except Exception as e:
        logging.error(f"Error processing XML file {xml_file}: {e}")
        conn.rollback()  # Rollback the transaction for safety
        update_ingestion_status(conn, xml_base_name, str(e), "PlusPro")
        return False  # Indicate failure
    
def process_pluspro_data(conn, pluspro_dir, xsd_schema_path, error_log, processed_files):
    """
    Main function to process PlusPro data.
    This will handle the year-specific subdirectories.
    """
    try:
        # Specify the year or directory to process (e.g., 2022PP)
        year_dirs = ['Daily'] # ['2022PP', '2023PP', '2024PP', 'Daily']  # Extend this list for more years or other directories
        
        for year_dir in year_dirs:
            pp_data_path = os.path.join(pluspro_dir, year_dir)
            
            if os.path.exists(pp_data_path):
                logging.info(f"Processing PlusPro data for {processed_files}")
                process_zip_files(pp_data_path, conn, xsd_schema_path, processed_files)
            else:
                logging.warning(f"Directory {pp_data_path} not found for {year_dir}")

        logging.info("PlusPro data ingestion completed successfully.")
    
    except Exception as e:
        logging.error(f"Error during PlusPro data ingestion: {e}")
        with open(error_log, 'a') as error_file:
            error_file.write(f"PlusPro data ingestion error: {e}\n")