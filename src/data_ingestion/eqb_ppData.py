import os
import zipfile
import logging
import tempfile
from ingestion_utils import validate_xml
from racedata import process_racedata_file
from horse_data import process_horsedata_file 
from dam import process_dam_file
from sire import process_sire_file
from stat_horse import process_stathorse_file
from stat_dam import process_stat_dam_file
from stat_sire import process_stat_sire_file
from jockey import process_jockey_file
from stat_jockey import process_stat_jockey_file
from trainer import process_trainer_file
from stat_trainer import process_stat_trainer_file
from runners import process_runners_file
from runners_stats import process_runners_stats_file
from workoutdata import process_workoutdata_file
from ppdata import process_ppData_file
from ingestion_utils import log_file_status, log_ingestion_status, get_unprocessed_files
from datetime import datetime

def process_zip_files(directory, conn, xsd_schema_path, processed_files):
    cursor = conn.cursor()
    total_zips = 0  # Counter for total ZIP files to be processed
    zips_processed = 0  # Counter for successfully processed ZIPs
    zips_failed = 0  # Counter for ZIPs that failed processing

    for root, dirs, files in os.walk(directory):
        total_zips += len([f for f in files if f.endswith("plusxml.zip")])

        for file in files:
            # Process only unprocessed files with "plusxml.zip" extension
            if file.endswith("plusxml.zip") and file not in processed_files:
                zip_path = os.path.join(root, file)
                
                try:
                    # Log "in-progress" status initially
                    log_file_status(conn, file, datetime.now(), "in-progress")
                    
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        # Extract the single XML file
                        xml_file = zip_ref.namelist()[0]
                        zip_ref.extract(xml_file, path=directory)
                        xml_path = os.path.join(directory, xml_file)

                        # Process the extracted XML file
                        logging.info(f"Processing XML file: {xml_path}")
                        if validate_xml(xml_path, xsd_schema_path):
                            if process_single_xml_file(xml_path, conn, cursor, xsd_schema_path):
                                log_file_status(conn, file, datetime.now(), "processed")
                                processed_files.add(file)
                                zips_processed += 1
                            else:
                                log_file_status(conn, file, datetime.now(), "error", "Partial processing failure")
                                zips_failed += 1
                        else:
                            log_file_status(conn, file, datetime.now(), "error", "Validation failed")
                            zips_failed += 1

                except zipfile.BadZipFile:
                    logging.error(f"Bad ZIP file: {file} in {directory}")
                    log_file_status(conn, file, datetime.now(), "error", "Bad ZIP file")
                    zips_failed += 1

                except Exception as xml_err:
                    logging.error(f"Error processing XML in ZIP {zip_path}: {xml_err}")
                    log_file_status(conn, file, datetime.now(), "error", str(xml_err))
                    zips_failed += 1

    cursor.close()
    
    # Print summary of processed ZIP files
    print(f"Total ZIP files found: {total_zips}")
    print(f"ZIP files processed successfully: {zips_processed}")
    print(f"ZIP files failed or skipped: {zips_failed}")
                    
def process_single_xml_file(xml_file, conn, cursor, xsd_schema_path):
    """
    Processes each XML file, logging success only after all sections are loaded.
    Returns True if all sections succeed; False otherwise.
    """
    try:
        if not validate_xml(xml_file, xsd_schema_path):
            logging.error(f"Validation failed for XML file: {xml_file}")
            return False  # Skip processing if validation fails
        
        # Sequentially process each part of the data
        logging.info(f"Processing race data file: {xml_file}")
        process_racedata_file(xml_file, xsd_schema_path, conn, cursor)
        logging.info(f"Processing horse data file: {xml_file}")
        logging.info(f"Processing  race data file: {xml_file}")
        process_racedata_file(xml_file, xsd_schema_path, conn, cursor)
        logging.info(f"Processing horse data file: {xml_file}")
        process_horsedata_file(xml_file, xsd_schema_path, conn, cursor)
        logging.info(f"Processing stat horse data file: {xml_file}")
        process_stathorse_file(xml_file, xsd_schema_path, conn, cursor)
        logging.info(f"Processing dam data file: {xml_file}")
        process_dam_file(xml_file, xsd_schema_path, conn, cursor)
        logging.info(f"Processing stat dam data file: {xml_file}")
        process_stat_dam_file(xml_file, xsd_schema_path, conn, cursor)
        logging.info(f"Processing sire data file: {xml_file}")
        process_sire_file(xml_file, xsd_schema_path, conn, cursor)
        logging.info(f"Processing stat sire data file: {xml_file}")
        process_stat_sire_file(xml_file, xsd_schema_path, conn, cursor)
        logging.info(f"Processing jockey data file: {xml_file}")
        process_jockey_file(xml_file, xsd_schema_path, conn, cursor)
        logging.info(f"Processing stat jockey data file: {xml_file}")
        process_stat_jockey_file(xml_file, xsd_schema_path, conn, cursor)
        logging.info(f"Processing trainer data file: {xml_file}")
        process_trainer_file(xml_file, xsd_schema_path, conn, cursor)
        logging.info(f"Processing stat trainer data file: {xml_file}")
        process_stat_trainer_file(xml_file, xsd_schema_path, conn, cursor)
        logging.info(f"Processing runners data file: {xml_file}")
        process_runners_file(xml_file, xsd_schema_path, conn, cursor)
        logging.info(f"Processing runners stats data file: {xml_file}")
        process_runners_stats_file(xml_file, xsd_schema_path, conn, cursor)
        logging.info(f"Processing workout data file: {xml_file}")
        process_workoutdata_file(xml_file, xsd_schema_path, conn, cursor)
        logging.info(f"Processing ppData file: {xml_file}")
        process_ppData_file(xml_file, xsd_schema_path, conn, cursor)
        conn.commit()
        return True  # Return True only if all sections succeeded
        
    except Exception as e:
        logging.error(f"Error processing XML file {xml_file}: {e}")
        conn.rollback()  # Rollback the transaction for safety
        return False  # Indicate failure
    
def process_pluspro_data(conn, pluspro_dir, xsd_schema_path, error_log, processed_files):
    """
    Main function to process PlusPro data.
    This will handle the year-specific subdirectories.
    """
    try:
        # Specify the year or directory to process (e.g., 2022PP)
        year_dirs = ['2022PP', '2023PP', '2024PP', 'Daily']  # Extend this list for more years or other directories
        
        for year_dir in year_dirs:
            pp_data_path = os.path.join(pluspro_dir, year_dir)
            
            if os.path.exists(pp_data_path):
                process_zip_files(pp_data_path, conn, xsd_schema_path, processed_files)
            else:
                logging.warning(f"Directory {pp_data_path} not found for {year_dir}")

        logging.info("PlusPro data ingestion completed successfully.")
    
    except Exception as e:
        logging.error(f"Error during PlusPro data ingestion: {e}")
        with open(error_log, 'a') as error_file:
            error_file.write(f"PlusPro data ingestion error: {e}\n")