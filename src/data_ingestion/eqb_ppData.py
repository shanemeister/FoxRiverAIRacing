import os
import zipfile
import logging
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from racedata import process_racedata_file
from horse_data import process_horsedata_file 
from stat_horse import process_stathorse_file 
from sire import process_sire_file
# from jockey import process_jockey_file
# from stat_jockey import process_stat_jockey_file

# EQB to TDP course_cd mapping
eqb_to_course_cd = {
    'BEL': '98',
    # Add more mappings as needed
    # 'CD': '12',
    # 'SAR': '20',
    # etc.
}

def process_zip_files(directory, conn):
    """
    Extract and process all XML files inside ZIP archives in the specified directory.
    """
    cursor = conn.cursor()

    # Define the number of worker threads (adjust based on system resources)
    max_workers = 4

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith("plusxml.zip"):
                logging.info(f"Processing ZIP file: {file}")
                zip_path = os.path.join(root, file)
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall("/tmp/racedata")  # Extract XMLs to temp folder
                    for xml_file in os.listdir("/tmp/racedata"):
                        if xml_file.endswith(".xml"):
                            logging.info(f"Processing XML file: {xml_file}")
                            xml_path = os.path.join("/tmp/racedata", xml_file)
                            process_xml_file_in_parallel(xml_path, conn, cursor, max_workers)

def process_xml_file_in_parallel(xml_file, conn, cursor, max_workers):
    """
    Parse an XML file and process its contents in parallel using multiple threads.
    """  
    try:
        # Parse the XML file
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Use ThreadPoolExecutor to process different tasks concurrently
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit different tasks (processing functions) to the executor
            future_racedata = executor.submit(process_racedata_file, xml_file, conn, cursor)
            future_horsedata = executor.submit(process_horsedata_file, xml_file, conn, cursor)
            future_stathorse = executor.submit(process_stathorse_file, xml_file, conn, cursor)
            future_sire = executor.submit(process_sire_file, xml_file, conn, cursor)
            #future_jockey = executor.submit(process_jockey_file, root, conn, cursor)
            #future_stat_jockey = executor.submit(process_stat_jockey_file, root, conn, cursor)
            # Add more future calls for other modules (trainer, stat_trainer, etc.)

            # Ensure all futures complete
            future_racedata.result()
            future_horsedata.result()
            future_stathorse.result()
            future_sire.result()
            #future_jockey.result()
            #future_stat_jockey.result()

    except Exception as e:
        logging.error(f"Error processing file {xml_file}: {e}")
        conn.rollback()
        
def ppData(conn, pluspro_dir, resultscharts_dir, error_log):
    """
    Main function to process race data for the racedata table.
    Iterate over the year-specific subdirectories in PlusPro and ResultsCharts.
    """
    try:
        # Iterate over each year directory for PlusPro
        for year_dir in ['2022PP', '2023PP', '2024PP', 'Daily']:
            pp_data_path = os.path.join(pluspro_dir, year_dir)
            print(f"Path to data: {pp_data_path}")
            if os.path.exists(pp_data_path):
                logging.info(f"Processing PlusPro data for {year_dir}")
                process_zip_files(pp_data_path, conn)
            else:
                logging.warning(f"PlusPro directory for {year_dir} not found")

        # Iterate over each year directory for ResultsCharts
        for year_dir in ['2022R', '2023R', '2024R', 'Daily']:
            results_data_path = os.path.join(resultscharts_dir, year_dir)
            print(f"Path to data: {results_data_path}")
            if os.path.exists(results_data_path):
                logging.info(f"Processing ResultsCharts data for {year_dir}")
                process_zip_files(results_data_path, conn)
            else:
                logging.warning(f"ResultsCharts directory for {year_dir} not found")

        logging.info("Race data ingestion completed successfully.")
    except Exception as e:
        logging.error(f"Error during race data ingestion: {e}")
        with open(error_log, 'a') as error_file:
            error_file.write(f"Race data ingestion error: {e}\n")
            
