import os
import zipfile
import xml.etree.ElementTree as ET
import logging
from ingestion_utils import get_db_connection, update_tracking  # Assuming helper functions for connection & tracking

def process_racedata_file(xml_file, conn, cursor):
    """
    Process individual XML race data file and insert into the racedata table.
    """
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        # Assuming the root has race data, adapt the following to match your XML structure
        for race in root.findall('Race'):
            race_date = race.find('race_date').text
            post_time = race.find('post_time').text
            course_cd = race.find('track').text
            race_number = race.find('raceord').text
            todays_cls = race.find('todays_cls').text
            distance = race.find('dist_disp').text
            dist_unit = race.find('dist_unit').text
            surface_type_code = race.find('course_id').text
            
            race_id = race.find('RaceID').text
            
            race_track = race.find('Track').text
            # Add any other required fields here

            # Insert into database
            cursor.execute("""
                INSERT INTO racedata (race_id, race_date, race_track)
                VALUES (%s, %s, %s)
                ON CONFLICT (race_id) DO UPDATE 
                SET race_date = EXCLUDED.race_date, race_track = EXCLUDED.race_track
            """, (race_id, race_date, race_track))
        conn.commit()

    except Exception as e:
        logging.error(f"Error processing file {xml_file}: {e}")
        conn.rollback()  # Rollback in case of failure

def process_zip_files(directory, conn):
    """
    Extract and process all XML files inside ZIP archives in the specified directory.
    """
    cursor = conn.cursor()
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith("plusxml.zip"):
                zip_path = os.path.join(root, file)
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall("/tmp/racedata")  # Extract XMLs to temp folder
                    for xml_file in os.listdir("/tmp/racedata"):
                        if xml_file.endswith(".xml"):
                            xml_path = os.path.join("/tmp/racedata", xml_file)
                            process_racedata_file(xml_path, conn, cursor)

def racedata(conn, pluspro_dir, resultscharts_dir, error_log):
    """
    Main function to process race data for the racedata table.
    Iterate over the year-specific subdirectories in PlusPro and ResultsCharts.
    """
    try:
        # Iterate over each year directory for PlusPro
        for year_dir in ['2022PP', '2023PP', '2024PP']:
            year_path = os.path.join(pluspro_dir, year_dir)
            if os.path.exists(year_path):
                logging.info(f"Processing PlusPro data for {year_dir}")
                process_zip_files(year_path, conn)
            else:
                logging.warning(f"PlusPro directory for {year_dir} not found")

        # Iterate over each year directory for ResultsCharts
        for year_dir in ['2022R', '2023R', '2024R']:
            year_path = os.path.join(resultscharts_dir, year_dir)
            if os.path.exists(year_path):
                logging.info(f"Processing ResultsCharts data for {year_dir}")
                process_zip_files(year_path, conn)
            else:
                logging.warning(f"ResultsCharts directory for {year_dir} not found")

        logging.info("Race data ingestion completed successfully.")
    except Exception as e:
        logging.error(f"Error during race data ingestion: {e}")
        with open(error_log, 'a') as error_file:
            error_file.write(f"Race data ingestion error: {e}\n")