import os
import zipfile
import xml.etree.ElementTree as ET
import logging
from datetime import datetime

# Set up logging configuration
log_file = '/home/exx/myCode/horse-racing/logs/equibase_loader.log'
logging.basicConfig(
    filename=log_file,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Define the base directory for the Equibase data
BASE_DIR = "/home/exx/myCode/horse-racing/Equibase-Sample-Data/2023PPs"

# Output file for processed records
output_file = '/home/exx/myCode/horse-racing/logs/equibase_race_sample.txt'

# Function to parse file name for track code and date
def parse_filename(file_name):
    try:
        # Example filename: SIMD20231221LRL_USA.xml
        file_base = os.path.basename(file_name).replace('.xml', '').replace('.zip', '')
        # Extract the date part (characters 4 to 11 for YYYYMMDD)
        date_str = file_base[4:12]  # SIMDYYYYMMDD
        race_date = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
        # Extract the track code part (characters 12 onward before "_")
        track_code = file_base[12:].split('_')[0]  # Example: LRL, RP, WO, etc.
        return track_code, race_date
    except Exception as e:
        logging.error(f"Error parsing filename {file_name}: {e}")
        return None, None

# Function to display and write XML element contents
def write_element_data(element, output_f, track_code, race_date, level=0):
    indent = "    " * level  # Indentation for nested elements
    tag = element.tag
    attributes = element.attrib
    text = element.text.strip() if element.text and element.text.strip() else None

    # Write the element's tag, attributes, and text content to the output file
    output_f.write(f"{indent}Tag: {tag}\n")
    if attributes:
        output_f.write(f"{indent}Attributes: {attributes}\n")
    if text:
        output_f.write(f"{indent}Text: {text}\n")

    # Recursively write child elements
    for child in element:
        write_element_data(child, output_f, track_code, race_date, level + 1)

# Function to process a single XML file
def process_xml(xml_content, file_name, output_f, record_limit=5):
    try:
        root = ET.fromstring(xml_content)
        logging.info(f"Processing XML from {file_name}")
        
        # Parse the track code and race date from the file name
        track_code, race_date = parse_filename(file_name)
        if not track_code or not race_date:
            return  # Skip this file if we can't parse the required info
        
        output_f.write(f"\nProcessing file: {file_name} (Track: {track_code}, Date: {race_date})\n")
        output_f.write(f"Root tag: {root.tag}\n")
        
        count = 0
        # Assuming the root contains multiple records, limit to processing 'record_limit' number of records
        for element in root:
            if count >= record_limit:
                break
            
            output_f.write("\n--- Start of Race ---\n")
            output_f.write(f"Track: {track_code}, Date: {race_date}, Race Number: {count + 1}\n")  # Add unique identifier
            write_element_data(element, output_f, track_code, race_date)  # Write each element's data recursively
            output_f.write("\n--- End of Race ---\n")
            count += 1
        
    except ET.ParseError as e:
        logging.error(f"Error parsing XML file {file_name}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while processing XML file {file_name}: {e}")

# Function to process the ZIP files
def process_zip_file(zip_file_path, record_limit=5):
    try:
        logging.info(f"Processing ZIP file: {zip_file_path}")
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name.endswith('.xml'):
                    logging.info(f"Found XML file: {file_name}")
                    with zip_ref.open(file_name) as xml_file:
                        xml_content = xml_file.read().decode('utf-8')
                        with open(output_file, 'a') as output_f:
                            process_xml(xml_content, file_name, output_f, record_limit=record_limit)
    except zipfile.BadZipFile as e:
        logging.error(f"Error reading ZIP file {zip_file_path}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while processing ZIP file {zip_file_path}: {e}")

# Function to iterate through all ZIP files in the directory
def process_equibase_data(record_limit=5):
    logging.info("Starting Equibase data processing...")
    for root_dir, dirs, files in os.walk(BASE_DIR):
        for file_name in files:
            if file_name.endswith('.zip'):
                zip_file_path = os.path.join(root_dir, file_name)
                logging.info(f"Found ZIP file: {zip_file_path}")
                process_zip_file(zip_file_path, record_limit=record_limit)

    logging.info("Completed Equibase data processing.")

if __name__ == '__main__':
    # Log the start of the script with a timestamp
    start_time = datetime.now().strftime("%A %I:%M %p")
    logging.info(f"################### equibase_loader.py {start_time} #########################")
    print(f"################### equibase_loader.py {start_time} #########################")

    try:
        process_equibase_data(record_limit=5)  # Limit to 5 records
    except Exception as e:
        logging.error(f"Error during Equibase data processing: {e}")

    # Log the end of the script with a timestamp
    end_time = datetime.now().strftime("%A %I:%M:%S %p")
    logging.info(f"#################### equibase_loader.py completed at {end_time} ####################")
    print(f"#################### equibase_loader.py completed at {end_time} ####################")