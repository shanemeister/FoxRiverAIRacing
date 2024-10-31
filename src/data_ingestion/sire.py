import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from ingestion_utils import validate_xml, get_text

def process_sire_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the sire table.
    """
    
        # **Validate the XML file first**
    if not validate_xml(xml_file, xsd_file_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        return  # Skip processing this file

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        # Iterate over each race data
        for race in root.findall('racedata'):
            # Iterate over each horse data
            for horse in race.findall('horsedata'):
                try:
                    axciskey = get_text(horse.find('axciskey'))
                    if not axciskey:
                        logging.warning(f"Missing axciskey for a horse in file {xml_file}. Skipping sire data.")
                        continue  # Skip this horse if axciskey is missing
                    
                    # Find the sire data section
                    sire_data = horse.find('sire')
                    if sire_data is not None:
                        # Extract sire information
                        sirename = get_text(sire_data.find('sirename'))
                        if not sirename:
                            logging.warning(f"Missing sirename for horse {axciskey} in file {xml_file}. Skipping sire data.")
                            continue  # Skip this sire if sirename is missing
                        
                        stat_breed = get_text(sire_data.find('stat_breed'))
                        tmmark = get_text(sire_data.find('tmmark'))
                        stud_fee = get_text(sire_data.find('stud_fee'))
                        
                        # Prepare SQL insert query for the sire table
                        insert_query = """
                            INSERT INTO sire (sirename, axciskey, stat_breed, tmmark, stud_fee)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (axciskey) DO UPDATE 
                            SET sirename = EXCLUDED.sirename,
                                stat_breed = EXCLUDED.stat_breed,
                                tmmark = EXCLUDED.tmmark,
                                stud_fee = EXCLUDED.stud_fee
                        """
    
                        # Execute the query
                        cursor.execute(insert_query, (
                            sirename, axciskey, stat_breed, tmmark, stud_fee
                        ))
                    else:
                        logging.warning(f"No sire data found for horse {axciskey} in file {xml_file}.")
                except Exception as e:
                    logging.error(f"Error processing sire data for horse {axciskey} in file {xml_file}: {e}")
                    conn.rollback()  # Rollback the transaction for this sire record
                    continue  # Skip to the next horse/sire

        # Commit the transaction after all sire data has been processed
        conn.commit()
        
    except Exception as e:
        # Handle exceptions and rollback in case of error
        logging.error(f"Error processing sire data in file {xml_file}: {e}")
        conn.rollback()  # Rollback the transaction in case of an error