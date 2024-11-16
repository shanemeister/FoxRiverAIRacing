import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from ingestion_utils import validate_xml, get_text, log_rejected_record, update_ingestion_status

def process_sire_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the sire table.
    """
    
        # **Validate the XML file first**
    if not validate_xml(xml_file, xsd_file_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        return  # Skip processing this file
    has_rejections = False  # Track if any records were rejected
    rejected_record = {}  # Store rejected records for logging

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
                        try:  
                            # Execute the query
                            cursor.execute(insert_query, (
                                sirename, axciskey, stat_breed, tmmark, stud_fee
                            ))
                            conn.commit()  # Commit the transaction
                        except Exception as sire_error:
                            has_rejections = True
                            logging.error(f"Error processing horse {horse}: {sire_error}")
                            # Prepare and log rejected record
                            rejected_record = {
                                'axciskey': axciskey,
                                'sirename': sirename,
                                'stat_breed': stat_breed,
                                'tmmark': tmmark,
                                'stud_fee': stud_fee
                            }
                            conn.rollback()  # Rollback the transaction before logging the rejected record
                            log_rejected_record(conn, 'sire', rejected_record, str(sire_error))
                            continue  # Skip to the next race record

                except Exception as e:
                    has_rejections = True
                    conn.rollback()  # Rollback the transaction before logging the rejected record
                    log_rejected_record(conn, 'horse_data', rejected_record, str(e))
                    continue  # Skip to the next race record
        
        return not has_rejections  # Returns True if no rejections, otherwise False

    except Exception as e:
        logging.error(f"Critical error processing horse data file {xml_file}: {e}")
        conn.rollback()  # Rollback transaction if an error occurred
        return False