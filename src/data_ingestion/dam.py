import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from ingestion_utils import validate_xml, get_text, log_rejected_record, update_ingestion_status

def process_dam_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the dam table.
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
                        logging.warning(f"Missing axciskey for a horse in file {xml_file}. Skipping dam data.")
                        continue  # Skip this horse if axciskey is missing
                    
                    # Find the dam data section
                    dam_data = horse.find('dam')
                    if dam_data is not None:
                        # Extract dam information
                        damname = get_text(dam_data.find('damname'))
                        if not damname:
                            logging.warning(f"Missing damname for horse {axciskey} in file {xml_file}. Skipping dam data.")
                            continue  # Skip this dam if damname is missing
                        
                        stat_breed = get_text(dam_data.find('stat_breed'))
                        damsire = get_text(dam_data.find('damsire'))
                        
                        # Prepare SQL insert query for the dam table
                        insert_query = """
                            INSERT INTO dam (damname, axciskey, stat_breed, damsire)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (axciskey) DO UPDATE 
                            SET stat_breed = EXCLUDED.stat_breed,
                                damname = EXCLUDED.damname,
                                damsire = EXCLUDED.damsire
                        """
                        try:
                            # Execute the query
                            cursor.execute(insert_query, (
                                damname, axciskey, stat_breed, damsire
                            ))
                            conn.commit()  # Commit the transaction
                            #logging.info(f"Inserted dam data for horse {axciskey} in file {xml_file}.")
                        except Exception as horse_error:
                            has_rejections = True
                            logging.error(f"Error processing horse {horse}: {horse_error}")
                            # Prepare and log rejected record
                            rejected_record = {
                                'damname': damname,
                                'axciskey': axciskey,
                                'stat_breed': stat_breed,
                                'damsire': damsire
                            }
                            conn.rollback()  # Rollback the transaction before logging the rejected record
                            log_rejected_record(conn, 'dam_data', rejected_record, str(horse_error))
                            continue  # Skip to the next race record

                    else:
                        logging.warning(f"No dam data found for horse {axciskey} in file {xml_file}.")
                except Exception as e:
                    has_rejections = True
                    conn.rollback()  # Rollback the transaction before logging the rejected record
                    log_rejected_record(conn, 'dam_data', rejected_record, str(e))
                    continue  # Skip to the next race record
        
        return not has_rejections  # Returns True if no rejections, otherwise False

    except Exception as e:
        logging.error(f"Critical error processing horse data file {xml_file}: {e}")
        conn.rollback()  # Rollback transaction if an error occurred
        return False