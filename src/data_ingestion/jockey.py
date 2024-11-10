import xml.etree.ElementTree as ET
import logging
from ingestion_utils import validate_xml, get_text, log_rejected_record

def process_jockey_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the jockey table.
    Validates the XML against the provided XSD schema.
    """
    # Validate the XML file first
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
                        logging.warning(f"Missing axciskey for a horse in file {xml_file}. Skipping jockey data.")
                        continue  # Skip this horse if axciskey is missing

                    # Find the jockey data section
                    jockey_data = horse.find('jockey')
                    if jockey_data is not None:
                        # Extract jockey information
                        stat_breed = get_text(jockey_data.find('stat_breed'))
                        jock_disp = get_text(jockey_data.find('jock_disp'))
                        jock_key = get_text(jockey_data.find('jock_key'))
                        j_type = get_text(jockey_data.find('j_type'))

                        if not jock_key:
                            logging.warning(f"Missing jock_key for horse {axciskey} in file {xml_file}. Skipping jockey data.")
                            continue  # Skip if jock_key is missing

                        # Prepare SQL insert query for the jockey table
                        insert_jockey_query = """
                            INSERT INTO jockey (stat_breed, jock_disp, jock_key, j_type)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (jock_key) DO UPDATE 
                            SET stat_breed = EXCLUDED.stat_breed,
                                jock_disp = EXCLUDED.jock_disp,
                                j_type = EXCLUDED.j_type
                        """
                        try:
                                
                            # Execute the query for jockey data
                            cursor.execute(insert_jockey_query, (
                                stat_breed, jock_disp, jock_key, j_type
                            ))
                        except Exception as horse_error:
                            has_rejections = True
                            logging.error(f"Error processing horse {horse}: {horse_error}")
                            # Prepare and log rejected record
                            rejected_record = {
                                'stat_breed': stat_breed,
                                'jock_disp': jock_disp,
                                'jock_key': jock_key,
                                'j_type': j_type
                            }
                            conn.rollback()  # Rollback the transaction before logging the rejected record
                            log_rejected_record(conn, 'jockey_data', rejected_record, str(horse_error))
                            continue  # Skip to the next race record
                    else:
                        logging.warning(f"No jockey data found for horse {axciskey} in file {xml_file}.")
                        continue  # Skip if no trainer data is found

                except Exception as e:
                    has_rejections = True
                    conn.rollback()  # Rollback the transaction before logging the rejected record
                    log_rejected_record(conn, 'jockey_data', rejected_record, str(e))
                    continue  # Skip to the next race record
            
        return not has_rejections  # Returns True if no rejections, otherwise False

    except Exception as e:
        logging.error(f"Critical error processing horse data file {xml_file}: {e}")
        conn.rollback()  # Rollback transaction if an error occurred
        return False