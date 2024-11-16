# trainer.py

import xml.etree.ElementTree as ET
import logging
from ingestion_utils import validate_xml, get_text, log_rejected_record, update_ingestion_status

def process_trainer_file(xml_file, xsd_schema_path, conn, cursor):
    """
    Process individual XML race data file and insert into the trainer table.
    Validates the XML against the provided XSD schema.
    """
    # Validate the XML file against the XSD schema
    if not validate_xml(xml_file, xsd_schema_path):
        logging.error(f"Validation failed for XML file: {xml_file}")
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
                        logging.warning(f"Missing axciskey for a horse in file {xml_file}. Skipping trainer data.")
                        continue  # Skip this horse if axciskey is missing

                    # Find the trainer data section
                    trainer_data = horse.find('trainer')
                    if trainer_data is not None:
                        # Extract trainer information
                        stat_breed = get_text(trainer_data.find('stat_breed'))
                        tran_disp = get_text(trainer_data.find('tran_disp'))
                        train_key = get_text(trainer_data.find('train_key'))
                        t_type = get_text(trainer_data.find('t_type'))

                        if not train_key:
                            logging.warning(f"Missing train_key for horse {axciskey} in file {xml_file}. Skipping trainer data.")
                            continue  # Skip if train_key is missing

                        # Prepare SQL insert query for the trainer table
                        insert_trainer_query = """
                            INSERT INTO trainer (stat_breed, tran_disp, train_key, t_type)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (train_key) DO UPDATE 
                            SET stat_breed = EXCLUDED.stat_breed,
                                tran_disp = EXCLUDED.tran_disp,
                                t_type = EXCLUDED.t_type
                        """
                        try:    
                            # Execute the query for trainer data
                            cursor.execute(insert_trainer_query, (
                                stat_breed, tran_disp, train_key, t_type
                            ))
                            conn.commit()  # Commit the transaction
                        except Exception as trainer_error:
                            has_rejections = True
                            logging.error(f"Error processing horse {horse}: {trainer_error}")
                            # Prepare and log rejected record
                            rejected_record = {
                                'stat_breed': stat_breed,
                                'tran_disp': tran_disp,
                                'train_key': train_key,
                                't_type': t_type
                            }
                            conn.rollback()  # Rollback the transaction before logging the rejected record
                            log_rejected_record(conn, 'trainer', rejected_record, str(trainer_error))
                            continue  # Skip to the next race record
                    else:
                        logging.warning(f"No trainer data found for horse {axciskey} in file {xml_file}.")
                        continue  # Skip if no trainer data is found

                except Exception as e:
                    has_rejections = True
                    conn.rollback()  # Rollback the transaction before logging the rejected record
                    log_rejected_record(conn, 'trainer', rejected_record, str(e))
                    continue  # Skip to the next race record
            
        return not has_rejections  # Returns True if no rejections, otherwise False

    except Exception as e:
        logging.error(f"Critical error processing horse data file {xml_file}: {e}")
        conn.rollback()  # Rollback transaction if an error occurred
        return False