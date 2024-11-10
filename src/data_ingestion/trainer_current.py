# trainer.py

import xml.etree.ElementTree as ET
import logging
from ingestion_utils import validate_xml, get_text, log_rejected_record, update_ingestion_status

def process_trainer_current_file(xml_file, conn, cursor, xsd_schema_path):
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

        for race_elem in root.findall('RACE'):
            for entry_elem in race_elem.findall('ENTRY'):
                train_key = get_text(entry_elem.find('./TRAINER/KEY'))
                first_name = get_text(entry_elem.find('./TRAINER/FIRST_NAME'))  
                last_name = get_text(entry_elem.find('./TRAINER/LAST_NAME'))
                middle_name = get_text(entry_elem.find('./TRAINER/MIDDLE_NAME'))
                suffix = get_text(entry_elem.find('./TRAINER/SUFFIX'))
                t_type = get_text(entry_elem.find('./TRAINER/TYPE'))

                # Insert into the jockey table
                insert_train_query = """
                    INSERT INTO trainer (
                        train_key, first_name, last_name, middle_name, suffix, t_type
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (train_key) DO UPDATE 
                    SET first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        middle_name = EXCLUDED.middle_name,
                        suffix = EXCLUDED.suffix,
                        t_type = EXCLUDED.t_type
                """
                try:
                    cursor.execute(insert_train_query, (
                        train_key, first_name, last_name, middle_name, suffix, t_type
                    ))
                    conn.commit()  # Ensure each successful operation is committed
                    return "processed"
                except Exception as entry_error:
                    has_rejections = True
                    logging.error(f"Error processing entry {train_key}: {entry_error}")
                    rejected_record = {
                        "train_key": train_key,
                        "first_name": first_name,
                        "last_name": last_name, 
                        "middle_name": middle_name,
                        "suffix": suffix,
                        "t_type": t_type
                    }
                    conn.rollback()  # Rollback transaction before logging the rejected record
                    log_rejected_record(conn, 'trainer_current', rejected_record, str(entry_error))
                    continue  # Skip to the next entry

        return not has_rejections  # Returns True if no rejections, otherwise False
           
    except Exception as e:
        has_rejections = True
        conn.rollback()  # Rollback the transaction before logging the rejected record
        log_rejected_record(conn, 'trainer_current_data', rejected_record, str(e))