import xml.etree.ElementTree as ET
import json
import logging
from ingestion_utils import (
    validate_xml, get_text, parse_time, parse_date, safe_int, safe_float,
    gen_race_identifier, log_rejected_record, convert_last_pp_to_json, convert_point_of_call_to_json
)
from datetime import datetime

def process_jockey_current_file(xml_file, conn, cursor, xsd_schema_path):
    if not validate_xml(xml_file, xsd_schema_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        return "error"  # Skip processing this file
    
    has_rejections = False  # Track if any records were rejected
    
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        for race_elem in root.findall('RACE'):
            for entry_elem in race_elem.findall('ENTRY'):
                jock_key = get_text(entry_elem.find('./JOCKEY/KEY'))
                first_name = get_text(entry_elem.find('./JOCKEY/FIRST_NAME'))  
                last_name = get_text(entry_elem.find('./JOCKEY/LAST_NAME'))
                middle_name = get_text(entry_elem.find('./JOCKEY/MIDDLE_NAME'))
                suffix = get_text(entry_elem.find('./JOCKEY/SUFFIX'))
                j_type = get_text(entry_elem.find('./JOCKEY/TYPE'))

                # Insert into the jockey table
                insert_jockey_query = """
                    INSERT INTO jockey (
                        jock_key, first_name, last_name, middle_name, suffix, j_type
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (jock_key) DO UPDATE 
                    SET first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        middle_name = EXCLUDED.middle_name,
                        suffix = EXCLUDED.suffix,
                        j_type = EXCLUDED.j_type
                """
                try:
                    cursor.execute(insert_jockey_query, (
                        jock_key, first_name, last_name, middle_name, suffix, j_type
                    ))
                except Exception as entry_error:
                    has_rejections = True
                    logging.error(f"Error processing entry {jock_key}: {entry_error}")
                    rejected_record = {
                        "jock_key": jock_key,
                        "first_name": first_name,
                        "last_name": last_name, 
                        "middle_name": middle_name,
                        "suffix": suffix,
                        "j_type": j_type
                    }
                    conn.rollback()  # Rollback transaction before logging the rejected record
                    log_rejected_record(conn, 'jockey', rejected_record, str(entry_error))
                    continue  # Skip to the next entry

    except Exception as race_error:
        logging.error(f"Error processing jockey data in file {xml_file}: {race_error}")
        conn.rollback()
        return "error"
    finally:
        conn.commit()

    return "processed_with_rejections" if has_rejections else "processed"