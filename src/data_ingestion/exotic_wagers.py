import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from ingestion_utils import (validate_xml, log_rejected_record, parse_date, safe_numeric_int, safe_numeric_float, parse_time, get_text)
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd

def process_exotic_wagers_file(xml_file, conn, cursor, xsd_schema_path):
    """
    Process individual XML race data file and insert into the exotic wagers table.
    Validates the XML against the provided XSD schema.
    """
    
    # Validate the XML file first
    if not validate_xml(xml_file, xsd_schema_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        return False # Skip processing this file

    has_rejections = False
    
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        # Initialize variables for logging in case of an exception
        course_cd = course_name = race_date = race_number = None
        
        # Extract course_cd and course_name
        course_cd = eqb_tpd_codes_to_course_cd.get(get_text(root.find('./TRACK/CODE')), 'EQE')
        course_name = get_text(root.find('./TRACK/NAME'))
        race_date = parse_date(root.get("RACE_DATE"))  # Get race date directly from attribute

        # Iterate over each RACE element to extract race-specific details
        for race_elem in root.findall('RACE'):
            race_number = safe_numeric_int(race_elem.get("NUMBER"), 'race_number')  # Attribute, so direct access is fine
            post_time = parse_time(get_text(race_elem.find('POST_TIME'))) or datetime.strptime("00:00", "%H:%M").time()
            
            for wager_elem in race_elem.findall('EXOTIC_WAGERS/WAGER'):
                if wager_elem is not None:
                    try:
                        # Extract wager_id as an attribute, ensuring it’s not missing
                        wager_id = safe_numeric_int(wager_elem.get("NUMBER"), 'wager_id')
                        if wager_id is None:
                            raise ValueError("WAGER NUMBER attribute is missing or invalid")

                        # Extract other fields using get_text with default values
                        wager_type = get_text(wager_elem.find('WAGER_TYPE'))
                        num_tickets = safe_numeric_float(get_text(wager_elem.find('NUM_TICKETS')), 'num_tickets')
                        pool_total = safe_numeric_float(get_text(wager_elem.find('POOL_TOTAL')), 'pool_total')
                        winners = get_text(wager_elem.find('WINNERS'))
                        payoff = safe_numeric_float(get_text(wager_elem.find('PAYOFF')), 'payoff')

                        # Insert the entry data into the database
                        insert_exotic_wagers_query = """
                            INSERT INTO exotic_wagers (
                                wager_id, course_cd, race_date, post_time, race_number, 
                                wager_type, num_tickets, pool_total, winners, payoff
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (wager_id, course_cd, race_date, race_number) DO UPDATE 
                        SET wager_type = EXCLUDED.wager_type,
                            post_time = EXCLUDED.post_time,
                            num_tickets = EXCLUDED.num_tickets,
                            pool_total = EXCLUDED.pool_total,
                            winners = EXCLUDED.winners,
                            payoff = EXCLUDED.payoff
                        """
                        cursor.execute(insert_exotic_wagers_query, (
                            wager_id, course_cd, race_date, post_time, race_number, 
                            wager_type, num_tickets, pool_total, winners, payoff
                        ))
                        conn.commit()  # Ensure each successful operation is committed
                        
                    except Exception as exotic_error:
                        has_rejections = True
                        logging.error(f"Error processing exotic wager in file {xml_file} with wager_id {wager_id}: {exotic_error}")
                        rejected_record = {
                            "wager_id" : wager_id,
                            "course_cd" : course_cd,
                            "race_date" : race_date,
                            "post_time" : post_time,
                            "race_number" : race_number,
                            "wager_type" : wager_type,
                            "num_tickets" : num_tickets,
                            "pool_total" : pool_total,
                            "winners" : winners,
                            "payoff" : payoff
                        }
                        conn.rollback()  # Rollback transaction before logging the rejected record
                        log_rejected_record(conn, 'exotic_wagers', rejected_record, str(exotic_error))
                        continue  # Skip to the next entry

    except Exception as exotic_error:
        logging.error(f"Error processing exotic_wagers data in file {xml_file}: {exotic_error}")
        conn.rollback()
        return False
    
    # Returns True if no rejections, otherwise False
    return not has_rejections