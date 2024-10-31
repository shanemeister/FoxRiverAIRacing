from lxml import etree
from datetime import datetime
import logging
from ingestion_utils import validate_xml, log_rejected_record, parse_date, gen_race_identifier, safe_int, safe_float

def process_exotic_wagers_file(xml_file, conn, cursor, xsd_schema_path):
    """
    Process individual XML race data file and insert into the exotic wagers table.
    Validates the XML against the provided XSD schema.
    """
    
    # Validate the XML file first
    if not validate_xml(xml_file, xsd_schema_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        return  # Skip processing this file

    has_rejections = False
    
    try:
        tree = etree.parse(xml_file)
        root = tree.getroot()

        course_cd_nodes = root.xpath('.//TRACK/CODE/text()')
        course_cd = course_cd_nodes[0] if course_cd_nodes else None
        if course_cd is None:
            raise ValueError("TRACK/CODE element not found or empty")

        race_date = parse_date(root.get("RACE_DATE"))

        for race_elem in root.xpath('.//RACE'):
            race_number = safe_int(race_elem.get("NUMBER"))
            race_identifier = gen_race_identifier(course_cd, race_date, race_number)
            
            for entry_elem in race_elem.xpath('.//EXOTIC_WAGERS/WAGER'):
                try:
                    # Extract wager_id as an attribute, ensuring it’s not missing
                    wager_id = safe_int(entry_elem.get("NUMBER"))
                    if wager_id is None:
                        raise ValueError("WAGER NUMBER attribute is missing or invalid")

                    # Extract other fields
                    wager_type = entry_elem.xpath('./WAGER_TYPE/text()')[0] if entry_elem.xpath('./WAGER_TYPE/text()') else None
                    num_tickets = safe_float(entry_elem.xpath('./NUM_TICKETS/text()')[0]) if entry_elem.xpath('./NUM_TICKETS/text()') else None
                    pool_total = safe_float(entry_elem.xpath('./POOL_TOTAL/text()')[0]) if entry_elem.xpath('./POOL_TOTAL/text()') else None
                    winners = entry_elem.xpath('./WINNERS/text()')[0] if entry_elem.xpath('./WINNERS/text()') else None
                    payoff = safe_float(entry_elem.xpath('./PAYOFF/text()')[0]) if entry_elem.xpath('./PAYOFF/text()') else None

                    # Insert the entry data into the database
                    insert_exotic_wagers_query = """
                        INSERT INTO exotic_wagers (
                            race_identifier, wager_id, wager_type, num_tickets, pool_total, winners, payoff
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (race_identifier, wager_id) DO UPDATE 
                    SET wager_type = EXCLUDED.wager_type,
                        num_tickets = EXCLUDED.num_tickets,
                        pool_total = EXCLUDED.pool_total,
                        winners = EXCLUDED.winners,
                        payoff = EXCLUDED.payoff
                    """
                    cursor.execute(insert_exotic_wagers_query, (
                        race_identifier, wager_id, wager_type, num_tickets, pool_total, winners, payoff
                    ))
                except Exception as exotic_error:
                    has_rejections = True
                    logging.error(f"Error processing entry {race_identifier} with wager_id {wager_id}: {exotic_error}")
                    rejected_record = {
                        "race_identifier" : race_identifier,
                        "wager_id" : wager_id,
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
        return "error"
    finally:
        conn.commit()

    return "processed_with_rejections" if has_rejections else "processed"