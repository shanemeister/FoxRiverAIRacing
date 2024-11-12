from lxml import etree
from datetime import datetime
import logging
from ingestion_utils import validate_xml, log_rejected_record, parse_date, safe_int

def process_results_scratches_file(xml_file, conn, cursor, xsd_schema_path):
    """
    Process individual XML race data file and insert into the race scratches table.
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
            
            for entry_elem in race_elem.xpath('.//SCRATCH'):
                try:
                    horse_name = entry_elem.xpath('./NAME/text()')[0] if entry_elem.xpath('./NAME/text()') else None
                    breed = entry_elem.xpath('./BREED/text()')[0] if entry_elem.xpath('./BREED/text()') else None
                    reason = entry_elem.xpath('./REASON/text()')[0] if entry_elem.xpath('./REASON/text()') else None
                    last_race_track_code = entry_elem.xpath('./LAST_PP/TRACK/CODE/text()')[0] if entry_elem.xpath('./LAST_PP/TRACK/CODE/text()') else None
                    last_race_track_name = entry_elem.xpath('./LAST_PP/TRACK/NAME/text()')[0] if entry_elem.xpath('./LAST_PP/TRACK/NAME/text()') else None
                    last_race_date = parse_date(entry_elem.xpath('./LAST_PP/RACE_DATE/text()')[0]) if entry_elem.xpath('./LAST_PP/RACE_DATE/text()') else None
                    last_race_number = safe_int(entry_elem.xpath('./LAST_PP/RACE_NUMBER/text()')[0]) if entry_elem.xpath('./LAST_PP/RACE_NUMBER/text()') else None
                    official_finish = safe_int(entry_elem.xpath('./LAST_PP/OFFICIAL_FINISH/text()')[0]) if entry_elem.xpath('./LAST_PP/OFFICIAL_FINISH/text()') else None
                    
                    # Insert the entry data into the database
                    insert_results_scratches_query = """
                        INSERT INTO results_scratches (
                            race_identifier, horse_name, breed, reason, last_race_track_code, 
                            last_race_track_name, last_race_date, last_race_number, official_finish
                        ) VALUES (%s, %s, %s, %s, %s,
                                  %s, %s, %s, %s)
                        ON CONFLICT (race_identifier, horse_name) DO UPDATE 
                    SET breed = EXCLUDED.breed, 
                        reason = EXCLUDED.reason,
                        last_race_track_code = EXCLUDED.last_race_track_code, 
                        last_race_track_name = EXCLUDED.last_race_track_name, 
                        last_race_date = EXCLUDED.last_race_date, 
                        last_race_number = EXCLUDED.last_race_number,
                        official_finish = EXCLUDED.official_finish
                    """
                    cursor.execute(insert_results_scratches_query, (
                        race_identifier, horse_name, breed, reason, last_race_track_code, 
                        last_race_track_name, last_race_date, last_race_number, official_finish
                    ))
                except Exception as scratch_error:
                    has_rejections = True
                    logging.error(f"Error processing entry {race_identifier}: {scratch_error}")
                    rejected_record = {
                        "race_identifier": race_identifier,
                        "horse_name": horse_name,
                        "breed": breed, 
                        "reason": reason,
                        "last_race_track_code": last_race_track_code,
                        "last_race_track_name": last_race_track_name,
                        "last_race_date": last_race_date.isoformat() if last_race_date else None,  # Convert date to string
                        "last_race_number": last_race_number,
                        "official_finish": official_finish
                    }
                    conn.rollback()  # Rollback transaction before logging the rejected record
                    log_rejected_record(conn, 'results_scratches', rejected_record, str(scratch_error))
                    continue  # Skip to the next entry

    except Exception as scratch_error:
        logging.error(f"Error processing results_scratches data in file {xml_file}: {scratch_error}")
        conn.rollback()
        return "error"
    finally:
        conn.commit()

    return "processed_with_rejections" if has_rejections else "processed"