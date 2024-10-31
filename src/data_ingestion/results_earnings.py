from lxml import etree
from datetime import datetime
import logging
from ingestion_utils import validate_xml, log_rejected_record, parse_date, gen_race_identifier, safe_int, safe_float

def process_results_earnings_file(xml_file, conn, cursor, xsd_schema_path):
    """
    Process individual XML race data file and insert into the results_earnings table.
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
            
            # Access EARNING_SPLITS
            earning_splits_elem = race_elem.xpath('.//EARNING_SPLITS')[0] if race_elem.xpath('.//EARNING_SPLITS') else None
            if earning_splits_elem is not None:
                split_num = 1  # Initialize split number counter

                # Iterate over each split element within EARNING_SPLITS
                for split in earning_splits_elem:
                    earning_amount = safe_float(split.text) if split.text else None

                    if earning_amount is not None:
                        # Insert the split data into the results_earnings table
                        insert_results_earnings_query = """
                            INSERT INTO results_earnings (
                                race_identifier, split_num, earning_amount
                            ) VALUES (%s, %s, %s)
                            ON CONFLICT (race_identifier, split_num) DO UPDATE
                            SET earning_amount = EXCLUDED.earning_amount
                        """
                        try:
                            cursor.execute(insert_results_earnings_query, (
                                race_identifier, split_num, earning_amount
                            ))
                        except Exception as split_error:
                            has_rejections = True
                            logging.error(f"Error processing entry {race_identifier}, split {split_num}: {split_error}")
                            rejected_record = {
                                "race_identifier": race_identifier,
                                "split_num": split_num,
                                "earning_amount": earning_amount
                            }
                            conn.rollback()  # Rollback transaction before logging the rejected record
                            log_rejected_record(conn, 'results_earnings', rejected_record, str(split_error))
                            continue  # Skip to the next split

                    split_num += 1  # Increment split number for each split

    except Exception as earnings_error:
        logging.error(f"Error processing results_earnings data in file {xml_file}: {earnings_error}")
        conn.rollback()
        return "error"
    finally:
        conn.commit()

    return "processed_with_rejections" if has_rejections else "processed"