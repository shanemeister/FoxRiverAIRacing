import logging
import xml.etree.ElementTree as ET
from src.data_ingestion.ingestion_utils import (
    validate_xml, get_text, safe_int, parse_date,
    log_rejected_record, update_ingestion_status, safe_numeric_int
)
from datetime import datetime
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd

def process_results_earnings_file(xml_file, conn, cursor, xsd_schema_path):
    """
    Process the XML file and insert data into the results_earnings table.
    """
    # Validate the XML file
    if not validate_xml(xml_file, xsd_schema_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        update_ingestion_status(conn, cursor, xml_file, "error", "results_earnings")
        return False

    has_rejections = False

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        course_code = get_text(root.find('./TRACK/CODE'), 'Unknown')
        course_cd = eqb_tpd_codes_to_course_cd.get(course_code, 'UNK')

        # Get race_date from <CHART RACE_DATE="...">
        race_date = parse_date(root.get('RACE_DATE'))

        for race in root.findall('RACE'):
            try:
                # Extract race information
                race_number = safe_int(race.get('NUMBER'))

                # The splits are under <EARNING_SPLITS> in <RACE>
                earning_splits = race.find('EARNING_SPLITS')
                if earning_splits is None:
                    logging.warning(f"No EARNING_SPLITS found in race {race_number}")
                    continue

                # Iterate over splits like <SPLIT_1>, <SPLIT_2>, etc.
                for split_elem in earning_splits:
                    try:
                        split_tag = split_elem.tag  # e.g., 'SPLIT_1'
                        split_num_str = split_tag.replace('SPLIT_', '')
                        split_num = safe_int(split_num_str)

                        earnings = safe_numeric_int(get_text(split_elem), split_tag)

                        if earnings is not None:
                            # Insert the split data into the results_earnings table
                            insert_query = """
                                INSERT INTO results_earnings (
                                    course_cd, race_date, race_number, split_num, earnings
                                ) VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (course_cd, race_date, race_number, split_num) DO UPDATE
                                SET earnings = EXCLUDED.earnings
                            """
                            cursor.execute(insert_query, (
                                course_cd, race_date, race_number, split_num, earnings
                            ))
                            conn.commit()
                            # logging.info(f"Inserted earnings for race {race_number}, split {split_num}")
                        else:
                            # Handle the case where earnings is None
                            has_rejections = True
                            rejected_record = {
                                "course_cd": course_cd,
                                "race_date": race_date.isoformat() if race_date else None,
                                "race_number": race_number,
                                "split_num": split_num,
                                "earnings": earnings
                            }
                            log_rejected_record(conn, cursor, 'results_earnings', rejected_record, "Earnings is None")
                    except Exception as e:
                        has_rejections = True
                        logging.error(f"Error processing split {split_num} in race {race_number}: {e}")
                        # Prepare rejected record
                        rejected_record = {
                            "course_cd": course_cd,
                            "race_date": race_date.isoformat() if race_date else None,
                            "race_number": race_number,
                            "split_num": split_num,
                            "earnings": earnings
                        }
                        log_rejected_record(conn, cursor, 'results_earnings', rejected_record, str(e))
                        conn.rollback()
                        continue  # Skip to the next split
            except Exception as e:
                has_rejections = True
                logging.error(f"Error processing race {race_number}: {e}")
                rejected_record = {
                    "course_cd": course_cd,
                    "race_date": race_date.isoformat() if race_date else None,
                    "race_number": race_number
                }
                log_rejected_record(conn, cursor, 'results_earnings', rejected_record, str(e))
                conn.rollback()
                continue  # Skip to the next race

        return not has_rejections  # Return True if no rejections, else False

    except Exception as e:
        logging.error(f"Critical error processing results_earnings file {xml_file}: {e}")
        conn.rollback()
        update_ingestion_status(conn, cursor, xml_file, "error", "results_earnings")
        return False