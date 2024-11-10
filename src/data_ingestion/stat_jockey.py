import xml.etree.ElementTree as ET
import logging
from ingestion_utils import validate_xml, get_text, log_rejected_record, update_ingestion_status

def process_stat_jockey_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the stat_jockey table.
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
                        logging.warning(f"Missing axciskey for a horse in file {xml_file}. Skipping stat_jockey data.")
                        continue  # Skip this horse if axciskey is missing

                    # Find the jockey data section
                    jockey_data = horse.find('jockey')
                    if jockey_data is not None:
                        jock_key = get_text(jockey_data.find('jock_key'))
                        jock_disp = get_text(jockey_data.find('jock_disp'))
                        if not jock_key:
                            logging.warning(f"Missing jock_key for horse {axciskey} in file {xml_file}. Skipping stat_jockey data.")
                            continue  # Skip if jock_key is missing

                        # Extract stats_data
                        stats_data = jockey_data.find('stats_data')
                        if stats_data is not None:
                            for stat in stats_data.findall('stat'):
                                    stat_type = stat.get('type')
                                    if not stat_type:
                                        logging.warning(f"Missing stat type for jockey '{jock_disp}' (jock_key: {jock_key}) in file {xml_file}. Skipping this stat.")
                                        continue  # Skip this stat if type is missing

                                    # Extract and convert stat fields
                                    starts = int(get_text(stat.find('starts'), '0'))
                                    wins = int(get_text(stat.find('wins'), '0'))
                                    places = int(get_text(stat.find('places'), '0'))
                                    shows = int(get_text(stat.find('shows'), '0'))
                                    earnings = get_text(stat.find('earnings'), '0.00')
                                    paid = float(get_text(stat.find('paid'), '0.00'))
                                    roi = get_text(stat.find('roi'), '0.00')

                                    # Prepare SQL insert query for the stat_jockey table (without axciskey)
                                    insert_stat_jockey_query = """
                                        INSERT INTO stat_jockey (jock_key, type, starts, wins, places, shows, earnings, paid, roi)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                        ON CONFLICT (jock_key, type) DO UPDATE 
                                        SET starts = EXCLUDED.starts,
                                            wins = EXCLUDED.wins,
                                            places = EXCLUDED.places,
                                            shows = EXCLUDED.shows,
                                            earnings = EXCLUDED.earnings,
                                            paid = EXCLUDED.paid,
                                            roi = EXCLUDED.roi
                                    """
                                    try:
                                            
                                        # Execute the query for jockey stats
                                        cursor.execute(insert_stat_jockey_query, (
                                            jock_key, stat_type, starts, wins, places, shows, earnings, paid, roi
                                        ))
                                    except Exception as horse_error:
                                        has_rejections = True
                                        logging.error(f"Error processing stat_jockey {horse}: {horse_error}")
                                        # Prepare and log rejected record
                                        rejected_record = {
                                            'jock_key': jock_key,
                                            'stat_type': stat_type,
                                            'starts': starts,
                                            'wins': wins,
                                            'places': places,
                                            'shows': shows,
                                            'earnings': earnings,
                                            'paid': paid,
                                            'roi': roi
                                        }
                                        conn.rollback()  # Rollback the transaction before logging the rejected record
                                        log_rejected_record(conn, 'stat_jockey', rejected_record, str(horse_error))
                                        continue  # Skip to the next race record

                except Exception as e:
                    has_rejections = True
                    conn.rollback()  # Rollback the transaction before logging the rejected record
                    log_rejected_record(conn, 'stat_jockey', rejected_record, str(e))
                    continue  # Skip to the next race record
        
        return not has_rejections  # Returns True if no rejections, otherwise False

    except Exception as e:
        logging.error(f"Critical error processing horse data file {xml_file}: {e}")
        conn.rollback()  # Rollback transaction if an error occurred
        return False