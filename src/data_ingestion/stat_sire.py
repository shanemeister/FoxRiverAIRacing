import xml.etree.ElementTree as ET
import logging
from ingestion_utils import validate_xml, get_text, log_rejected_record, update_ingestion_status

def process_stat_sire_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the stat_sire table.
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
            for horse in race.findall('horsedata'):
                try:
                    axciskey = get_text(horse.find('axciskey'))
                    if not axciskey:
                        logging.warning(f"Missing axciskey for a horse in file {xml_file}. Skipping stat_sire data.")
                        continue  # Skip this horse if axciskey is missing
                    
                    # Find the sire data section
                    sire_data = horse.find('sire')
                    if sire_data is not None:
                        sirename = get_text(sire_data.find('sirename'))
                        if not sirename:
                            logging.warning(f"Missing sirename for horse {axciskey} in file {xml_file}. Skipping stat_sire data.")
                            continue  # Skip this sire if sirename is missing
                        
                        # Extract stats_data
                        stats_data = sire_data.find('stats_data')
                        if stats_data is not None:
                            for stat in stats_data.findall('stat'):
                                    stat_type = stat.get('type')
                                    if not stat_type:
                                        logging.warning(f"Missing stat type for sire '{sirename}' of horse {axciskey} in file {xml_file}. Skipping this stat.")
                                        continue  # Skip this stat if type is missing
                                    
                                    # Extract and convert stat fields
                                    starts = int(get_text(stat.find('starts'), '0'))
                                    wins = int(get_text(stat.find('wins'), '0'))
                                    places = int(get_text(stat.find('places'), '0'))
                                    shows = int(get_text(stat.find('shows'), '0'))
                                    earnings = float(get_text(stat.find('earnings'), '0.00'))
                                    paid = float(get_text(stat.find('paid'), '0.00'))
                                    roi_text = get_text(stat.find('roi'), None)
                                    roi = float(roi_text) if roi_text is not None else None
                                    
                                    # Prepare SQL insert query for the stat_sire table
                                    insert_stat_sire_query = """
                                        INSERT INTO stat_sire (sirename, type, axciskey, starts, wins, places, shows, earnings, paid, roi)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                        ON CONFLICT (type, axciskey) DO UPDATE 
                                        SET sirename = EXCLUDED.sirename,
                                            starts = EXCLUDED.starts,
                                            wins = EXCLUDED.wins,
                                            places = EXCLUDED.places,
                                            shows = EXCLUDED.shows,
                                            earnings = EXCLUDED.earnings,
                                            paid = EXCLUDED.paid,
                                            roi = EXCLUDED.roi
                                    """
                                    try:        
                                        # Execute the query
                                        cursor.execute(insert_stat_sire_query, (
                                            sirename, stat_type, axciskey, starts, wins, places, shows, earnings, paid, roi
                                        ))
                                        conn.commit()
                                    except Exception as stat_error:
                                        # Log and store rejected stat_sire record
                                        has_rejections = True
                                        logging.error(f"Error processing stat '{stat}' for horse {axciskey} in file {xml_file}: {stat_error}")
                                        rejected_record = {
                                            "sirename": sirename,
                                            "stat_type": stat_type,
                                            "axciskey": axciskey,
                                            "starts": starts,
                                            "wins": wins,
                                            "places": places,
                                            "shows": shows,
                                            "earnings": earnings,
                                            "paid": paid,
                                            "roi": roi
                                        }
                                        conn.rollback()  # Rollback the transaction before logging the rejected record
                                        log_rejected_record(conn, 'stat_sire', rejected_record, str(stat_error))
                                        continue  # Skip to the next stat

                except Exception as e:
                    has_rejections = True
                    conn.rollback()  # Rollback the transaction before logging the rejected record
                    log_rejected_record(conn, 'horse_data', rejected_record, str(e))
                    continue  # Skip to the next horse record
        
        return not has_rejections  # Returns True if no rejections, otherwise False

    except Exception as e:
        logging.error(f"Critical error processing horse data file {xml_file}: {e}")
        conn.rollback()  # Rollback transaction if an error occurred
        return False