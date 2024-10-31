import xml.etree.ElementTree as ET
import logging
from ingestion_utils import validate_xml, get_text

def process_stat_jockey_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the stat_jockey table.
    Validates the XML against the provided XSD schema.
    """
    # Validate the XML file first
    if not validate_xml(xml_file, xsd_file_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        return  # Skip processing this file

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
                                try:
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

                                    # Execute the query for jockey stats
                                    cursor.execute(insert_stat_jockey_query, (
                                        jock_key, stat_type, starts, wins, places, shows, earnings, paid, roi
                                    ))
                                except Exception as e:
                                    logging.error(f"Error processing stat '{stat_type}' for jockey '{jock_disp}' (jock_key: {jock_key}) of horse {axciskey} in file {xml_file}: {e}")
                                    conn.rollback()  # Rollback the transaction for this stat record
                                    continue  # Skip to the next stat
                        else:
                            logging.warning(f"No stats_data found for jockey '{jock_disp}' (jock_key: {jock_key}) of horse {axciskey} in file {xml_file}.")
                    else:
                        logging.warning(f"No jockey data found for horse {axciskey} in file {xml_file}.")
                except Exception as e:
                    logging.error(f"Error processing stat_jockey data for horse {axciskey} in file {xml_file}: {e}")
                    conn.rollback()  # Rollback the transaction for this horse
                    continue  # Skip to the next horse

        # Commit the transaction after all stat_jockey data has been processed
        conn.commit()
        
    except Exception as e:
        # Handle exceptions and rollback in case of error
        logging.error(f"Error processing stat_jockey data in file {xml_file}: {e}")
        conn.rollback()  # Rollback the transaction in case of an error