import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from ingestion_utils import validate_xml, get_text, log_rejected_record

def process_stathorse_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the stat_horse table.
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
            for horse in race.findall('horsedata'):
                axciskey = get_text(horse.find('axciskey'))
                if not axciskey:
                    logging.warning(f"Missing axciskey for a horse in file {xml_file}. Skipping stat_horse data.")
                    continue  # Skip if axciskey is missing

                stats_data = horse.find('stats_data')
                if stats_data is None:
                    logging.warning(f"No stats_data found for horse {axciskey} in file {xml_file}.")
                    continue

                # Iterate over each stat within the stats_data section
                for stat in stats_data.findall('stat'):
                    try:
                        type_stat = stat.attrib.get('type')
                        if not type_stat:
                            logging.warning(f"Missing stat type for horse {axciskey} in file {xml_file}. Skipping this stat.")
                            continue  # Skip if type_stat is missing

                        # Extract stat fields
                        starts = int(get_text(stat.find('starts'), '0'))
                        wins = int(get_text(stat.find('wins'), '0'))
                        places = int(get_text(stat.find('places'), '0'))
                        shows = int(get_text(stat.find('shows'), '0'))
                        earnings = float(get_text(stat.find('earnings'), '0.00'))
                        paid = float(get_text(stat.find('paid'), '0.00'))
                        roi = get_text(stat.find('roi'))
                        roi = float(roi) if roi is not None else None

                        # Prepare SQL insert query for the stat_horse table
                        insert_query = """
                            INSERT INTO stat_horse (type_stat, axciskey, starts, wins, places, shows, earnings, paid, roi)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (type_stat, axciskey) DO UPDATE 
                            SET starts = EXCLUDED.starts,
                                wins = EXCLUDED.wins,
                                places = EXCLUDED.places,
                                shows = EXCLUDED.shows,
                                earnings = EXCLUDED.earnings,
                                paid = EXCLUDED.paid,
                                roi = EXCLUDED.roi
                        """

                        # Execute the query
                        cursor.execute(insert_query, (
                            type_stat, axciskey, starts, wins, places, shows, earnings, paid, roi
                        ))
                    except Exception as stat_error:
                        # Log and store rejected stat_horse record
                        logging.error(f"Error processing stat '{type_stat}' for horse {axciskey} in file {xml_file}: {stat_error}")
                        rejected_record = {
                            "axciskey": axciskey,
                            "type_stat": type_stat,
                            "starts": starts,
                            "wins": wins,
                            "places": places,
                            "shows": shows,
                            "earnings": earnings,
                            "paid": paid,
                            "roi": roi
                        }
                        log_rejected_record('stat_horse', rejected_record, str(stat_error))
                        continue  # Skip to the next stat entry

        # Commit the transaction after all stat_horse data has been processed
        conn.commit()

    except Exception as e:
        # Handle general exceptions and rollback if needed
        logging.error(f"Error processing stat_horse data in file {xml_file}: {e}")
        conn.rollback()