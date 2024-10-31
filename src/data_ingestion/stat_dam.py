import xml.etree.ElementTree as ET
import logging
from ingestion_utils import validate_xml, get_text

def process_stat_dam_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the stat_dam table.
    """
    
        # **Validate the XML file first**
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
                        logging.warning(f"Missing axciskey for a horse in file {xml_file}. Skipping stat_dam data.")
                        continue  # Skip this horse if axciskey is missing
                    
                    # Find the dam data section
                    dam_data = horse.find('dam')
                    if dam_data is not None:
                        damname = get_text(dam_data.find('damname'))
                        if not damname:
                            logging.warning(f"Missing damname for horse {axciskey} in file {xml_file}. Skipping stat_dam data.")
                            continue  # Skip this dam if damname is missing
                        
                        # Extract stats_data
                        stats_data = dam_data.find('stats_data')
                        if stats_data is not None:
                            for stat in stats_data.findall('stat'):
                                try:
                                    stat_type = stat.get('type')
                                    if not stat_type:
                                        logging.warning(f"Missing stat type for dam '{damname}' of horse {axciskey} in file {xml_file}. Skipping this stat.")
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
                                    
                                    # Prepare SQL insert query for the stat_dam table
                                    insert_stat_dam_query = """
                                        INSERT INTO stat_dam (damname, type, axciskey, starts, wins, places, shows, earnings, paid, roi)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                        ON CONFLICT (type, axciskey) DO UPDATE 
                                        SET damname = EXCLUDED.damname,
                                            starts = EXCLUDED.starts,
                                            wins = EXCLUDED.wins,
                                            places = EXCLUDED.places,
                                            shows = EXCLUDED.shows,
                                            earnings = EXCLUDED.earnings,
                                            paid = EXCLUDED.paid,
                                            roi = EXCLUDED.roi
                                    """
    
                                    # Execute the query
                                    cursor.execute(insert_stat_dam_query, (
                                        damname, stat_type, axciskey, starts, wins, places, shows, earnings, paid, roi
                                    ))
                                except Exception as e:
                                    logging.error(f"Error processing stat '{stat_type}' for dam '{damname}' of horse {axciskey} in file {xml_file}: {e}")
                                    conn.rollback()  # Rollback the transaction for this stat record
                                    continue  # Skip to the next stat
                        else:
                            logging.warning(f"No stats_data found for dam '{damname}' of horse {axciskey} in file {xml_file}.")
                    else:
                        logging.warning(f"No dam data found for horse {axciskey} in file {xml_file}.")
                except Exception as e:
                    logging.error(f"Error processing stat_dam data for horse {axciskey} in file {xml_file}: {e}")
                    conn.rollback()  # Rollback the transaction for this horse
                    continue  # Skip to the next horse

        # Commit the transaction after all stat_dam data has been processed
        conn.commit()
         
    except Exception as e:
        # Handle exceptions and rollback in case of error
        logging.error(f"Error processing stat_dam data in file {xml_file}: {e}")
        conn.rollback()  # Rollback the transaction in case of an error