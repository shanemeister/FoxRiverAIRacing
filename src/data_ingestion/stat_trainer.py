# stat_trainer.py

import xml.etree.ElementTree as ET
import logging
from ingestion_utils import validate_xml, get_text

def process_stat_trainer_file(xml_file, xsd_schema_path, conn, cursor):
    """
    Process individual XML race data file and insert into the stat_trainer table.
    Validates the XML against the provided XSD schema.
    """
    # Validate the XML file against the XSD schema
    if not validate_xml(xml_file, xsd_schema_path):
        logging.error(f"Validation failed for XML file: {xml_file}")
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
                        logging.warning(f"Missing axciskey for a horse in file {xml_file}. Skipping stat_trainer data.")
                        continue  # Skip this horse if axciskey is missing

                    # Find the trainer data section
                    trainer_data = horse.find('trainer')
                    if trainer_data is not None:
                        train_key = get_text(trainer_data.find('train_key'))
                        tran_disp = get_text(trainer_data.find('tran_disp'))
                        if not train_key:
                            logging.warning(f"Missing train_key for horse {axciskey} in file {xml_file}. Skipping stat_trainer data.")
                            continue  # Skip if train_key is missing

                        # Extract stats_data
                        stats_data = trainer_data.find('stats_data')
                        if stats_data is not None:
                            for stat in stats_data.findall('stat'):
                                try:
                                    stat_type = stat.get('type')
                                    if not stat_type:
                                        logging.warning(f"Missing stat type for trainer '{tran_disp}' (train_key: {train_key}) in file {xml_file}. Skipping this stat.")
                                        continue  # Skip this stat if type is missing

                                    # Extract and convert stat fields
                                    starts = float(get_text(stat.find('starts'), '0'))
                                    wins = float(get_text(stat.find('wins'), '0'))
                                    places = float(get_text(stat.find('places'), '0'))
                                    shows = float(get_text(stat.find('shows'), '0'))
                                    earnings = get_text(stat.find('earnings'), '0.00')
                                    paid = float(get_text(stat.find('paid'), '0.00'))
                                    roi = get_text(stat.find('roi'), '0.00')

                                    # Prepare SQL insert query for the stat_trainer table
                                    insert_stat_trainer_query = """
                                        INSERT INTO stat_trainer (starts, wins, places, shows, earnings, paid, roi, type, train_key)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                        ON CONFLICT (type, train_key) DO UPDATE 
                                        SET starts = EXCLUDED.starts,
                                            wins = EXCLUDED.wins,
                                            places = EXCLUDED.places,
                                            shows = EXCLUDED.shows,
                                            earnings = EXCLUDED.earnings,
                                            paid = EXCLUDED.paid,
                                            roi = EXCLUDED.roi
                                    """

                                    # Execute the query for trainer stats
                                    cursor.execute(insert_stat_trainer_query, (
                                        starts, wins, places, shows, earnings, paid, roi, stat_type, train_key
                                    ))
                                except Exception as e:
                                    logging.error(f"Error processing stat '{stat_type}' for trainer '{tran_disp}' (train_key: {train_key}) of horse {axciskey} in file {xml_file}: {e}")
                                    conn.rollback()  # Rollback the transaction for this stat record
                                    continue  # Skip to the next stat
                        else:
                            logging.warning(f"No stats_data found for trainer '{tran_disp}' (train_key: {train_key}) of horse {axciskey} in file {xml_file}.")
                    else:
                        logging.warning(f"No trainer data found for horse {axciskey} in file {xml_file}.")
                except Exception as e:
                    logging.error(f"Error processing stat_trainer data for horse {axciskey} in file {xml_file}: {e}")
                    conn.rollback()  # Rollback the transaction for this horse
                    continue  # Skip to the next horse

        # Commit the transaction after all stat_trainer data has been processed
        conn.commit()
        
    except Exception as e:
        # Handle exceptions and rollback in case of error
        logging.error(f"Error processing stat_trainer data in file {xml_file}: {e}")
        conn.rollback()  # Rollback the transaction in case of an error