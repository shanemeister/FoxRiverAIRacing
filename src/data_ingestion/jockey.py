import xml.etree.ElementTree as ET
import logging
from ingestion_utils import validate_xml, get_text

def process_jockey_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the jockey table.
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
                        logging.warning(f"Missing axciskey for a horse in file {xml_file}. Skipping jockey data.")
                        continue  # Skip this horse if axciskey is missing

                    # Find the jockey data section
                    jockey_data = horse.find('jockey')
                    if jockey_data is not None:
                        # Extract jockey information
                        stat_breed = get_text(jockey_data.find('stat_breed'))
                        jock_disp = get_text(jockey_data.find('jock_disp'))
                        jock_key = get_text(jockey_data.find('jock_key'))
                        j_type = get_text(jockey_data.find('j_type'))

                        if not jock_key:
                            logging.warning(f"Missing jock_key for horse {axciskey} in file {xml_file}. Skipping jockey data.")
                            continue  # Skip if jock_key is missing

                        # Prepare SQL insert query for the jockey table
                        insert_jockey_query = """
                            INSERT INTO jockey (stat_breed, jock_disp, jock_key, j_type)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (jock_key) DO UPDATE 
                            SET stat_breed = EXCLUDED.stat_breed,
                                jock_disp = EXCLUDED.jock_disp,
                                j_type = EXCLUDED.j_type
                        """

                        # Execute the query for jockey data
                        cursor.execute(insert_jockey_query, (
                            stat_breed, jock_disp, jock_key, j_type
                        ))
                    else:
                        logging.warning(f"No jockey data found for horse {axciskey} in file {xml_file}.")
                except Exception as e:
                    logging.error(f"Error processing jockey data for horse {axciskey} in file {xml_file}: {e}")
                    conn.rollback()  # Rollback the transaction for this horse
                    continue  # Skip to the next horse

        # Commit the transaction after all jockey data has been processed
        conn.commit()
        
    except Exception as e:
        # Handle exceptions and rollback in case of error
        logging.error(f"Error processing jockey data in file {xml_file}: {e}")
        conn.rollback()  # Rollback the transaction in case of an error