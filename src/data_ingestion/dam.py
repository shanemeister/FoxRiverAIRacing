import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from ingestion_utils import validate_xml, get_text

def process_dam_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the dam table.
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
                        logging.warning(f"Missing axciskey for a horse in file {xml_file}. Skipping dam data.")
                        continue  # Skip this horse if axciskey is missing
                    
                    # Find the dam data section
                    dam_data = horse.find('dam')
                    if dam_data is not None:
                        # Extract dam information
                        damname = get_text(dam_data.find('damname'))
                        if not damname:
                            logging.warning(f"Missing damname for horse {axciskey} in file {xml_file}. Skipping dam data.")
                            continue  # Skip this dam if damname is missing
                        
                        stat_breed = get_text(dam_data.find('stat_breed'))
                        damsire = get_text(dam_data.find('damsire'))
                        
                        # Prepare SQL insert query for the dam table
                        insert_query = """
                            INSERT INTO dam (damname, axciskey, stat_breed, damsire)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (axciskey) DO UPDATE 
                            SET stat_breed = EXCLUDED.stat_breed,
                                damname = EXCLUDED.damname,
                                damsire = EXCLUDED.damsire
                        """

                        # Execute the query
                        cursor.execute(insert_query, (
                            damname, axciskey, stat_breed, damsire
                        ))
                    else:
                        logging.warning(f"No dam data found for horse {axciskey} in file {xml_file}.")
                except Exception as e:
                    logging.error(f"Error processing dam data for horse {axciskey} in file {xml_file}: {e}")
                    conn.rollback()  # Rollback the transaction for this dam record
                    continue  # Skip to the next horse/dam

        # Commit the transaction after all dam data has been processed
        conn.commit()
        
    except Exception as e:
        # Handle exceptions and rollback in case of error
        logging.error(f"Error processing dam data in file {xml_file}: {e}")
        conn.rollback()  # Rollback the transaction in case of an error