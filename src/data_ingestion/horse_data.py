import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from ingestion_utils import validate_xml, get_text

# from mapping_dictionaries import eqb_to_course_cd  # Not used in this function

def process_horsedata_file(xml_file, xsd_file_path, conn, cursor):
    """
    Process individual XML race data file and insert into the horse table.
    """
    # **Validate the XML file first**
    if not validate_xml(xml_file, xsd_file_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        return  # Skip processing this file

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        # Assuming the root has race data
        for race in root.findall('racedata'):
            # Now, process horse-specific data within this race
            for horse in race.findall('horsedata'):
                try:
                    horse_name = get_text(horse.find('horse_name'))
                    axciskey = get_text(horse.find('axciskey'))
                    foal_date = get_text(horse.find('foal_date'))
                    sex = get_text(horse.find('sex'))
                    wh_foaled = get_text(horse.find('wh_foaled'))
                    color = get_text(horse.find('color'))

                    # Validate required fields
                    if not axciskey or not horse_name:
                        logging.warning(f"Missing axciskey or horse_name for a horse in file {xml_file}. Skipping.")
                        continue  # Skip this horse

                    # Prepare SQL insert query for the horse table
                    insert_query = """
                        INSERT INTO horse (axciskey, horse_name, foal_date, sex, wh_foaled, color)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (axciskey) DO UPDATE 
                        SET horse_name = EXCLUDED.horse_name,
                            foal_date = EXCLUDED.foal_date,
                            sex = EXCLUDED.sex,
                            wh_foaled = EXCLUDED.wh_foaled,
                            color = EXCLUDED.color
                    """
                    
                    # Execute the query
                    cursor.execute(insert_query, (
                        axciskey, horse_name, foal_date, sex, wh_foaled, color
                    ))
                except Exception as e:
                    logging.error(f"Error processing horse {axciskey} in file {xml_file}: {e}")
                    conn.rollback()  # Rollback the transaction for this horse
                    continue  # Skip to the next horse

        # Commit the transaction after all horses are processed
        conn.commit()
        
    except Exception as e:
        logging.error(f"Error processing file {xml_file}: {e}")
        conn.rollback()