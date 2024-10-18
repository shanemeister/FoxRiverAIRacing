
import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from mapping_dictionaries import eqb_to_course_cd


def process_horsedata_file(xml_file, conn, cursor):
    """
    Process individual XML race data file and insert into the horse table.
    """
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        logging.info(f"Parsed XML successfully: {xml_file}")
        
        # Assuming the root has race data
        for race in root.findall('racedata'):
            # Process each race
            
            # Now, process horse-specific data within this race
            for horse in race.findall('horsedata'):
                horse_name = horse.find('horse_name').text
                axciskey = horse.find('axciskey').text  # Unique key for the horse
                foal_date = horse.find('foal_date').text
                sex = horse.find('sex').text
                wh_foaled = horse.find('wh_foaled').text
                color = horse.find('color').text

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
        
        # Commit the transaction
        conn.commit()
        logging.info("Horse data processed successfully.")

    except Exception as e:
        # Handle exceptions and rollback in case of error
        logging.error(f"Error processing file {xml_file}: {e}")
        conn.rollback()  # Rollback the transaction in case of an error