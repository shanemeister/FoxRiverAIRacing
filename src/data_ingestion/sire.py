import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from mapping_dictionaries import eqb_to_course_cd

def process_sire_file(xml_file, conn, cursor):
    """
    Process individual XML race data file and insert into the sire table.
    """
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        logging.info(f"Parsed XML successfully: {xml_file}")
        
        # Iterate over each race data
        for race in root.findall('racedata'):
            # Iterate over each horse data
            for horse in race.findall('horsedata'):
                axciskey = horse.find('axciskey').text  # Unique key for the horse
                # Iterate over each stat within the stats_data section
                sire_data = horse.find('sire')
                if sire_data is not None:
                    for sire in sire_data.findall('sire'):
                        stat_breed = sire.find('stat_breed').text
                        sirename = sire.find('sirename').text
                        tmmark = sire.find('tmmark').text
                        stud_fee = sire.find('stud_fee').text
                        # Prepare SQL insert query for the stat_horse table
                        insert_query = """
                            INSERT INTO sire (sirename, axciskey, stat_breed, tmmark, stud_fee)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (sirename, axciskey) DO UPDATE 
                            SET stat_breed = EXCLUDED.stat_breed,
                                tmmark = EXCLUDED.tmmark,
                                stud_fee = EXCLUDED.stud_fee
                        """

                        # Execute the query
                        cursor.execute(insert_query, (
                            sirename, axciskey, stat_breed, tmmark, stud_fee
                        ))

        # Commit the transaction
        conn.commit()
        logging.info("Stat_Horse data processed successfully.")

    except Exception as e:
        # Handle exceptions and rollback in case of error
        logging.error(f"Error processing file {xml_file}: {e}")
        conn.rollback()  # Rollback the transaction in case of an error