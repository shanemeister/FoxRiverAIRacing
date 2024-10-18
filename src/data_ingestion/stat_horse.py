import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from mapping_dictionaries import eqb_to_course_cd

def process_stathorse_file(xml_file, conn, cursor):
    """
    Process individual XML race data file and insert into the stat_horse table.
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
                stats_data = horse.find('stats_data')
                if stats_data is not None:
                    for stat in stats_data.findall('stat'):
                        stat_type = stat.attrib.get('type')  # Get the type attribute of the stat

                        # Extract the stat fields
                        starts = stat.find('starts').text or '0'
                        wins = stat.find('wins').text or '0'
                        places = stat.find('places').text or '0'
                        shows = stat.find('shows').text or '0'
                        earnings = stat.find('earnings').text or '0.00'
                        paid = stat.find('paid').text or '0.00'
                        roi = stat.find('roi').text or None  # ROI might be empty

                        # Prepare SQL insert query for the stat_horse table
                        insert_query = """
                            INSERT INTO stat_horse (type, axciskey, starts, wins, places, shows, earnings, paid, roi)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (type, axciskey) DO UPDATE 
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
                            stat_type, axciskey, starts, wins, places, shows, earnings, paid, roi
                        ))

        # Commit the transaction
        conn.commit()
        logging.info("Stat_Horse data processed successfully.")

    except Exception as e:
        # Handle exceptions and rollback in case of error
        logging.error(f"Error processing file {xml_file}: {e}")
        conn.rollback()  # Rollback the transaction in case of an error