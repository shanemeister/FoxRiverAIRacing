import os
import json
import logging
from datetime import datetime
import psycopg2
from src.data_ingestion.ingestion_utils import update_ingestion_status, parse_time, parse_date, extract_course_code, extract_race_date
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd

def process_tpd_gpsdata(conn, data, course_cd, race_date, post_time, race_number, filename):
    has_rejections = False  # Track if any records were rejected
    cursor = conn.cursor()
    try:
        for sec_info in data:
            #logging.info(f"Processing GPS data from file {filename}")
            #logging.info(f"sec_info content: {sec_info}")

            # Extract the saddle cloth number as the last two characters of 'I' field
            saddle_cloth_number = sec_info['I'][-2:]          # Extract the last two characters
            saddle_cloth_number = saddle_cloth_number.replace(' ', '')  # Remove all spaces
            if saddle_cloth_number.startswith('0'):
                saddle_cloth_number = saddle_cloth_number[1:]  # Remove leading '0' if present
            time_stamp = sec_info.get('T', None)  # UTC time in ISO 8601 format
            longitude = sec_info.get('X', None)
            latitude = sec_info.get('Y', None)
            speed = sec_info.get('V', None)
            progress = sec_info.get('P', None)
            stride_frequency = sec_info.get('SF', None)
            
            # Ensure post_time is a complete timestamp
            if isinstance(post_time, str) and len(post_time) == 8:  # Check if it's a time-only string
                post_time = f"{race_date} {post_time}"  # Combine with race_date
            elif isinstance(post_time, datetime):  # If it's already a datetime object
                post_time = post_time.strftime('%Y-%m-%d %H:%M:%S')  # Format it as a string

            # Insert SQL for gpspoint data
            insert_query = """
                INSERT INTO public.gpspoint (
                    course_cd, race_date, race_number, saddle_cloth_number, time_stamp, post_time,
                    longitude, latitude, speed, progress, stride_frequency, location
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                ON CONFLICT (course_cd, race_date, race_number, saddle_cloth_number, time_stamp)
                DO UPDATE SET
                    longitude = EXCLUDED.longitude,
                    latitude = EXCLUDED.latitude,
                    speed = EXCLUDED.speed,
                    progress = EXCLUDED.progress,
                    stride_frequency = EXCLUDED.stride_frequency,
                    location = EXCLUDED.location,
                    post_time = EXCLUDED.post_time;
            """
            
            try:
                cursor.execute(insert_query, (
                    course_cd, race_date, race_number, saddle_cloth_number, time_stamp, post_time,
                    longitude, latitude, speed, progress, stride_frequency,
                    longitude, latitude  # For location as ST_MakePoint
                ))
                conn.commit()
                logging.info(f"Successfully inserted record for saddle_cloth_number: {saddle_cloth_number}, time_stamp: {time_stamp}")
            except psycopg2.Error as e:
                has_rejections = True  # Track if any records were rejected
                logging.error(f"Error inserting GPS data in file {filename}: {e}")
                conn.rollback()
                update_ingestion_status(conn, filename, str(e), "GPSData")
    
    except KeyError as e:
        has_rejections = True
        logging.error(f"Missing key {e} in GPS data from file {filename}")
        update_ingestion_status(conn, filename, f"Missing key {e}", "GPSData")
    except TypeError as e:
        has_rejections = True
        logging.error(f"Type error in GPS data from file {filename}: {e}")
        update_ingestion_status(conn, filename, f"Type error: {e}", "GPSData")
    finally:
        cursor.close()
        return not has_rejections  # Returns True if no rejections, otherwise False