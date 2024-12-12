import os
import json
import logging
from datetime import datetime
import psycopg2
from src.data_ingestion.ingestion_utils import (update_ingestion_status, parse_time, parse_date, extract_course_code, 
    extract_race_date, extract_post_time, convert_gate_name)
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd

def process_tpd_sectionals(conn, data, course_cd, race_date, race_number, filename, post_time):
    has_rejections = False  # Track if any records were rejected
    # logging.info(f"Processing sectionals data from file {filename}")
    cursor = conn.cursor()
    try:
        for sec_info in data:
            # logging.info(f"sec_info content: {sec_info}")
            # logging.info(f"Gate Name (G): {sec_info.get('G', '')}")
            
            # Extract the saddle cloth number as the last two characters of 'I' field
            saddle_cloth_number = sec_info['I'][-2:]          # Extract the last two characters
            saddle_cloth_number = saddle_cloth_number.replace(' ', '')  # Remove all spaces
            if saddle_cloth_number.startswith('0'):
                saddle_cloth_number = saddle_cloth_number[1:]  # Remove leading '0' if present
            # Map each field to the relevant table column
            gate_name = sec_info.get('G', '')
            gate_numeric = convert_gate_name(gate_name)
            length_to_finish = sec_info.get('L', None)
            sectional_time = sec_info.get('S', None)
            running_time = sec_info.get('R', None)
            distance_back = sec_info.get('B', None)
            distance_ran = sec_info.get('D', None)
            number_of_strides = sec_info.get('N', None)
            
            # Ensure post_time is a complete timestamp
            if isinstance(post_time, str) and len(post_time) == 8:  # Check if it's a time-only string
                post_time = f"{race_date} {post_time}"  # Combine with race_date
            elif isinstance(post_time, datetime):  # If it's already a datetime object
                post_time = post_time.strftime('%Y-%m-%d %H:%M:%S')  # Format it as a string
                
            # SQL insert statement
            insert_query = """
                INSERT INTO public.sectionals (
                    course_cd, race_date, race_number, saddle_cloth_number, post_time,
                    gate_name, length_to_finish, sectional_time, running_time, distance_back, 
                    distance_ran, number_of_strides, gate_numeric
                ) VALUES (%s, %s, %s, %s, %s, %s, 
                          %s, %s, %s, %s, %s,
                          %s, %s)
                ON CONFLICT (course_cd, race_date, race_number, saddle_cloth_number, gate_name)
                DO UPDATE SET
                    post_time = EXCLUDED.post_time,
                    length_to_finish = EXCLUDED.length_to_finish,
                    sectional_time = EXCLUDED.sectional_time,
                    running_time = EXCLUDED.running_time,
                    distance_back = EXCLUDED.distance_back,
                    distance_ran = EXCLUDED.distance_ran,
                    number_of_strides = EXCLUDED.number_of_strides,
                    gate_numeric = EXCLUDED.gate_numeric;
            """
            
            try:
                cursor.execute(insert_query, (
                    course_cd, race_date, race_number, saddle_cloth_number, post_time,
                    gate_name, length_to_finish, sectional_time, running_time, distance_back, 
                    distance_ran, number_of_strides, gate_numeric
                ))
                conn.commit()
                logging.info(f"Successfully inserted record for sectionals: {course_cd}")
            except psycopg2.Error as e:
                has_rejections = True  # Track if any records were rejected
                logging.error(f"Error inserting sectional data in file {filename}: {e}")
                conn.rollback()
                #update_ingestion_status(conn, filename, str(e), "Sectionals")
    
    except KeyError as e:
        has_rejections = True
        logging.error(f"Missing key {e} in sectional data from file {filename}")
        #update_ingestion_status(conn, filename, f"Missing key {e}", "Sectionals")
    except TypeError as e:
        has_rejections = True
        logging.error(f"Type error in sectional data from file {filename}: {e}")
        #update_ingestion_status(conn, filename, f"Type error: {e}", "Sectionals")
    finally:
        
        return not has_rejections  # Returns True if no rejections, otherwise False


