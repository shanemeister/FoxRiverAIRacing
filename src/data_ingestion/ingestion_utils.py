import logging
import json
import psycopg2
from datetime import datetime
from datetime import date
import configparser
from lxml import etree
import re
import os
import csv
import argparse
import base64


# Configure logging
logging.basicConfig(filename='/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/ingestion.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def log_status(table_name, status, message=""):
    """Log the status of the ingestion."""
    #logging.info(f"Table: {table_name} - Status: {status} - {message}")

def update_tracking(conn, table_name, status, message=""):
    """Update the ingestion tracking table."""
    try:
        with conn.cursor() as cur:
            query = """
            INSERT INTO ingestion_tracking (table_name, last_processed, status, message)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (table_name)
            DO UPDATE SET last_processed = EXCLUDED.last_processed,
                          status = EXCLUDED.status,
                          message = EXCLUDED.message;
            """
            cur.execute(query, (table_name, datetime.now(), status, message))
        conn.commit()
    except Exception as e:
        logging.error(f"Error updating tracking table for {table_name}: {e}")

def get_db_connection(config):
    """Establish connection using .pgpass for security."""
    return psycopg2.connect(
        host=config['database']['host'],
        database=config['database']['dbname'],
        user=config['database']['user'],
        port=config['database']['port']
        # Password is retrieved from .pgpass
    )

def get_text(element, default=None):
    """
    Safely extract text from an XML element.
    Returns the text if available, otherwise returns the default value.
    """
    return element.text.strip() if element is not None and element.text else default

def validate_xml(xml_file, xsd_file_path):
    """
    Validates an XML file against an XSD schema.
    """
    try:
        # Parse the XSD schema
        with open(xsd_file_path, 'rb') as xsd_file:
            xmlschema_doc = etree.parse(xsd_file)
            xmlschema = etree.XMLSchema(xmlschema_doc)

        # Parse the XML file
        xml_doc = etree.parse(xml_file)

        # Validate the XML document against the schema
        result = xmlschema.validate(xml_doc)
        if result:
            #logging.info(f"XML file {xml_file} is valid.")
            return True
        else:
            # Log the errors
            for error in xmlschema.error_log:
                logging.error(f"XML validation error in {xml_file}: {error.message}")
            return False

    except Exception as e:
        logging.error(f"Exception during XML validation of file {xml_file}: {e}")
        return False

def parse_race_id(race_id):
    """
    Parses the race_id into course_cd, race_date, and post_time.
    The format of race_id is 'coursecode + date + time' (e.g., '71201703091245').
    """
    try:
        # Extract the course code (first two characters)
        course_cd = race_id[:2]  # First 2 characters for course_cd
        
        # Extract the date (next 8 characters)
        race_date_str = race_id[2:10]  # Next 8 characters for date (YYYYMMDD)
        race_date = datetime.strptime(race_date_str, "%Y%m%d").date()  # Convert to date object
        
        # Extract the time (remaining characters)
        race_time_str = race_id[10:14]  # Next 4 characters for time (HHMM)
        post_time = datetime.strptime(f"{race_date_str} {race_time_str}", "%Y%m%d %H%M")  # Combine date and time
        
        return course_cd, race_date, post_time
    except ValueError as e:
        raise ValueError(f"Error parsing race_id '{race_id}': {e}")

def extract_course_code(i_field):
    """
    Extract the course code from the first two characters of the 'I' field.
    """
    return i_field[:2]

def extract_race_date(i_field):
    """
    Extract the race date from the 'I' field in the format 'YYYYMMDD'.
    The race date starts from the third character and is eight digits long.
    
    Parameters:
    - i_field (str): The 'I' field value from which to extract the race date.
    
    Returns:
    - race_date (date): The extracted date in the format YYYY-MM-DD.
    """
    race_date_str = i_field[2:10]  # Extract characters from position 2 to 9 (inclusive)
    return datetime.strptime(race_date_str, '%Y%m%d').date()

def extract_post_time(post_time_str):
    """
    Extracts the time from the PostTime string.
    
    Parameters:
    - post_time_str (str): The full PostTime string in ISO format, e.g., '2023-08-10T18:52:00-04:00'.
    
    Returns:
    - time (time): The extracted time in the format HH:MM:SS.
    """
    post_time = datetime.fromisoformat(post_time_str).time()  # Extract only the time component
    return post_time

def parse_date(date_str, attribute = ''):
    # Ensure date_str is a valid non-empty string that doesn't equal '0'
    if not date_str or date_str.strip() in ('0', '', None):  
        # logging.info(f"Invalid or missing date string: {date_str}, for attritbute {attribute}, returning None")
        return '1970-01-01'  # Return a default date if the input is invalid

    #logging.info(f"Date string: {date_str}")
    
    # Try parsing the date in 'YYYYMMDD' format first
    try:
        return datetime.strptime(date_str, '%Y%m%d').date()
    except ValueError:
        # If that fails, try parsing it in 'YYYY-MM-DD' format
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            # If both parsing attempts fail, log an error and return None
            logging.error(f"Error parsing date {date_str}")
            return '1970-01-01'  # Return None for invalid dates
        
# Function to parse AM/PM formatted time strings

from datetime import datetime

def parse_time(time_str):
    """
    Parse time string, assuming PM if no AM/PM designator is present.
    """
    if time_str:
        try:
            # Try parsing with AM/PM designator if present (e.g., 1:00PM)
            return datetime.strptime(time_str, '%I:%M%p').time()
        except ValueError:
            try:
                # Try parsing as 24-hour format
                parsed_time = datetime.strptime(time_str, '%H:%M').time()
                
                # If parsed hour is between 1 and 11, assume PM if AM/PM is missing
                if 1 <= parsed_time.hour <= 11:
                    parsed_time = parsed_time.replace(hour=(parsed_time.hour + 12) % 24)
                    
                return parsed_time

            except ValueError as e:
                logging.error(f"Error parsing time {time_str}: {e}")
                return None

    return None

def safe_float(value):
    try:
        return float(value) if value not in ['', 'NA', None] else 0.0
    except ValueError:
        logging.error(f"Error converting {value} to float")
        return 0.0

# Function to remove special characters like '.' and '"' from owner_name
def clean_text(text):
    # Replace periods, double quotes, and any other unwanted characters
    # Keep only alphanumeric characters and spaces
    return re.sub(r'[^\w\s]', '', text)

def clean_attribute(attribute):
    """Removes extra spaces from an attribute's value"""
    if attribute is not None:
        return attribute.strip()
    return attribute

# Function to convert fractional odds to probability
def odds_to_probability(odds_str):
    try:
        # Use regex to capture the fractional odds (e.g., '10/1')
        match = re.match(r'(\d+)/(\d+)', odds_str)
        if match:
            numerator, denominator = int(match.group(1)), int(match.group(2))
            return 1 / (numerator / denominator + 1)
        else:
            return None  # Handle cases where odds format is not valid
    except Exception as e:
        print(f"Error converting odds: {e}")
        return None

def validate_numeric_fields(record_data):
    """
    Validate and sanitize numeric fields in the record_data dictionary to ensure they meet precision requirements.
    """
    for key, value in record_data.items():
        if isinstance(value, (int, float)):
            # Ensure the value is within the allowed precision
            if abs(value) >= 10**8:
                record_data[key] = None  # Set to None or handle as appropriate
        elif isinstance(value, dict):
            # Recursively validate nested dictionaries
            validate_numeric_fields(value)

def log_rejected_record(conn, table_name, record_data, error_message):
    """
    Logs a rejected record with the reason for rejection into the rejected_records table.
    
    Parameters:
    - conn: Database connection
    - table_name: Name of the table where the record was supposed to be inserted
    - record_data: Dictionary containing the record data
    - error_message: Error message or reason for rejection
    """
    # Validate and sanitize numeric fields in record_data
    validate_numeric_fields(record_data)

    # Convert JSON fields within record_data to strings if necessary
    formatted_data = {
        key: (json.dumps(value) if isinstance(value, dict) else value)
        for key, value in record_data.items()
    }

    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO rejected_records (table_name, record_data, error_message, timestamp)
            VALUES (%s, %s, %s, %s);
        """, (table_name, json.dumps(formatted_data), error_message, datetime.now()))
        conn.commit()
    except Exception as e:
        logging.error(f"Failed to log rejected record for table {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()

def gen_race_identifier(course_cd, race_date, race_number):
    try:
        if not course_cd or not race_date or race_number is None:
            raise ValueError("Invalid inputs: course_cd, race_date, and race_number must not be empty or None.")

        race_number_int = int(race_number)

        # Convert race_date to string if it's a date object
        if isinstance(race_date, date):
            race_date = race_date.strftime('%Y-%m-%d')

        # Remove dashes from race_date to get a clean string
        formatted_race_date = race_date.replace('-', '')

        # Concatenate data (course_cd + race_date + race_number)
        data = f"{course_cd.upper()}{formatted_race_date}{race_number_int:02d}"

        # Ensure the data is of correct format and length
        if len(data) < 10:
            raise ValueError(f"Generated data '{data}' seems too short, possible invalid inputs.")

        # Encode the data using Base64
        encoded_data = base64.b64encode(data.encode('utf-8')).decode('utf-8')

        return encoded_data

    except (ValueError, TypeError) as e:
        print(f"Error generating race identifier: {e}")
        return None
    
def decode_race_identifier(encoded_data):
    try:
        # Decode the Base64 string back to the original string
        decoded_data = base64.b64decode(encoded_data).decode('utf-8')

        # Extract race_date (next 8 digits), and race_number (last 2 digits)
        race_date = decoded_data[-10:-2]  # Last 10 characters are the date and race number
        race_number = decoded_data[-2:]  # Last 2 characters are the race number

        # Format race_date back to YYYY-MM-DD
        formatted_race_date = f"{race_date[:4]}-{race_date[4:6]}-{race_date[6:]}"

        # Now, the rest of the decoded data is the course_cd
        course_cd = decoded_data[:-10]

        return course_cd, formatted_race_date, int(race_number)

    except (ValueError, TypeError, base64.binascii.Error) as e:
        print(f"Error decoding race identifier: {e}")
        return None

    except (ValueError, TypeError, base64.binascii.Error) as e:
        print(f"Error decoding race identifier: {e}")
        return None
    
from datetime import datetime

def log_ingestion_status(conn, table_name, status, message=None):
    """
    Logs the status of ingestion for a particular table.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO ingestion_tracking (table_name, last_processed, status, message)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (table_name) DO UPDATE 
            SET last_processed = EXCLUDED.last_processed,
                status = EXCLUDED.status,
                message = EXCLUDED.message;
        """, (table_name, datetime.now(), status, message))
        conn.commit()  # Commit the transaction
        cursor.close()  # Close the cursor after execution
    except Exception as e:
        conn.rollback()  # Rollback transaction on error
        logging.error(f"Failed to log ingestion status for {table_name}: {e}")

def log_file_status(conn, file_name, timestamp, status, message=None):
    """
    Logs the status of a file in the ingestion_files table.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO ingestion_files (file_name, last_processed, status, message)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (file_name) 
        DO UPDATE SET last_processed = EXCLUDED.last_processed,
                      status = EXCLUDED.status,
                      message = EXCLUDED.message
        """,
        (file_name, timestamp, status, message)
    )
    conn.commit()
    cursor.close()

def get_unprocessed_files(conn, directory_path):
    """
    Returns a list of unprocessed files by querying ingestion_files 
    for a specific directory path.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT file_name FROM ingestion_files 
        WHERE (status != 'processed' OR status IS NULL)
        AND file_name LIKE %s;
    """, (f"%{directory_path}%",))
    files = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return files

def load_processed_files(conn):
    """
    Loads all previously processed file names from ingestion_files into a set.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT file_name FROM ingestion_files WHERE status = 'processed'")
    processed_files = {row[0] for row in cursor.fetchall()}
    cursor.close()
    return processed_files

def log_rejected_record(conn, table_name, record_data, error_message):
    """
    Logs a rejected record with the reason for rejection into the rejected_records table.
    
    Parameters:
    - conn: Database connection
    - table_name: Name of the table where the record was supposed to be inserted
    - record_data: Dictionary containing the record data
    - error_message: Error message or reason for rejection
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO rejected_records (table_name, record_data, error_message, timestamp)
            VALUES (%s, %s, %s, %s);
        """, (table_name, json.dumps(record_data), error_message, datetime.now()))
        conn.commit()
    except Exception as e:
        logging.error(f"Failed to log rejected record for table {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()
              
def parse_claims(claimed_elem):
    claims = []
    for claim in claimed_elem.findall('CLAIM'):
        claim_info = {
            "claim_number": int(claim.get("NUMBER")),
            "horse": claim.find("HORSE").text,
            "price": int(claim.find("PRICE").text),
            "owner": claim.find("OWNER").text,
            "trainer": {
                "first_name": claim.find("TRAINER/FIRST_NAME").text,
                "last_name": claim.find("TRAINER/LAST_NAME").text,
                "middle_name": claim.find("TRAINER/MIDDLE_NAME").text or "",
                "suffix": claim.find("TRAINER/SUFFIX").text or ""
            }
        }
        claims.append(claim_info)
    return claims  # Return as a list of dictionaries

def safe_int(value):
    """
    Convert a string to an integer, handling 'NA' and other non-integer values.
    """
    try:
        if value == 'NA':
            return None  # or return -1 if you prefer to use a special integer value
        return int(value)
    except (ValueError, TypeError):
        return None  # or return -1 if you prefer to use a special integer value
    
def convert_last_pp_to_json(last_pp_elem):
    """
    Convert the LAST_PP element to a JSON object.
    
    Parameters:
    - last_pp_elem: The LAST_PP element from the XML.
    
    Returns:
    - A JSON object representing the LAST_PP element.
    """
    last_pp_dict = {
        "TRACK": {
            "CODE": last_pp_elem.find('./TRACK/CODE').text,
            "NAME": last_pp_elem.find('./TRACK/NAME').text
        },
        "RACE_DATE": last_pp_elem.find('RACE_DATE').text,
        "RACE_NUMBER": int(last_pp_elem.find('RACE_NUMBER').text),
        "OFL_FINISH": int(last_pp_elem.find('OFL_FINISH').text)
    }
    return json.dumps(last_pp_dict)

def extract_sex_code(sex_elem):
    """
    Extract the CODE element from the SEX element.
    
    Parameters:
    - sex_elem: The SEX element from the XML.
    
    Returns:
    - The value of the CODE element.
    """
    return sex_elem.find('CODE').text

def convert_point_of_call_to_json(point_of_call_elems):
    """
    Convert the POINT_OF_CALL elements to a JSON object.
    
    Parameters:
    - point_of_call_elems: A list of POINT_OF_CALL elements from the XML.
    
    Returns:
    - A JSON object representing the POINT_OF_CALL elements.
    """
    point_of_call_list = []
    for elem in point_of_call_elems:
        point_of_call_list.append({
            "WHICH": elem.get("WHICH"),
            "POSITION": int(elem.find('POSITION').text),
            "LENGTHS": float(elem.find('LENGTHS').text)
        })
    return json.dumps(point_of_call_list)