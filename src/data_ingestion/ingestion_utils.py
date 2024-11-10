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
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd

def truncate_text(value, max_length=255):
    return value[:max_length] if value and len(value) > max_length else value

def parse_filename(filename):
    """
    Parse the TPD filename to extract course code, race date, and post time.

    Parameters:
    - filename: The name of the file.

    Returns:
    - course_cd: The standardized course code.
    - race_date: The race date as a datetime.date object.
    - post_time: The post time as a datetime.time object.
    """
    try:
        # Extract the 2-digit numeric course code
        numeric_course_code = filename[:2]
        
        # Map the numeric code to the standardized course code
        course_cd = eqb_tpd_codes_to_course_cd.get(numeric_course_code)
        if course_cd is None:
            logging.error(f"Unknown numeric course code in filename: {numeric_course_code}")
            raise ValueError(f"Unknown numeric course code: {numeric_course_code}")
        
        # Extract race date and post time
        race_date_str = filename[2:10]   # YYYYMMDD (8 characters)
        post_time_str = filename[10:14]  # HHMM (4 characters)
        
        # Convert race_date_str to a datetime.date object
        race_date = datetime.strptime(race_date_str, '%Y%m%d').date()
        
        # Convert post_time_str to a datetime.time object
        post_time = datetime.strptime(post_time_str, '%H%M').time()
        
        return course_cd, race_date, post_time

    except ValueError as e:
        logging.error(f"Error parsing filename {filename}: {e}")
        raise

def translate_course_code(course_code):
    """Translate an EQB course code to a standardized course code."""
    standardized_code = eqb_tpd_codes_to_course_cd.get(course_code)
    if standardized_code is None:
        logging.error(f"Missing mapping for course code: {course_code}")
        raise ValueError(f"Unknown course code: {course_code}")
    return standardized_code

# Configure logging
logging.basicConfig(filename='/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/ingestion.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def update_ingestion_status(conn, file_name, status, message=None):
    """
    Update the ingestion status for a given file in the ingestion_files table, ensuring only the date is stored.
    """
    try:
        cursor = conn.cursor()
        # Convert datetime to string and take the first 10 characters to get "YYYY-MM-DD"
        date_only = parse_date(str(datetime.now())[:10])
        
        cursor.execute(
            """
            INSERT INTO ingestion_files (file_name, last_processed, status, message)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (file_name, message) 
            DO UPDATE SET 
                status = EXCLUDED.status,
                last_processed = EXCLUDED.last_processed
                """,
            (file_name, date_only, status, message)  # Use date_only for last_processed
        )
        conn.commit()
        cursor.close()
    except Exception as e:
        logging.error(f"Failed to update ingestion status for {file_name}: {e}")
        conn.rollback()
            
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

def extract_course_code(filename):
    """
    Extract the course code from the identifier and return the mapped course_cd.
    Assumes the course code is the first two characters of the identifier.
    """
    try:
        # Extract the first two characters as the course code and strip any whitespace
        course_code = filename[:2].strip()
        #logging.info(f"Extracted course code: {course_code} from identifier: {filename}")
        
        # Look up the course code in the dictionary
        mapped_course_cd = eqb_tpd_codes_to_course_cd.get(course_code)
        
        if mapped_course_cd and len(mapped_course_cd) == 3:
            # logging.info(f"Mapped course code: {mapped_course_cd} for course code: {course_code}")
            return mapped_course_cd
        else:
            logging.error(f"Course code {course_code} not found in mapping dictionary or mapped course code is not three characters.")
            return None
    except Exception as e:
        logging.error(f"Error extracting course code from identifier {filename}: {e}")
        return None

def extract_race_date(filename):
    """
    Extract the race date from the filename.
    
    Parameters:
    - filename: The name of the file.
    
    Returns:
    - The race date as a datetime object.
    """
    try:
        # Assuming the race date is in the format 'ccyyyymmddHHMM' and starts at the 3rd character
        if len(filename) < 10:
            raise ValueError("Filename too short to contain a valid race date")
        race_date_str = filename[2:10]  # Extract 'yyyymmdd' part
        return datetime.strptime(race_date_str, '%Y%m%d').date()
    except ValueError as e:
        raise ValueError(f"Error parsing race date from filename {filename}: {e}")
    
def extract_post_time(post_time_str):
    """
    Extracts the time from either an ISO-format string or a HHMM time string.
    
    Parameters:
    - post_time_str (str): The time string in either ISO format (e.g., '2023-08-10T18:52:00-04:00') 
      or a 4-digit format (e.g., '1852').
    
    Returns:
    - str: The extracted time in the format HH:MM:SS.
    """
    try:
        # Check if it's an ISO-format string
        if "T" in post_time_str:
            post_time = datetime.fromisoformat(post_time_str).time()
            return post_time.strftime("%H:%M:%S")
        else:
            # Assume it's a 4-digit HHMM format
            return f"{post_time_str[:2]}:{post_time_str[2:]}:00"  # Convert to HH:MM:SS
    except ValueError as e:
        raise ValueError(f"Invalid time format for post_time_str '{post_time_str}': {e}")
    
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
import logging

def parse_time(time_str):
    """
    Parse a time string in various formats and return a datetime.time object.
    Handles ISO 8601 timestamps with timezone info if provided.
    """
    if time_str:
        time_str = time_str.strip()
        
        # List of time formats to try, including ISO 8601 with timezone info
        time_formats = [
            '%I:%M%p',  # e.g., '2:33PM'
            '%I%p',     # e.g., '2PM'
            '%H:%M',    # e.g., '14:33' or '11:55'
            '%H%M',     # e.g., '1433'
            '%I:%M',    # e.g., '2:33' (ambiguous AM/PM)
            '%H',       # e.g., '14' or '2'
            '%Y-%m-%dT%H:%M:%S%z',  # e.g., '2024-10-12T11:30:00-04:00'
        ]
        
        for fmt in time_formats:
            try:
                # Attempt to parse the time string
                parsed_datetime = datetime.strptime(time_str, fmt)
                # Return only the time part if timezone-aware format was parsed
                return parsed_datetime.time() if '%z' not in fmt else parsed_datetime.timetz()
            except ValueError:
                continue
        
        logging.error(f"Error parsing time {time_str}: No matching format.")
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
        logging.info(f"Error converting odds: {e}")
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
    - record_data: Dictionary or tuple containing the record data
    - error_message: Error message or reason for rejection
    """
    # Convert tuple to dictionary if necessary
    if isinstance(record_data, tuple):
        record_data = {f"field_{i}": value for i, value in enumerate(record_data)}
    
    # Validate and sanitize numeric fields in record_data
    validate_numeric_fields(record_data)

    # Convert dates and dictionaries within record_data to JSON-compatible formats
    formatted_data = {
        key: (
            value.isoformat() if isinstance(value, (datetime, date)) else
            json.dumps(value) if isinstance(value, dict) else
            value
        )
        for key, value in record_data.items()
    }

    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO rejected_records (table_name, record_data, error_message, timestamp)
            VALUES (%s, %s, %s, %s);
        """, (table_name, json.dumps(formatted_data), error_message, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
    except Exception as e:
        logging.error(f"Failed to log rejected record for table {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        
def gen_race_identifier(course_cd, race_date, race_number):
    """ Generate a race identifier by encoding course code, race date, and race number. """
    
    try:
        if not course_cd or not race_date or race_number is None:
            raise ValueError("Invalid inputs: course_cd, race_date, and race_number must not be empty or None.")

        # Map the course_cd using the dictionary
        if course_cd in eqb_tpd_codes_to_course_cd:
            course_cd = eqb_tpd_codes_to_course_cd[course_cd]
        else:
            raise ValueError(f"Invalid course_cd: {course_cd} not found in mapping dictionary.")

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
        logging.info(f"Error generating race identifier: {e}, course_cd: {course_cd}, race_date: {race_date}, race_number: {race_number}")
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
        logging.info(f"Error decoding race identifier: {e}")
        return None

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

def load_processed_files(conn, dataset_type=None):
    """
    Loads previously processed file names from ingestion_files into a set.
    
    Parameters:
    - conn: Database connection
    - dataset_type (str, optional): Dataset type to filter by (e.g., 'Sectionals', 'GPSData', etc.)
    
    Returns:
    - A set of processed file names for the specified dataset type, or all if no dataset_type is given.
    """
    cursor = conn.cursor()
    if dataset_type:
        # Query to filter by dataset type if specified
        cursor.execute(
            "SELECT file_name FROM ingestion_files WHERE status = 'processed' AND message = %s",
            (dataset_type,)
        )
    else:
        # Backward-compatible query to load all processed files
        cursor.execute("SELECT file_name FROM ingestion_files WHERE status = 'processed'")
    
    # Use set comprehension to gather processed files
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

# Assuming other fields might have overflow constraints as well
def safe_numeric_int(value, field_name, max_value=10**8):
    """Helper function to handle numeric fields with overflow checks."""
    try:
        number = safe_int(value)
        if number is not None and abs(number) >= max_value:
            logging.warning(f"{field_name} value {number} exceeds allowable range; setting to NULL")
            return None
        return number
    except Exception as e:
        logging.error(f"Failed to process {field_name} field: {e}")
        return None
    
def safe_numeric_float(value, field_name, max_value=10**8 - 0.01):
    """Helper function to handle float fields with overflow checks for database constraints."""
    try:
        number = float(value) if value is not None else None
        if number is not None and abs(number) >= max_value:
            logging.warning(f"{field_name} float value {number} exceeds allowable range; setting to NULL")
            return None  # Set to NULL if it exceeds the allowable range
        return number
    except ValueError:
        logging.error(f"Failed to process {field_name} float field with value {value}; returning NULL")
        return None  # Return NULL if conversion fails
    
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
            "CODE": last_pp_elem.find('./TRACK/CODE').text if last_pp_elem.find('./TRACK/CODE') is not None else None,
            "NAME": last_pp_elem.find('./TRACK/NAME').text if last_pp_elem.find('./TRACK/NAME') is not None else None
        },
        "RACE_DATE": last_pp_elem.find('RACE_DATE').text if last_pp_elem.find('RACE_DATE') is not None else None,
        "RACE_NUMBER": int(last_pp_elem.find('RACE_NUMBER').text) if last_pp_elem.find('RACE_NUMBER') is not None else None,
        "OFL_FINISH": int(last_pp_elem.find('OFL_FINISH').text) if last_pp_elem.find('OFL_FINISH') is not None else None
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
        # Use None checks for POSITION and LENGTHS to prevent AttributeError
        position = elem.find('POSITION')
        lengths = elem.find('LENGTHS')
        
        point_of_call_list.append({
            "WHICH": elem.get("WHICH"),
            "POSITION": int(position.text) if position is not None and position.text is not None else None,
            "LENGTHS": float(lengths.text) if lengths is not None and lengths.text is not None else None
        })
    return json.dumps(point_of_call_list)

from datetime import datetime, timedelta
import re

def parse_finish_time(time_str):
    """
    Parses the finish time string. If it matches a standard time format, returns a time object;
    if it's a duration in seconds or minutes (e.g., "99.999" or "38:38"), returns it as a timedelta.
    If no format matches, logs an error and returns the original string.
    """
    # Define common time formats for racing
    time_formats = ["%H:%M:%S", "%M:%S", "%S.%f"]

    # Try parsing as a standard time format
    for fmt in time_formats:
        try:
            return datetime.strptime(time_str, fmt).time()
        except ValueError:
            continue  # Try the next format if the current one fails

    # Handle cases like "38:38" (MM:SS or MM:SS.sss format) by splitting and checking each component
    if re.match(r"^\d{1,2}:\d{2}(\.\d+)?$", time_str):
        minutes, seconds = time_str.split(":")
        seconds = float(seconds)  # Convert seconds to float to handle fractions
        return timedelta(minutes=int(minutes), seconds=seconds)

    # Handle cases like "99.999" which may represent a duration in seconds
    try:
        duration_seconds = float(time_str)
        return timedelta(seconds=duration_seconds)
    except ValueError:
        logging.error(f"Error parsing time {time_str}: No matching format.")
        return time_str  # Return the original string if parsing fails