import logging
import psycopg2
from datetime import datetime

# Configure logging
logging.basicConfig(filename='/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/ingestion.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def log_status(table_name, status, message=""):
    """Log the status of the ingestion."""
    logging.info(f"Table: {table_name} - Status: {status} - {message}")

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

def get_db_connection():
    """Establish connection using .pgpass for security."""
    return psycopg2.connect(
        host="192.168.4.25",
        database="foxriverai",
        user="rshane",
        port="5433"  # We don't specify password since it's in .pgpass
    )
            
from datetime import datetime

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

