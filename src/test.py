import requests
import time
import csv
from datetime import datetime, timedelta
import pytz

# API Key for TPD
API_KEY = "4cUvs34jBniNffJfY2hdf8kcAEPhvOpF"  # <-- Replace with your actual API key

# List of race codes
RACE_CODES = [
    "76202502111243",
    "76202502111311",
    "76202502111339",
    "76202502111407",
    "76202502111435",
    "76202502111503",
    "76202502111531",
    "76202502111559"
]

BASE_URL = "https://www.tpd.zone/json-rpc/v2/performance/"
CSV_FILENAME = "tpd_race_data.csv"

# Define CSV Columns
CSV_COLUMNS = [
    "timestamp",
    "race_code",
    "runner_sc",
    "runner_number",
    "finish_time",
    "horse_name",
    "position",
    "TTR",
    "ROS",
    "DR",
    "VP",
    "BP",
    "DB",
    "ASL",
    "SLmin",
    "SLmax",
    "ASF",
    "SFmin",
    "SFmax",
    "GT"
]

# Convert race codes into a sorted list by time (last 4 digits)
RACE_CODES.sort(key=lambda x: int(x[-4:]))  # Sort by last 4 digits (time in EST)

# Time settings
EST = pytz.timezone("America/New_York")
CST = pytz.timezone("America/Chicago")

# Timeout settings
MAX_NO_DATA_WAIT = 300  # Stop polling if no data appears in this time (seconds)
DATA_TIMEOUT = 30  # Stop polling if data was appearing but then stops (seconds)


def fetch_race_performance(api_key, race_code):
    """
    Calls the TPD performance endpoint for the given race_code.
    Returns the parsed JSON (dict) if successful, or None on error.
    """
    params = {
        "k": api_key,
        "action": "race",
        "code": race_code
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get("success") is True and "runners" in data:
            return data
        else:
            print(f"Warning: No expected data for race {race_code}:", data)
            return None
    except requests.exceptions.RequestException as e:
        print(f"API request error for race {race_code}:", e)
        return None


def write_rows_to_csv(filename, columns, rows):
    """
    Appends the given rows (list of dicts) to a CSV file,
    creating the file with headers if it doesn't exist.
    """
    file_exists = False
    try:
        with open(filename, "r", encoding="utf-8") as f:
            file_exists = True
    except FileNotFoundError:
        file_exists = False

    with open(filename, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def poll_race(race_code):
    """
    Polls the race at 1-second intervals, then 0.5-second intervals once data is received.
    Stops polling if no data appears for a set period or if data stops coming in.
    """
    print(f"Polling race {race_code}...")

    polling_interval = 1.0  # Start with 1-second intervals
    last_data_time = None  # Tracks the last time we received data
    start_time = time.time()  # Track the start of polling

    while True:
        race_data = fetch_race_performance(API_KEY, race_code)
        current_time = time.time()

        if race_data and "runners" in race_data:
            timestamp_str = datetime.utcnow().isoformat()
            runner_dict = race_data["runners"]

            csv_rows = []
            for runner_cloth, runner_info in runner_dict.items():
                row = {
                    "timestamp": timestamp_str,
                    "race_code": race_code,
                    "runner_sc": runner_info.get("sc"),
                    "runner_number": runner_info.get("number"),
                    "finish_time": runner_info.get("finish_time"),
                    "horse_name": runner_info.get("horse"),
                    "position": runner_info.get("position"),
                    "TTR": runner_info.get("TTR"),
                    "ROS": runner_info.get("ROS"),
                    "DR": runner_info.get("DR"),
                    "VP": runner_info.get("VP"),
                    "BP": runner_info.get("BP"),
                    "DB": runner_info.get("DB"),
                    "ASL": runner_info.get("ASL"),
                    "SLmin": runner_info.get("SLmin"),
                    "SLmax": runner_info.get("SLmax"),
                    "ASF": runner_info.get("ASF"),
                    "SFmin": runner_info.get("SFmin"),
                    "SFmax": runner_info.get("SFmax"),
                    "GT": runner_info.get("GT"),
                }
                csv_rows.append(row)

            write_rows_to_csv(CSV_FILENAME, CSV_COLUMNS, csv_rows)

            # Update last data received timestamp
            last_data_time = current_time

            # If we received data, switch to 0.5-second polling
            polling_interval = 0.5

        else:
            # Check if we've exceeded the timeout with no data at all
            if last_data_time is None and current_time - start_time > MAX_NO_DATA_WAIT:
                print(f"No data received for race {race_code} after {MAX_NO_DATA_WAIT} seconds. Stopping polling.")
                break

            # Check if we were receiving data, but it has stopped
            if last_data_time and current_time - last_data_time > DATA_TIMEOUT:
                print(f"Data stopped appearing for race {race_code}. Stopping polling.")
                break

        time.sleep(polling_interval)


def get_race_datetime(race_code):
    """
    Converts the last 4 digits of the race code to a scheduled datetime in EST.
    Returns the corresponding datetime in CST for execution.
    """
    race_time_str = race_code[-4:]  # Extract last 4 digits
    race_hour = int(race_time_str[:2])
    race_minute = int(race_time_str[2:])

    # Assume the races are on today's date
    now = datetime.now(EST)
    race_datetime_est = now.replace(hour=race_hour, minute=race_minute, second=0, microsecond=0)

    # Convert to CST (local execution timezone)
    race_datetime_cst = race_datetime_est.astimezone(CST)
    return race_datetime_cst


def track_races():
    """
    Loops through all race codes in chronological order and starts polling them at the right time.
    """
    for race_code in RACE_CODES:
        race_time_cst = get_race_datetime(race_code)
        start_polling_time = race_time_cst - timedelta(minutes=1)

        print(f"Scheduled polling for race {race_code} at {start_polling_time.strftime('%Y-%m-%d %H:%M:%S')} CST")

        # Wait until the polling time
        while datetime.now(CST) < start_polling_time:
            time.sleep(1)

        # Start polling the race
        poll_race(race_code)


if __name__ == "__main__":
    print("Starting race tracking based on EST race times (adjusted for CST)...")
    track_races()
    print("All races tracked. Data saved to", CSV_FILENAME)