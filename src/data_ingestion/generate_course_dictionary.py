import pandas as pd
import os
from configparser import ConfigParser
from ingestion_utils import get_db_connection

# Use to regenerate the mappings_dictionaries.py file with the latest course mappings from the database.

# Load your configuration file
config_path = 'config.ini'
config = ConfigParser()
config.read(config_path)

# Establish the database connection
conn = get_db_connection(config)  # Pass the parsed ConfigParser object here

# Define the output file path
output_file = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/src/data_ingestion/mappings_dictionaries.py'  # Adjust to your preferred output path

# Query the course table to get the necessary mappings
query = """
SELECT course_cd, tpd_track_cd, eqb_track_cd, track_name
FROM public.course;
"""
df = pd.read_sql(query, conn)

# Initialize an empty dictionary for mappings
course_mappings = {}

# Populate the dictionary with both TPD and EQB mappings
for index, row in df.iterrows():
    course_cd = row['course_cd']
    tpd_code = row['tpd_track_cd']
    eqb_code = row['eqb_track_cd']
    track_name = row['track_name']
    
    # Add entries for TPD and EQB codes if they exist
    if pd.notna(tpd_code):  # Ensure TPD code is not NaN
        course_mappings[tpd_code] = (course_cd, track_name)
    if pd.notna(eqb_code):  # Ensure EQB code is not NaN
        course_mappings[eqb_code] = (course_cd, track_name)

# Close the database connection
conn.close()

# Write the dictionary to mappings.py file
with open(output_file, 'w') as f:
    f.write("eqb_tpd_codes_to_course_cd = {\n")
    for key, (value, track_name) in course_mappings.items():
        f.write(f"    \"{key}\": \"{value}\",  # {track_name}\n")
    f.write("}\n")

print(f"Mappings have been saved to {output_file}")