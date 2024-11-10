import os
import re

def append_race_index(base_path):
    # List all files in the directory
    files = [f for f in os.listdir(base_path) if os.path.isfile(os.path.join(base_path, f))]
    
    # Filter out files that are exactly 14 characters long and alphanumeric
    valid_files = [f for f in files if len(f) == 14 and re.match(r"^[a-zA-Z0-9]+$", f)]
    
    # Group files by the first 10 characters
    grouped_files = {}
    for file in valid_files:
        prefix = file[:10]
        if prefix not in grouped_files:
            grouped_files[prefix] = []
        grouped_files[prefix].append(file)
    
    # Process each group: sort, then rename with appended index
    for prefix, file_group in grouped_files.items():
        # Sort files in ascending order
        sorted_files = sorted(file_group)
        
        # Rename files with a 2-digit race index
        for i, filename in enumerate(sorted_files, start=1):
            new_name = f"{filename[:14]}{i:02}"
            old_path = os.path.join(base_path, filename)
            new_path = os.path.join(base_path, new_name)
            os.rename(old_path, new_path)
            print(f"Renamed {filename} to {new_name}")

# Example usage:
append_race_index("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/TPD/sectionals")