#!/bin/zsh

# Exit immediately if a command exits with a non-zero status
set -e

# Define paths
WORKING_DIR="/home/exx/myCode/horse-racing/gmaxfeed"  # The directory where the script needs to run
PYTHON_SCRIPT="download_data.py"  # Just the script name, since we will cd into the directory
DATA_DIR="/home/exx/myCode/horse-racing/gmaxfeed/data"
TARGET_DIR="/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/TPD"
LOG_FILE="/home/exx/myCode/horse-racing/FoxRiverAIRacing/cron_logs/tpd_download_and_move.log"
SUMMARY_LOG="/home/exx/myCode/horse-racing/FoxRiverAIRacing/cron_logs/tpd_summary.log"

# Define the Python executable from the tf_gpu environment
PYTHON_EXEC="/home/exx/anaconda3/envs/mamba_env/envs/tf_gpu/bin/python"

# Initialize counters
downloaded_files=0
moved_files=0

# Ensure the working directory exists
if [ ! -d "$WORKING_DIR" ]; then
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Working directory $WORKING_DIR does not exist. Exiting." | tee -a "$LOG_FILE"
    echo "$(date +'%Y-%m-%d %H:%M:%S') - FAILED: Working directory missing" >> "$SUMMARY_LOG"
    exit 1
fi

# Change directory to the required working directory
cd "$WORKING_DIR"

# Run the Python script using the correct Python interpreter
if "$PYTHON_EXEC" "$PYTHON_SCRIPT" 2>&1 | tee -a "$LOG_FILE"; then
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Python script ran successfully." | tee -a "$LOG_FILE"

    # Count number of files downloaded (assuming all files are placed in DATA_DIR)
    downloaded_files=$(find "$DATA_DIR" -type f | wc -l)

    # Ensure the data directory exists before proceeding
    if [ ! -d "$DATA_DIR" ]; then
        echo "$(date +'%Y-%m-%d %H:%M:%S') - Data directory $DATA_DIR does not exist. Exiting." | tee -a "$LOG_FILE"
        echo "$(date +'%Y-%m-%d %H:%M:%S') - FAILED: Data directory missing" >> "$SUMMARY_LOG"
        exit 1
    fi

    # Move the downloaded data to the target directory using rsync
    rsync_output=$(rsync -av --ignore-existing "$DATA_DIR/" "$TARGET_DIR/" 2>&1 | tee -a "$LOG_FILE")

    # Extract and count files moved
    moved_files=$(echo "$rsync_output" | grep -c '^>f')

    echo "$(date +'%Y-%m-%d %H:%M:%S') - Data moved successfully to $TARGET_DIR. Downloaded $downloaded_files files, Moved $moved_files files." | tee -a "$LOG_FILE"
    echo "$(date +'%Y-%m-%d %H:%M:%S') - SUCCESS: Downloaded $downloaded_files files, Moved $moved_files files" >> "$SUMMARY_LOG"
else
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Python script failed to run." | tee -a "$LOG_FILE"
    echo "$(date +'%Y-%m-%d %H:%M:%S') - FAILED: Python script error" >> "$SUMMARY_LOG"
    exit 1
fi