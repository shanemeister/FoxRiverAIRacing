#!/bin/bash

# Define paths
WORKING_DIR="/home/exx/myCode/horse-racing/gmaxfeed"  # The directory where the script needs to run
PYTHON_SCRIPT="download_data.py"  # Just the script name, since we will cd into the directory
DATA_DIR="/home/exx/myCode/horse-racing/gmaxfeed/data"
TARGET_DIR="/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/TPD"
LOG_FILE="/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/tpd_download_and_move.log"

# Change directory to the required working directory
cd $WORKING_DIR

# Run the Python script using the correct Python interpreter from the tf_gpu environment
python $PYTHON_SCRIPT 2>&1 | tee -a $LOG_FILE

# Check if the Python script ran successfully
if [ $? -eq 0 ]; then
    echo "$(date) - Python script ran successfully." | tee -a $LOG_FILE

    # Move the downloaded data to the target directory using rsync
    rsync -av --ignore-existing $DATA_DIR/ $TARGET_DIR/ 2>&1 | tee -a $LOG_FILE

    # Check if the rsync was successful
    if [ $? -eq 0 ]; then
        echo "$(date) - Data moved successfully to $TARGET_DIR." | tee -a $LOG_FILE
    else
        echo "$(date) - Failed to move data to $TARGET_DIR." | tee -a $LOG_FILE
    fi
else
    echo "$(date) - Python script failed to run." | tee -a $LOG_FILE
fi