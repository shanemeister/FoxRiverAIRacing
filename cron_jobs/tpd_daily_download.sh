#!/bin/zsh

# Source your .zshrc file
source ~/.zshrc

# Log environment variables for debugging
env > /home/exx/myCode/horse-racing/FoxRiverAIRacing/cron_logs/cron_env.log

# Define paths
WORKING_DIR="/home/exx/myCode/horse-racing/gmaxfeed"  # The directory where the script needs to run
PYTHON_SCRIPT="download_data.py"  # Just the script name, since we will cd into the directory
DATA_DIR="/home/exx/myCode/horse-racing/gmaxfeed/data"
TARGET_DIR="/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/TPD"
LOG_FILE="/home/exx/myCode/horse-racing/FoxRiverAIRacing/cron_logs/tpd_download_and_move.log"

# Define the Python executable from the tf_gpu environment
PYTHON_EXEC="/home/exx/anaconda3/envs/mamba_env/envs/tf_gpu/bin/python"

# Ensure the working directory exists
if [ ! -d "$WORKING_DIR" ]; then
    echo "$(date) - Working directory $WORKING_DIR does not exist. Exiting." | tee -a $LOG_FILE
    exit 1
fi

# Change directory to the required working directory
cd $WORKING_DIR

# Run the Python script using the correct Python interpreter
$PYTHON_EXEC $PYTHON_SCRIPT 2>&1 | tee -a $LOG_FILE

# Check if the Python script ran successfully
if [ $? -eq 0 ]; then
    echo "$(date) - Python script ran successfully." | tee -a $LOG_FILE

    # Ensure the data directory exists before proceeding
    if [ ! -d "$DATA_DIR" ]; then
        echo "$(date) - Data directory $DATA_DIR does not exist. Exiting." | tee -a $LOG_FILE
        exit 1
    fi

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