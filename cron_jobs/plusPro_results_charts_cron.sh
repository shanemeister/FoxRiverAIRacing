#!/bin/zsh

# Source your .zshrc file to ensure environment variables are loaded
source ~/.zshrc

# Define the log file for this cron job
LOG_FILE="/home/exx/myCode/horse-racing/FoxRiverAIRacing/cron_logs/pluspro_results_charts.log"

# Define the Python script location
PYTHON_SCRIPT="/home/exx/myCode/horse-racing/FoxRiverAIRacing/src/data_download/Trackmaster/plusPro_results_charts.py"

# Define the Python interpreter (adjust path if needed)
PYTHON_EXEC="/home/exx/anaconda3/envs/mamba_env/envs/tf_gpu/bin/python"

# Define the data directory to backup
DATA_DIR="/home/exx/myCode/horse-racing/FoxRiverAIRacing/data"

# Define the S3 bucket for the data backup
S3_BUCKET="s3://rshane/FoxRiverAIRacing/data"

# Track the number of downloaded files
RESULTS_FILES_COUNT=0
PLUSPRO_FILES_COUNT=0

# Start time and log the start of the job
START_TIME=$(date +'%Y-%m-%d %H:%M:%S')
echo "$START_TIME - Starting download job" >> $LOG_FILE

# Run the Python script and capture the output in variables instead of logging each file
PYTHON_OUTPUT=$($PYTHON_EXEC $PYTHON_SCRIPT 2>&1)

# Check if the script succeeded
if [ $? -eq 0 ]; then
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Download job succeeded" >> $LOG_FILE
  # Count the number of downloaded files from PlusPro and ResultsCharts
  RESULTS_FILES_COUNT=$(echo "$PYTHON_OUTPUT" | grep -c 'Downloaded: .*ResultsCharts')
  PLUSPRO_FILES_COUNT=$(echo "$PYTHON_OUTPUT" | grep -c 'Downloaded: .*PlusPro')
else
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Download job failed" >> $LOG_FILE
  echo "$PYTHON_OUTPUT" >> $LOG_FILE  # Log the error output for troubleshooting
  exit 1
fi

# Log the file counts
echo "$(date +'%Y-%m-%d %H:%M:%S') - Results files downloaded: $RESULTS_FILES_COUNT" >> $LOG_FILE
echo "$(date +'%Y-%m-%d %H:%M:%S') - PlusPro files downloaded: $PLUSPRO_FILES_COUNT" >> $LOG_FILE

# Backup the data directory to S3
echo "$(date +'%Y-%m-%d %H:%M:%S') - Syncing data directory to S3: $S3_BUCKET" >> $LOG_FILE
aws s3 sync $DATA_DIR $S3_BUCKET >> $LOG_FILE 2>&1

# Check if the sync succeeded
if [ $? -eq 0 ]; then
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Sync to S3 succeeded" >> $LOG_FILE
else
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Sync to S3 failed" >> $LOG_FILE
fi

# End time and log the completion
END_TIME=$(date +'%Y-%m-%d %H:%M:%S')
echo "$END_TIME - Job completed" >> $LOG_FILE

# Summarize the log in a single line for easy tracking (Optional)
SUMMARY="$START_TIME - Results: $RESULTS_FILES_COUNT files, PlusPro: $PLUSPRO_FILES_COUNT files, Backup: $(if [ $? -eq 0 ]; then echo "Success"; else echo "Failed"; fi)"
echo $SUMMARY >> $LOG_FILE