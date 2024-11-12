#!/bin/zsh

# Source your .zshrc file to ensure environment variables are loaded
source ~/.zshrc

# Define the log file for this cron job
LOG_FILE="/home/exx/myCode/horse-racing/FoxRiverAIRacing/cron_logs/ingestion_controller.log"

# Define the Python script location
PYTHON_SCRIPT="/home/exx/myCode/horse-racing/FoxRiverAIRacing/src/data_ingestion/ingestion_controller.py"

# Define the Python interpreter (adjust path if needed)
PYTHON_EXEC="/home/exx/anaconda3/envs/mamba_env/envs/tf_gpu/bin/python"

# Define the data directory to backup
DATA_DIR="/home/exx/myCode/horse-racing/FoxRiverAIRacing/data"

# Define the S3 bucket for the data backup
S3_BUCKET="s3://rshane/FoxRiverAIRacing/data"

# Start time and log the start of the job
START_TIME=$(date +'%Y-%m-%d %H:%M:%S')
echo "$START_TIME - Starting ingestion job" >> $LOG_FILE

# Run the ingestion Python script and capture the output in a variable
PYTHON_OUTPUT=$($PYTHON_EXEC $PYTHON_SCRIPT 2>&1)

# Check if the script succeeded
if [ $? -eq 0 ]; then
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Ingestion job succeeded" >> $LOG_FILE
else
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Ingestion job failed" >> $LOG_FILE
  echo "$PYTHON_OUTPUT" >> $LOG_FILE  # Log the error output for troubleshooting
  exit 1
fi

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