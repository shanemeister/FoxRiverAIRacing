#!/bin/zsh

# Source your .zshrc file to ensure environment variables are loaded
source ~/.zshrc

# Define the log file for this cron job
LOG_FILE="/home/exx/myCode/horse-racing/FoxRiverAIRacing/cron_logs/ingestion_controller.log"

# Define the Python script location
PYTHON_SCRIPT="/home/exx/myCode/horse-racing/FoxRiverAIRacing/src/data_ingestion/ingestion_controller.py"

# Define the Python interpreter
PYTHON_EXEC="/home/exx/anaconda3/envs/mamba_env/envs/tf_gpu/bin/python"

# Define the data directory to backup
DATA_DIR="/home/exx/myCode/horse-racing/FoxRiverAIRacing/data"

# Define the S3 bucket for the data backup
S3_BUCKET="s3://rshane/FoxRiverAIRacing/data"

# Start time and log the start of the job
START_TIME=$(date +'%Y-%m-%d %H:%M:%S')
echo "$START_TIME - Starting ingestion job" >> $LOG_FILE

# Run the ingestion Python script and log output directly
echo "$(date +'%Y-%m-%d %H:%M:%S') - Running Python script: $PYTHON_SCRIPT" >> $LOG_FILE
$PYTHON_EXEC $PYTHON_SCRIPT >> $LOG_FILE 2>&1
PYTHON_EXIT_CODE=$?

# Check if the Python script succeeded
if [ $PYTHON_EXIT_CODE -eq 0 ]; then
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Python script succeeded" >> $LOG_FILE
else
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Python script failed with exit code $PYTHON_EXIT_CODE" >> $LOG_FILE
  exit 1
fi

# Backup the data directory to S3 and log output directly
echo "$(date +'%Y-%m-%d %H:%M:%S') - Syncing data directory to S3: $S3_BUCKET" >> $LOG_FILE
aws s3 sync $DATA_DIR $S3_BUCKET --only-show-errors >> $LOG_FILE 2>&1
AWS_EXIT_CODE=$?

# Check if the sync succeeded
if [ $AWS_EXIT_CODE -eq 0 ]; then
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Sync to S3 succeeded" >> $LOG_FILE
else
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Sync to S3 failed with exit code $AWS_EXIT_CODE" >> $LOG_FILE
  exit 1
fi

# End time and log the completion
END_TIME=$(date +'%Y-%m-%d %H:%M:%S')
echo "$END_TIME - Job completed" >> $LOG_FILE