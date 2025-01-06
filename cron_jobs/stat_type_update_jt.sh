#!/bin/bash

# Define the log file for this cron job
LOG_FILE="/home/exx/myCode/horse-racing/FoxRiverAIRacing/cron_logs/stat_type_cron_job_jt.log"

# Start time and log the start of the job
START_TIME=$(date +'%Y-%m-%d %H:%M:%S')
echo "$START_TIME - Starting stat_type_update job" >> "$LOG_FILE"

# Load environment variables from .env file
ENV_FILE="/home/exx/myCode/horse-racing/FoxRiverAIRacing/config/.env"

if [ -f "$ENV_FILE" ]; then
    set -a  # Automatically export all variables
    source "$ENV_FILE"
    set +a
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Environment variables loaded from $ENV_FILE" >> "$LOG_FILE"
else
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Environment file $ENV_FILE not found. Exiting." >> "$LOG_FILE"
    exit 1
fi

# Ensure PATH is set
export PATH="/usr/local/bin:/usr/bin:/bin:/home/exx/.local/bin"

# Define the Python script location
PYTHON_SCRIPT="/home/exx/myCode/horse-racing/FoxRiverAIRacing/src/data_preprocessing/stat_type_update_jt.py"

# Define the Python interpreter
PYTHON_EXEC="/home/exx/anaconda3/envs/mamba_env/envs/tf_gpu/bin/python"

# Optionally activate the Conda environment
# source "/home/exx/anaconda3/etc/profile.d/conda.sh"
# conda activate tf_gpu

# Run the ingestion Python script and log output directly
echo "$(date +'%Y-%m-%d %H:%M:%S') - Running Python script: $PYTHON_SCRIPT" >> "$LOG_FILE"
"$PYTHON_EXEC" "$PYTHON_SCRIPT" >> "$LOG_FILE" 2>&1
PYTHON_EXIT_CODE=$?

# Check if the Python script succeeded
if [ $PYTHON_EXIT_CODE -eq 0 ]; then
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Python script succeeded" >> "$LOG_FILE"
else
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Python script failed with exit code $PYTHON_EXIT_CODE" >> "$LOG_FILE"
  exit 1
fi

# End time and log the completion
END_TIME=$(date +'%Y-%m-%d %H:%M:%S')
echo "$END_TIME - Job completed" >> "$LOG_FILE"