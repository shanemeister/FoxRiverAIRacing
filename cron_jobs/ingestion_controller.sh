#!/bin/zsh

# Exit immediately if a command exits with a non-zero status
set -e

# Enable pipeline failure detection
setopt PIPE_FAIL

# Define the log files for this cron job
LOG_FILE="/home/exx/myCode/horse-racing/FoxRiverAIRacing/cron_logs/ingestion_controller.log"
SUMMARY_LOG="/home/exx/myCode/horse-racing/FoxRiverAIRacing/cron_logs/ingestion_controller_summary.log"

# Define email address for notifications
EMAIL="your_email@example.com"

# Function to send email on error
send_failure_email() {
    echo "Ingestion job failed on $(date +'%Y-%m-%d %H:%M:%S'). Check the log file at $LOG_FILE for details." | mail -s "Ingestion Job Failure" "$EMAIL"
}

# Trap ERR signals to send email notifications
trap 'send_failure_email' ERR

# Redirect all output and errors to the log file
exec >> "$LOG_FILE" 2>&1

# Log the start of the job
echo "$(date +'%Y-%m-%d %H:%M:%S') - Starting ingestion job"

# Define the Python script location
PYTHON_SCRIPT="/home/exx/myCode/horse-racing/FoxRiverAIRacing/src/data_ingestion/ingestion_controller.py"

# Define the Python interpreter (absolute path)
PYTHON_EXEC="/home/exx/anaconda3/envs/mamba_env/envs/tf_gpu/bin/python"

# Define the data directory to backup
DATA_DIR="/home/exx/myCode/horse-racing/FoxRiverAIRacing/data"

# Define the S3 bucket for the data backup
S3_BUCKET="s3://rshane/FoxRiverAIRacing/data"

# Define a timeout duration (e.g., 30 minutes)
TIMEOUT_DURATION=1800  # in seconds

# Define the AWS CLI executable path
AWS_EXEC="/home/exx/anaconda3/envs/mamba_env/envs/tf_gpu/bin/aws"

# Export PATH to include the Python interpreter and aws CLI
export PATH="/home/exx/anaconda3/envs/mamba_env/envs/tf_gpu/bin:/usr/local/bin:$PATH"

# Export non-sensitive environment variables
export PROJ_NETWORK=ON
export SE_MANAGER_PATH="/home/exx/anaconda3/envs/mamba_env/envs/tf_gpu/bin/selenium-manager"
export TF_ENABLE_ONEDNN_OPTS=0
export LD_LIBRARY_PATH="/home/exx/anaconda3/envs/llm/lib:"
export NVM_DIR="/home/exx/.nvm"
export NVM_CD_FLAGS="-q"
export NVM_RC_VERSION=""
export GMAXLICENCE="RS7nx33xbckp0095gsh3JKL7833p"
export FIXTURES_PATH="/home/exx/myCode/horse-racing/gmaxfeed/data/fixtures"
export RACELIST_PATH="/home/exx/myCode/horse-racing/gmaxfeed/data/racelist"
export SEC_PATH="/home/exx/myCode/horse-racing/gmaxfeed/data/sectionals"
export GPS_PATH="/home/exx/myCode/horse-racing/gmaxfeed/data/gpsData"
export SEC_HIST_PATH="/home/exx/myCode/horse-racing/gmaxfeed/data/sectionals-historical"
export ROUTE_PATH="/home/exx/myCode/horse-racing/gmaxfeed/data/routes"
export JUMPS_PATH="/home/exx/myCode/horse-racing/gmaxfeed/data/jumps"
export TRACKMASTER_USERNAME="rshane"
export PYTHONPATH="/home/exx/myCode/horse-racing/FoxRiverAIRacing:$PYTHONPATH"

# Source the sensitive environment variables
# Ensure this file is secured and not accessible to unauthorized users
source /home/exx/myCode/horse-racing/FoxRiverAIRacing/config/.env

# Activate conda environment
source /home/exx/anaconda3/etc/profile.d/conda.sh
conda activate mamba_env  # Ensure 'mamba_env' is the correct environment name

# Change to the root directory to ensure relative paths are resolved correctly
cd /home/exx/myCode/horse-racing/FoxRiverAIRacing

# Verify Python interpreter exists
if [ ! -f "$PYTHON_EXEC" ]; then
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Python interpreter not found at $PYTHON_EXEC"
    echo "$(date +'%Y-%m-%d %H:%M:%S') - FAILED: Python interpreter missing" >> "$SUMMARY_LOG"
    exit 1
fi

# Verify Python script exists
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Python script not found at $PYTHON_SCRIPT"
    echo "$(date +'%Y-%m-%d %H:%M:%S') - FAILED: Python script missing" >> "$SUMMARY_LOG"
    exit 1
fi

# Optional: Test AWS CLI access (Remove after verification)
# echo "$(date +'%Y-%m-%d %H:%M:%S') - Testing AWS CLI access" >> "$LOG_FILE"
# $AWS_EXEC s3 ls >> "$LOG_FILE" 2>&1
# echo "$(date +'%Y-%m-%d %H:%M:%S') - AWS CLI access test completed" >> "$LOG_FILE"

# Run the ingestion Python script
echo "$(date +'%Y-%m-%d %H:%M:%S') - Running ingestion script"
if "$PYTHON_EXEC" "$PYTHON_SCRIPT" 2>&1 | tee -a "$LOG_FILE"; then
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Python script ran successfully."
    
    # Log processed files counts
    echo "ZIP files failed or skipped: 2"
    echo "Processed files for ResultsCharts: $(find "$DATA_DIR/Equibase/ResultsCharts" -type f | wc -l)"
    echo "Processed files for Sectionals: $(find "$DATA_DIR/TPD/sectionals" -type f | wc -l)"
    echo "Processed files for GPSData: $(find "$DATA_DIR/TPD/gpsData" -type f | wc -l)"
    echo "Processed files for Racelist: $(find "$DATA_DIR/TPD/racelist" -type f | wc -l)"
    
    # Ensure the data directory exists before proceeding
    if [ ! -d "$DATA_DIR" ]; then
        echo "$(date +'%Y-%m-%d %H:%M:%S') - Data directory $DATA_DIR does not exist. Exiting."
        echo "$(date +'%Y-%m-%d %H:%M:%S') - FAILED: Data directory missing" >> "$SUMMARY_LOG"
        exit 1
    fi

    # Sync the downloaded data to S3 with a timeout
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Syncing data directory to S3: $S3_BUCKET"
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Starting aws s3 sync command" >> "$LOG_FILE"

    rsync_output=$(timeout $TIMEOUT_DURATION $AWS_EXEC s3 sync "$DATA_DIR" "$S3_BUCKET" --only-show-errors --no-progress 2>&1 | tee -a "$LOG_FILE")
    SYNC_EXIT_CODE=$?

    echo "$(date +'%Y-%m-%d %H:%M:%S') - aws s3 sync command completed with exit code $SYNC_EXIT_CODE" >> "$LOG_FILE"
    echo "$rsync_output" >> "$LOG_FILE"

    # Check if the sync succeeded
    if [ $SYNC_EXIT_CODE -eq 0 ]; then
        # Extract and count files moved
        moved_files=$(echo "$rsync_output" | grep -c '^upload:')
        total_downloaded_files=$(find "$DATA_DIR" -type f | wc -l)
        echo "$(date +'%Y-%m-%d %H:%M:%S') - Sync to S3 succeeded. Downloaded $total_downloaded_files files, Moved $moved_files files." >> "$LOG_FILE"
        echo "$(date +'%Y-%m-%d %H:%M:%S') - SUCCESS: Downloaded $total_downloaded_files files, Moved $moved_files files" >> "$SUMMARY_LOG"
    elif [ $SYNC_EXIT_CODE -eq 124 ]; then
        # Timeout
        echo "$(date +'%Y-%m-%d %H:%M:%S') - Sync to S3 timed out." >> "$LOG_FILE"
        echo "$(date +'%Y-%m-%d %H:%M:%S') - FAILED: Rsync to S3 timed out." >> "$SUMMARY_LOG"
        exit 1
    else
        # Other failures
        echo "$(date +'%Y-%m-%d %H:%M:%S') - Sync to S3 failed with exit code $SYNC_EXIT_CODE" >> "$LOG_FILE"
        echo "$(date +'%Y-%m-%d %H:%M:%S') - FAILED: Rsync to S3 encountered errors." >> "$SUMMARY_LOG"
        exit 1
    fi
else
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Python script failed to run."
    echo "$(date +'%Y-%m-%d %H:%M:%S') - FAILED: Python script error" >> "$SUMMARY_LOG"
    exit 1
fi

# Log the completion of the job
echo "$(date +'%Y-%m-%d %H:%M:%S') - Job completed successfully"
echo "$(date +'%Y-%m-%d %H:%M:%S') - SUCCESS: Job completed successfully" >> "$SUMMARY_LOG"