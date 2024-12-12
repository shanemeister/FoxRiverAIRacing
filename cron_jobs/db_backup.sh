#!/bin/bash

# Define the log file for this cron job
LOG_FILE="/home/exx/myCode/horse-racing/FoxRiverAIRacing/cron_logs/db_backup.log"

# Start time and log the start of the job
START_TIME=$(date +'%Y-%m-%d %H:%M:%S')
echo "$START_TIME - Starting database backup job" >> "$LOG_FILE"

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

# Include directories for docker and aws if necessary
export PATH="$PATH:/usr/bin:/usr/local/bin"

# Log environment variables for debugging
env > /home/exx/myCode/horse-racing/FoxRiverAIRacing/cron_logs/cron_env.log

# Set variables
DB_NAME="foxriverai"
DB_USER="rshane"
DB_PORT="5433"
DB_HOST="192.168.4.25"
BACKUP_DIR="/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/backups"
BACKUP_FILE="$BACKUP_DIR/$(date +'%Y-%m-%d')_$DB_NAME.sql"
S3_BUCKET="s3://rshane/FoxRiverAIRacing/db_backups"

# Create backup directory if it doesn't exist
mkdir -p "$BACKUP_DIR"

# Create a backup
echo "$(date +'%Y-%m-%d %H:%M:%S') - Starting database backup" >> "$LOG_FILE"

# Run pg_dump inside the Docker container
/usr/bin/docker exec -i c188a793099b pg_dump -U $DB_USER -d $DB_NAME --clean --if-exists > "$BACKUP_FILE" 2>> "$LOG_FILE"

# Check if backup succeeded
if [ $? -eq 0 ]; then
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Backup succeeded: $BACKUP_FILE" >> $LOG_FILE
else
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Backup failed" >> $LOG_FILE
  exit 1
fi

# Sync backups to S3
echo "$(date +'%Y-%m-%d %H:%M:%S') - Syncing to S3: $S3_BUCKET" >> $LOG_FILE
/usr/local/bin/aws s3 sync $BACKUP_DIR $S3_BUCKET --only-show-errors >> $LOG_FILE 2>&1

if [ $? -eq 0 ]; then
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Sync to S3 succeeded" >> $LOG_FILE
else
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Sync to S3 failed" >> $LOG_FILE
fi

# End time and log the completion
END_TIME=$(date +'%Y-%m-%d %H:%M:%S')
echo "$END_TIME - Job completed" >> "$LOG_FILE"