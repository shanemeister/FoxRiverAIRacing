#!/bin/zsh

# Source your .zshrc file
source ~/.zshrc

# Log environment variables for debugging
env > /home/exx/myCode/horse-racing/FoxRiverAIRacing/cron_logs/cron_env.log

# Set variables
DB_NAME="foxriverai"
DB_USER="rshane"
DB_PORT="5433"
DB_HOST="192.168.4.25"
BACKUP_DIR="/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/backups"
BACKUP_FILE="$BACKUP_DIR/$(date +\%Y-\%m-\%d)_$DB_NAME.sql"
S3_BUCKET="s3://rshane/FoxRiverAIRacing/db_backups"
LOG_FILE="/home/exx/myCode/horse-racing/FoxRiverAIRacing/cron_logs/db_backup.log"

# Create a backup
echo "$(date +'%Y-%m-%d %H:%M:%S') - Starting database backup" >> $LOG_FILE
pg_dump -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME --clean --if-exists > $BACKUP_FILE

# Check if backup succeeded
if [ $? -eq 0 ]; then
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Backup succeeded: $BACKUP_FILE" >> $LOG_FILE
else
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Backup failed" >> $LOG_FILE
  exit 1
fi

# Sync backups to S3
echo "$(date +'%Y-%m-%d %H:%M:%S') - Syncing to S3: $S3_BUCKET" >> $LOG_FILE
aws s3 sync $BACKUP_DIR $S3_BUCKET >> $LOG_FILE 2>&1

if [ $? -eq 0 ]; then
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Sync to S3 succeeded" >> $LOG_FILE
else
  echo "$(date +'%Y-%m-%d %H:%M:%S') - Sync to S3 failed" >> $LOG_FILE
fi