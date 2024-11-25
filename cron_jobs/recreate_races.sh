#!/bin/bash

# Define the log file for this cron job
LOG_FILE="/home/exx/myCode/horse-racing/FoxRiverAIRacing/cron_logs/recreate_races.log"

# Start time and log the start of the job
START_TIME=$(date +'%Y-%m-%d %H:%M:%S')
echo "$START_TIME - Starting races table recreation job" >> "$LOG_FILE"

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

# Define the SQL script location inside the container
SQL_SCRIPT="/sql_scripts/recreate_races.sql"  # Path inside the Docker container

# Define Docker container name
DOCKER_CONTAINER="postgis_container_new"  # Ensure this matches your container name

# Define psql connection parameters from environment variables
PSQL_USER="$USER"       # Corrected to uppercase
PSQL_DB="$DBNAME"       # Corrected to uppercase
PSQL_HOST="$HOST"       # Corrected to uppercase
PSQL_PORT="$PORT"       # Corrected to uppercase

# Check if Docker container is running
if ! docker ps --format '{{.Names}}' | grep -w "$DOCKER_CONTAINER" > /dev/null; then
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Docker container $DOCKER_CONTAINER is not running. Exiting." >> "$LOG_FILE"
    exit 1
fi

# Run the SQL script inside the Docker container and log output directly
echo "$(date +'%Y-%m-%d %H:%M:%S') - Running SQL script: $SQL_SCRIPT inside container $DOCKER_CONTAINER" >> "$LOG_FILE"

# Execute the SQL script inside the container
docker exec -i "$DOCKER_CONTAINER" psql -U "$PSQL_USER" -d "$PSQL_DB" -f "$SQL_SCRIPT" >> "$LOG_FILE" 2>&1
PSQL_EXIT_CODE=$?

# Check if the psql command succeeded
if [ $PSQL_EXIT_CODE -eq 0 ]; then
    echo "$(date +'%Y-%m-%d %H:%M:%S') - SQL script executed successfully" >> "$LOG_FILE"
else
    echo "$(date +'%Y-%m-%d %H:%M:%S') - SQL script failed with exit code $PSQL_EXIT_CODE" >> "$LOG_FILE"
    exit 1
fi

# End time and log the completion
END_TIME=$(date +'%Y-%m-%d %H:%M:%S')
echo "$END_TIME - Races table recreation job completed" >> "$LOG_FILE"