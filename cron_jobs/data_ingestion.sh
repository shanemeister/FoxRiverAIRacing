#!/bin/bash

source /path/to/your/venv/bin/activate  # Activate virtual environment
# Run the Python ingestion script
python /path/to/your/project/src/data_ingestion/ingestion_controller.py

# Exit if there is an error
if [ $? -ne 0 ]; then
  echo "Ingestion failed!"
  exit 1
fi

echo "Ingestion completed successfully."