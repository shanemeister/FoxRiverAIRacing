# model_data_gps_prep.py

import os
import logging
from src.models.config_utils import load_config, initialize_logging, initialize_spark
from src.models.data_loader import load_data_from_postgresql, reload_parquet_files
from src.models.sql_queries import sql_queries
from src.models.merge_gps_sectional import merge_gps_sectionals
from src.models.data_utils import save_parquet

def initialize_environment():
    # Paths and configurations
    config_path = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/config.ini'
    log_file = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs/SparkPy_load.log"
    jdbc_driver_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/jdbc/postgresql-42.7.4.jar"
    parquet_dir = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/"
    os.makedirs(parquet_dir, exist_ok=True)
    
    # Clear the log file by opening it in write mode
    with open(log_file, 'w'):
        pass  # This will truncate the file without writing anything
    
    # Load configuration
    config = load_config(config_path)

    # Database credentials from config
    db_host = config['database']['host']
    db_port = config['database']['port']
    db_name = config['database']['dbname']
    db_user = config['database']['user']
    # db_password = os.getenv("DB_PASSWORD", "SparkPy24!")  # Ensure DB_PASSWORD is set

    # Validate database password
    #if not db_password:
    #    raise ValueError("Database password is missing. Set it in the DB_PASSWORD environment variable.")

    # JDBC URL and properties
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    jdbc_properties = {
        "user": db_user,
        #"password": db_password,
        "driver": "org.postgresql.Driver"
    }
    # Initialize logging
    initialize_logging(log_file)
    queries = sql_queries()
    # Initialize Spark session
    spark = initialize_spark(jdbc_driver_path)
    return spark, jdbc_url, jdbc_properties, queries, parquet_dir, log_file

def main():
    
    spark, jdbc_url, jdbc_properties, queries, parquet_dir, log_file = initialize_environment()
    # Load data from PostgreSQL and save as Parquet
    load_data_from_postgresql(spark, jdbc_url, jdbc_properties, queries, parquet_dir)

    # Reload Parquet files into Spark DataFrames for processing
    results_df, sectionals_df, gps_df = reload_parquet_files(spark, parquet_dir)

    # Display schema of each DataFrame
    print("Results DataFrame Schema:")
    results_df.printSchema()
    print("Sectionals DataFrame Schema:")
    sectionals_df.printSchema()
    print("GPS DataFrame Schema:")
    gps_df.printSchema()
    
    matched_df = merge_gps_sectionals(spark, results_df, sectionals_df, gps_df)
    # Save DataFrames as Parquet files
    save_parquet(matched_df, "matched_df", parquet_dir)
    return spark

if __name__ == "__main__":
    spark = main()
    # Stop the Spark session
    spark.stop()