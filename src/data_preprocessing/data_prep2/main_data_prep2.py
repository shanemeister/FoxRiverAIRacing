import os
import logging
import argparse
import pprint
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql, reload_parquet_files
from src.data_preprocessing.data_prep1.sql_queries import sql_queries
from src.data_preprocessing.data_prep1.merge_gps_sectional import merge_gps_sectionals
from src.data_preprocessing.data_prep1.data_utils import (save_parquet, gather_statistics, initialize_environment,
                                               load_config, initialize_spark,
                                               identify_and_impute_outliers, identify_and_remove_outliers, detect_cardinality_columns,
                                               identify_missing_and_outliers)
from src.data_preprocessing.data_prep2.data_healthcheck import time_series_data_healthcheck, dataframe_summary

def manage_outliers_and_missing_data(spark, parquet_dir, cols, handle_outliers='impute'):
    """
    Load matched_df and process data to identify and handle outliers and missing data.
    
    Parameters:
    cols (list): List of columns to check for outliers and missing data
    handle_outliers (str): Method to handle outliers ('remove' or 'impute')
    """
    master_df = spark.read.parquet(os.path.join(parquet_dir, "master_df.parquet"))
    # Identify and handle outliers and missing data for specific columns
    for column in cols:
        if handle_outliers == 'remove':
            master_df = identify_and_remove_outliers(master_df, column)
        elif handle_outliers == 'impute':
            print(f"Imputing outliers for column: {column}############################################")
            master_df = identify_and_impute_outliers(master_df, column)
    
    # After imputing
    save_parquet(spark, master_df, "master_df", parquet_dir)
    # Re-load master_df after overwrite to get a clean reference
    spark.catalog.clearCache()
    # master_df = spark.read.parquet(os.path.join(parquet_dir, "master_stride_df.parquet"))
    #identify_missing_and_outliers(spark, parquet_dir, master_df, cols)
    #gather_statistics(master_df, "master_df")
    
def main():
    spark = None
    # Set global references to None
    master_results_df = None
    results_df = None
    spark, jdbc_url, jdbc_properties, parquet_dir, log_file = initialize_environment()
    spark.catalog.clearCache()
   # Argument parser setup
    parser = argparse.ArgumentParser(description="Run EQB and TPD ingestion pipeline.")
    parser.add_argument('--view_frame', nargs='+', help="Use to see contents of a DataFrame.")
    parser.add_argument('--data_check', nargs='+', help="Columns to identify missing and outlier data.")
    parser.add_argument('--cardinality', nargs='+', help="Provide dataframe and threshold for high/low cardinality -- 50 is default.") 
    args = parser.parse_args()

    if args.view_frame:
        # Debugging: Print the parsed arguments
        print(f"args.view_frame: {args.view_frame}")
        # Extract DataFrame name and optional columns
        if len(args.view_frame) < 1:
            raise ValueError("You must specify a DataFrame name.")
        df_name = args.view_frame[0]  # First element is the DataFrame name
        cols = args.view_frame[1:] if len(args.view_frame) > 1 else None  # Remaining elements are columns, if provided
        # Construct the path and load the DataFrame
        df_path = os.path.join(parquet_dir, f"{df_name}.parquet")
        print(f"Loading DataFrame {df_name} for viewing from {df_path}...")
        try:
            df = spark.read.parquet(df_path)
        except Exception as e:
            raise RuntimeError(f"Error reading DataFrame from {df_path}: {e}")
        # Call the dataframe_summary function
        dataframe_summary(spark, df, cols=cols)    
    elif args.data_check:
        df_name = args.data_check[0]
        df_path = os.path.join(parquet_dir, f"{df_name}.parquet")
        print(f"Loading DataFrame {df_path} for data check...")
        df = spark.read.parquet(df_path)
        healthcheck_report = time_series_data_healthcheck(df, args.data_check)
        pprint.pprint(healthcheck_report)
    elif args.cardinality:
        df_name = args.cardinality[0]
        card = int(args.cardinality[1])  # Convert threshold to an integer
        type = args.cardinality[2]
        df_path = os.path.join(parquet_dir, f"{df_name}.parquet")
        df = spark.read.parquet(df_path)
        detect_cardinality_columns(df, threshold=card, cardinality_type=type)    
    else:
        race_df_p1 = spark.read.parquet(os.path.join(parquet_dir, "race_df_p1.parquet"))
        healthcheck_report = time_series_data_healthcheck(race_df_p1)
        pprint.pprint(healthcheck_report)
        #results_df = spark.read.parquet(os.path.join(parquet_dir, "results.parquet"))
        #results_df.count()
        #results_df.show(20, truncate=False)
        #results_df.printSchema()
        
    logging.info("Ingestion job succeeded")
    return spark

if __name__ == "__main__":
    spark = main()
    # Stop the Spark session
    spark.stop()
