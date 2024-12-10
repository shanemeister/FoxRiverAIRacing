# model_data_gps_prep.py

import os
import logging
import argparse
from pyspark.sql.functions import col
from src.data_preprocessing.data_loader import load_data_from_postgresql, reload_parquet_files
from src.data_preprocessing.sql_queries import sql_queries
from src.data_preprocessing.merge_gps_sectional import merge_gps_sectionals
from src.data_preprocessing.data_utils import (save_parquet, gather_statistics, initialize_environment,
                                               load_config, initialize_logging, initialize_spark, drop_duplicates_with_tolerance,
                                               identify_and_impute_outliers, identify_and_remove_outliers,
                                               identify_missing_and_outliers)

def load_postgresql_data(spark, jdbc_url, jdbc_properties, queries, parquet_dir):
    """
    Load data from PostgreSQL and save as Parquet.
    """
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
    
    matched_df = merge_gps_sectionals(spark, results_df, sectionals_df, gps_df, parquet_dir)
    # Save DataFrames as Parquet files
    save_parquet(spark, matched_df, "matched_df", parquet_dir)
    return matched_df

def rebuild_master_df(spark, parquet_dir):
    """
    Rebuild the master DataFrame from Parquet files.
    """
    results_df, sectionals_df, gps_df = reload_parquet_files(spark, parquet_dir)
    matched_df = merge_gps_sectionals(spark, results_df, sectionals_df, gps_df, parquet_dir)
    save_parquet(spark, matched_df, "matched_df", parquet_dir)
    return matched_df

def process_data(spark, parquet_dir):
    """
    Load master_df and process data to remove duplicates where sec_time_stamp maps to multiple gps time_stamp.
    """
    matched_df = spark.read.parquet(os.path.join(parquet_dir, "matched_df.parquet"))
    # gather_statistics(matched_df, "matched_df")
    # Drop duplicates where sec_time_stamp maps to multiple gps time_stamp
    master_df = drop_duplicates_with_tolerance(matched_df)
    
    gather_statistics(master_df, "master_df")
    save_parquet(spark, master_df, "master_df", parquet_dir)
    return master_df

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
    
def merge_results(spark, parquet_dir):
    """
    Merge results_df with master_df.
    """
    results_df = spark.read.parquet(os.path.join(parquet_dir, "results.parquet"))
    results_df.printSchema()
    master_df = spark.read.parquet(os.path.join(parquet_dir, "master_df.parquet"))
    # Merge results_df with master_df
    # Perform the join
    master_results_df = master_df.join(
        results_df.select(
            "horse_id", "official_fin", "course_cd", "race_date", "race_number",
            "saddle_cloth_number", "post_pos", "speed_rating",
            "turf_mud_mark", "weight", "morn_odds", "avgspd", "surface", "trk_cond",
            "class_rating", "weather", "wps_pool", "stk_clm_md", "todays_cls", "net_sentiment"
        ),
        on=["course_cd", "race_date", "race_number", "saddle_cloth_number"],
        how="left"
    )
    # Save merged_df as Parquet
    save_parquet(spark, master_results_df, "master_results_df", parquet_dir)
    master_results_df.printSchema()    
    
def main():
    spark = None
    # Set global references to None
    master_df = None
    matched_df = None

    spark, jdbc_url, jdbc_properties, queries, parquet_dir, log_file = initialize_environment()
    
    # Argument parser to allow selective processing of datasets via command line
    parser = argparse.ArgumentParser(description="Run EQB and TPD ingestion pipeline.")
    parser.add_argument('--rebuild', action='store_true', help="Rebuild master DataFrame.") # Rebuild from saved Parquet files
    parser.add_argument('--process', action='store_true', help="Process data to remove sec_time_stamp duplicates.") # Process data to remove duplicates
    parser.add_argument('--identify', nargs='+', help="Columns to identify missing and outlier data.") # Identify missing and outlier data -- does not change the data just identifies missing and outlier data
    parser.add_argument('--manage_outliers', nargs='+', help="Columns to check for outliers and missing data, followed by method (remove or impute).") # to run this command, use the following command: python main_model_data_prep.py --manage_outliers col1 col2 col3 impute -- remove has not been tested
    parser.add_argument('--results', action='store_true', help="Merge Results with Master_df.") # Merge results_df with master_df
    args = parser.parse_args() # Load from Postgres by default

    if args.rebuild:
        matched_df = rebuild_master_df(spark, parquet_dir)
    elif args.process:
        master_df = process_data(spark, parquet_dir)
    elif args.identify:
        cols = args.identify  # Directly use the list of columns
        master_df = spark.read.parquet(os.path.join(parquet_dir, "master_df.parquet"))
        identify_missing_and_outliers(spark, parquet_dir, master_df, cols)
    elif args.manage_outliers:
        cols = args.manage_outliers[:-1]
        method = args.manage_outliers[-1]
        manage_outliers_and_missing_data(spark, parquet_dir, cols, handle_outliers=method)
    elif args.results:
        merge_results(spark, parquet_dir)
    else:
        matched_df = load_postgresql_data(spark, jdbc_url, jdbc_properties, queries, parquet_dir)
    
    logging.info("Ingestion job succeeded")
    return spark

if __name__ == "__main__":
    spark = main()
    # Stop the Spark session
    spark.stop()