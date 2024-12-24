import os
import logging
import pprint
from pyspark.sql.functions import col, upper, trim, row_number
from pyspark.sql.window import Window
from src.data_preprocessing.data_prep2.data_healthcheck import time_series_data_healthcheck, dataframe_summary
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql, reload_parquet_files, load_named_parquet_files, merge_results_sectionals
from src.data_preprocessing.data_prep1.sql_queries import sql_queries
from src.data_preprocessing.gps_aggregated.merge_gps_sectionals_agg import merge_gps_sectionals
from src.data_preprocessing.data_prep1.data_utils import (
    save_parquet, gather_statistics, initialize_environment,
    load_config, initialize_logging, initialize_spark, drop_duplicates_with_tolerance,
    identify_and_impute_outliers, identify_and_remove_outliers, process_merged_results_sectionals,
    identify_missing_and_outliers
)

def clear_screen():
    os.system('clear') 

def load_postgresql_data(spark, jdbc_url, jdbc_properties, queries, parquet_dir):
    """
    Main function to load data from PostgreSQL, write to parquet, reload, and print schemas.
    This uses the dictionary of queries to handle all data frames dynamically.
    """
    # Load and write data to parquet
    load_data_from_postgresql(spark, jdbc_url, jdbc_properties, queries, parquet_dir)

    # Reload data from parquet
    reloaded_dfs = reload_parquet_files(spark, parquet_dir, queries)

    # Print schemas dynamically
    for name, df in reloaded_dfs.items():
        print(f"DataFrame '{name}' Schema:")
        df.printSchema()

    return reloaded_dfs
    
    
def process_data(spark, jdbc_url, jdbc_properties, queries, parquet_dir):
    clear_screen()
    print("Select an action:")
    print("1) Load data with queries (from PostgreSQL and write to parquet)")
    print("2) Load data from parquet files and process data for time series analysis")

    choice = input("Enter your choice (1 or 2): ").strip()

    if choice == "1":
        reloaded_dfs = load_postgresql_data(spark, jdbc_url, jdbc_properties, queries, parquet_dir)
        print("Keys in reloaded_dfs:", reloaded_dfs.keys())
        logging.info("Ingestion job succeeded")
        return spark

    elif choice == "2":
        merge_choice = input("Merge results with sectionals (Y/N)? ").strip().lower()
        if merge_choice == "y":
            df_names = ["results", "sectionals"]
            loaded_parquet_dfs = load_named_parquet_files(spark, df_names, parquet_dir)
            print("Keys in loaded_parquet_dfs:", loaded_parquet_dfs.keys())

            if "results" not in loaded_parquet_dfs or "sectionals" not in loaded_parquet_dfs:
                print("Missing 'results' or 'sectionals' in loaded_parquet_dfs. Check your keys!")
                return spark

            results_df = loaded_parquet_dfs["results"]
            sectionals_df = loaded_parquet_dfs["sectionals"]

            merge_results_sectionals_df = merge_results_sectionals(spark, results_df, sectionals_df, parquet_dir)
            merge_results_sectionals_df.write.parquet(os.path.join(parquet_dir, "merge_results_sectionals.parquet"), mode="overwrite")
            merge_results_sectionals_df.printSchema()
            healthcheck_report = time_series_data_healthcheck(merge_results_sectionals_df)
            pprint.pprint(healthcheck_report)

        gps_merge_choice = input("Merge Results and Sectionals with GPS data (Y/N)? ").strip().lower()
        if gps_merge_choice == "y":
            if 'merge_results_sectionals_df' not in locals():
                merge_results_sectionals_df = spark.read.parquet(os.path.join(parquet_dir, "merge_results_sectionals.parquet"))
            gps_df = spark.read.parquet(os.path.join(parquet_dir, "gpspoint.parquet"))
            merged_df = merge_gps_sectionals(spark, merge_results_sectionals_df, gps_df, parquet_dir)
            merged_df.printSchema()
            healthcheck_report = time_series_data_healthcheck(merged_df)
            pprint.pprint(healthcheck_report)
            save_parquet(spark, merged_df, "merged_df", parquet_dir)
#Step 3: Process the matched DataFrame
        dataprep_choice = input("Do you want to prep merged_df DataFrame for processing (Y/N)? ").strip().lower()
        if dataprep_choice == "y":
            # Load the merged DataFrame
            merged_df = spark.read.parquet(os.path.join(parquet_dir, "merged_df.parquet"))
            processed_data = process_merged_results_sectionals(spark, merged_df, parquet_dir)
            save_parquet(spark, processed_data, "processed_data", parquet_dir)
        else:
            print("Ok, Exiting. Good Bye!")
            return spark

def main():
    spark = None
    master_df = None
    matched_df = None

    spark, jdbc_url, jdbc_properties, queries, parquet_dir, log_file = initialize_environment()
    spark = process_data(spark, jdbc_url, jdbc_properties, queries, parquet_dir)
    return spark


if __name__ == "__main__":
    spark = main()
    if spark:
        spark.stop()