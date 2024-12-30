from pyspark.sql import SparkSession
import os
import logging
import pprint
from pyspark.sql.functions import (
    col, unix_timestamp, when, first, last, lag, udf, sum as spark_sum,
    mean as spark_mean, min as spark_min, max as spark_max, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from datetime import timedelta

# Importing utility functions from your project structure
from src.data_preprocessing.data_prep2.data_healthcheck import (
    time_series_data_healthcheck, dataframe_summary
)
from src.data_preprocessing.data_prep1.data_loader import (
    load_data_from_postgresql, reload_parquet_files, 
    load_named_parquet_files, merge_results_sectionals
)
from src.data_preprocessing.data_prep1.sql_queries import sql_queries
from src.data_preprocessing.gps_aggregated.data_features_enhancements import (
    data_enhancements
)
from src.data_preprocessing.gps_aggregated.merge_gps_sectionals_agg import (
    merge_gps_sectionals
)
from src.data_preprocessing.data_prep1.data_utils import (
    save_parquet, gather_statistics, initialize_environment,
    load_config, initialize_logging, initialize_spark, 
    identify_and_impute_outliers, identify_and_remove_outliers, process_merged_results_sectionals,
    identify_missing_and_outliers
)

def clear_screen():
    """Clears the terminal screen."""
    os.system('cls' if os.name == 'nt' else 'clear')

def get_valid_input(prompt, valid_options, allow_exit=True):
    """
    Prompts the user for input until a valid response is received.
    
    Args:
        prompt (str): The input prompt to display.
        valid_options (list): A list of valid responses.
        allow_exit (bool): Whether to allow the user to exit.
        
    Returns:
        str: The validated user input.
    """
    while True:
        user_input = input(prompt).strip().lower()
        if allow_exit and user_input == 'exit':
            print("Exiting the program. Goodbye!")
            exit(0)
        if user_input in valid_options:
            return user_input
        else:
            print(f"Invalid input. Please enter one of the following: {', '.join(valid_options)} or type 'exit' to quit.")

def load_postgresql_data(spark, jdbc_url, jdbc_properties, queries, parquet_dir):
    """
    Main function to load data from PostgreSQL, write to parquet, reload, and print schemas.
    This uses the dictionary of queries to handle all data frames dynamically.
    
    Args:
        spark (SparkSession): The Spark session.
        jdbc_url (str): JDBC URL for PostgreSQL.
        jdbc_properties (dict): JDBC connection properties.
        queries (dict): Dictionary of queries to execute.
        parquet_dir (str): Directory to save parquet files.
        
    Returns:
        dict: Dictionary of loaded DataFrames.
    """
    print("Loading data from PostgreSQL and writing to parquet...")
    try:
        # Load and write data to parquet
        load_data_from_postgresql(spark, jdbc_url, jdbc_properties, queries, parquet_dir)
    
        # Reload data from parquet
        reloaded_dfs = reload_parquet_files(spark, parquet_dir, queries)
    
        # Print schemas dynamically
        for name, df in reloaded_dfs.items():
            print(f"DataFrame '{name}' Schema:")
            df.printSchema()
    
        logging.info("Ingestion job succeeded")
        return reloaded_dfs
    except Exception as e:
        print(f"Error during loading data from PostgreSQL: {e}")
        logging.error(f"Error during loading data from PostgreSQL: {e}")
        return {}

def merge_results_sectionals_action(spark, parquet_dir):
    """
    Merges 'results' and 'sectionals' parquet files.
    
    Args:
        spark (SparkSession): The Spark session.
        parquet_dir (str): Directory where parquet files are stored.
        
    Returns:
        DataFrame or None: The merged DataFrame or None if merging failed.
    """
    print("Merging 'results' with 'sectionals'...")
    try:
        df_names = ["results", "sectionals"]
        loaded_parquet_dfs = load_named_parquet_files(spark, df_names, parquet_dir)
        print("Loaded Parquet DataFrames:")
        for name in loaded_parquet_dfs.keys():
            print(f" - {name}")
        
        if "results" not in loaded_parquet_dfs or "sectionals" not in loaded_parquet_dfs:
            print("Error: Missing 'results' or 'sectionals' in loaded_parquet_dfs. Check your keys!")
            logging.error("Missing 'results' or 'sectionals' in loaded_parquet_dfs.")
            return None
        
        results_df = loaded_parquet_dfs["results"]
        sectionals_df = loaded_parquet_dfs["sectionals"]
 
        print("Merging DataFrames...")
        merge_results_sectionals_df = merge_results_sectionals(spark, results_df, sectionals_df, parquet_dir)
        merge_results_sectionals_df.write.parquet(
            os.path.join(parquet_dir, "merge_results_sectionals.parquet"), 
            mode="overwrite"
        )
        print("Merged DataFrame Schema:")
        merge_results_sectionals_df.printSchema()
        
        healthcheck_report = time_series_data_healthcheck(merge_results_sectionals_df)
        print("Health Check Report:")
        pprint.pprint(healthcheck_report)
        
        logging.info("Merging results with sectionals succeeded")
        return merge_results_sectionals_df
    except Exception as e:
        print(f"Error during merging results and sectionals: {e}")
        logging.error(f"Error during merging results and sectionals: {e}")
        return None

def prep_data_action(spark, parquet_dir):
    """
    Prepares the merged DataFrame for processing.
    
    Args:
        spark (SparkSession): The Spark session.
        parquet_dir (str): Directory where parquet files are stored.
        
    Returns:
        DataFrame: The processed DataFrame.
    """
    print("Preparing merged DataFrame for processing...")
    try:
        merged_df_path = os.path.join(parquet_dir, "enriched_data.parquet")
        if not os.path.exists(merged_df_path):
            print(f"Error: '{merged_df_path}' does not exist.")
            logging.error(f"Parquet file '{merged_df_path}' not found.")
            return None
        merged_df = spark.read.parquet(merged_df_path)
        processed_data = process_merged_results_sectionals(spark, merged_df, parquet_dir)
        save_parquet(spark, processed_data, "processed_data", parquet_dir)
        print("Processed DataFrame saved.")
        logging.info("Data preparation succeeded")
        processed_data.printSchema()
        input("\nPress Enter to continue...")
        return processed_data
    except Exception as e:
        print(f"Error during data preparation: {e}")
        logging.error(f"Error during data preparation: {e}")
        return None

def process_data_interactive(spark, jdbc_url, jdbc_properties, queries, parquet_dir):
    """
    Main function to process data interactively.
    
    Args:
        spark (SparkSession): The Spark session.
        jdbc_url (str): JDBC URL for PostgreSQL.
        jdbc_properties (dict): JDBC connection properties.
        queries (dict): Dictionary of queries to execute.
        parquet_dir (str): Directory to save/read parquet files.
        
    Returns:
        SparkSession: The Spark session after processing data.
    """
    while True:
        clear_screen()
        print("Select an action:")
        print("1) Load data with queries (from PostgreSQL and write to parquet)")
        print("2) Load data from parquet files and process data for time series analysis")
        print("3) Exit")
    
        choice = get_valid_input("Enter your choice (1, 2, or 3): ", ["1", "2", "3"])
    
        if choice == "1":
            try:
                reloaded_dfs = load_postgresql_data(spark, jdbc_url, jdbc_properties, queries, parquet_dir)
                print("Keys in reloaded_dfs:", list(reloaded_dfs.keys()))
            except Exception as e:
                print(f"Error during data loading: {e}")
                logging.error(f"Error during data loading: {e}")
            input("\nPress Enter to continue...")
    
        elif choice == "2":
            # Option 2: Load data from parquet and process
            while True:
                merge_choice = get_valid_input("Merge results with sectionals? (Y/N): ", ["y", "n"])
                if merge_choice == "y":
                    try:
                        merge_results_sectionals_df = merge_results_sectionals_action(spark, parquet_dir)
                        if merge_results_sectionals_df is None:
                            print("Failed to merge results and sectionals. Returning to main menu.")
                            break
                    except Exception as e:
                        print(f"Error during merging results and sectionals: {e}")
                        logging.error(f"Error during merging results and sectionals: {e}")
                        break
                else:
                    print("Skipping merging results with sectionals.")
                    merge_results_sectionals_df = None
    
                gps_merge_choice = get_valid_input("Merge Results and Sectionals with GPS data? (Y/N): ", ["y", "n"])
                if gps_merge_choice == "y":
                    try:
                        merged_df = merge_gps_sectionals(spark, parquet_dir)
                        if merged_df is None:
                            print("Failed to merge GPS data. Returning to main menu.")
                            break
                        else:
                            print("GPS data merged successfully.")
                    except Exception as e:
                        print(f"Error during merging GPS data: {e}")
                        logging.error(f"Error during merging GPS data: {e}")
                        break
                else: 
                    print("Skipping merging GPS data.")  
                                      
                dataprep_choice = get_valid_input("Do you want to populate enhanced features (must have a merged_df from previous step)? (Y/N): ", ["y", "n"])
                if dataprep_choice == "y":
                    try:
                        merged_df = data_enhancements(spark, parquet_dir)
                        if merged_df is not None:
                            print("Data enrichment completed successfully.")
                    except Exception as e:
                        print(f"Error during data enrichment: {e}")
                        logging.error(f"Error during data enrichment: {e}")
                else:
                    print("Data enrichment skipped.")
    
                dataprep_choice = get_valid_input(
                    "Do you want to prep merged_df DataFrame for processing? (Y/N): ", 
                        ["y", "n"]
                )
                if dataprep_choice == "y":
                    try:
                        processed_data = prep_data_action(spark, parquet_dir)
                        if processed_data is not None:
                            print("Data preparation completed successfully.")
                            break  # Exit the inner loop to return to main menu    

                    except Exception as e:
                        print(f"Error during data preparation: {e}")
                        logging.error(f"Error during data preparation: {e}")
                else:
                    print("Data preparation skipped.")   
            else:
                print("No merged DataFrame available for preparation.")
    
        elif choice == "3":
            print("Exiting the program. Goodbye!")
            break
    
    return spark

def main():
    try:
        spark, jdbc_url, jdbc_properties, queries, parquet_dir, log_file = initialize_environment()
        spark.catalog.clearCache()
        # input("Press Enter to continue...")
        spark = process_data_interactive(spark, jdbc_url, jdbc_properties, queries, parquet_dir)
    except Exception as e:
        print(f"An error occurred during initialization: {e}")
        logging.error(f"An error occurred during initialization: {e}")
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped.")

if __name__ == "__main__":
  main()