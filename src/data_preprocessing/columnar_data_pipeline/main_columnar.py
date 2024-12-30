import os
import logging
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from src.data_preprocessing.columnar_data_pipeline.merge_and_agg_gps_data import results_sectionals_gps_merged_agg_features
from pyspark.sql.functions import trim

# -- Our own modules and utility imports --
from src.data_preprocessing.data_prep1.data_utils import (
    initialize_environment,
    load_config,
    initialize_logging,
    initialize_spark,
    save_parquet
)
from src.data_preprocessing.data_prep1.data_loader import (
    load_data_from_postgresql,
    reload_parquet_files,
    load_named_parquet_files,
    merge_results_sectionals_with_agg_features
)
from src.data_preprocessing.data_prep1.sql_queries import sql_queries
from src.data_preprocessing.gps_aggregated.data_features_enhancements import (
    data_enhancements
)

def cleanse_data(spark, dfs):
    """
    Ensure that all commonly named attributes are the correct datatype across DataFrames.
    """
    # Define the correct schema for common columns
    common_schema = {
        "course_cd": StringType(),
        "race_date": DateType(),
        "race_number": IntegerType(),
        "saddle_cloth_number": StringType(),
        "horse_id": IntegerType(),
        "purse": IntegerType(),
        "wps_pool": DecimalType(10, 2),
        "weight": DecimalType(10, 2),
        "date_of_birth": DateType(),
        "start_position": LongType(),
        "claimprice": DoubleType(),
        "distance": DecimalType(10, 2),
        "power": DecimalType(10, 2),
        "morn_odds": DecimalType(10, 2),
        "avgspd": DoubleType(),
        "class_rating": IntegerType(),
        "net_sentiment": IntegerType(),
        "avg_spd_sd": DoubleType(),
        "ave_cl_sd": DoubleType(),
        "hi_spd_sd": DoubleType(),
        "pstyerl": DoubleType(),
        "all_starts": IntegerType(),
        "all_win": IntegerType(),
        "all_place": IntegerType(),
        "all_show": IntegerType(),
        "all_fourth": IntegerType(),
        "all_earnings": DecimalType(12, 2),
        "cond_starts": IntegerType(),
        "cond_win": IntegerType(),
        "cond_place": IntegerType(),
        "cond_show": IntegerType(),
        "cond_fourth": IntegerType(),
        "cond_earnings": DecimalType(12, 2),
        "time_stamp": TimestampType(),
        "longitude": DoubleType(),
        "latitude": DoubleType(),
        "speed": DoubleType(),
        "progress": DoubleType(),
        "stride_frequency": DoubleType(),
        "post_time": TimestampType(),
        "location": StringType(),
        "gate_name": StringType(),
        "gate_numeric": DoubleType(),
        "length_to_finish": DoubleType(),
        "sectional_time": DoubleType(),
        "running_time": DoubleType(),
        "distance_back": DoubleType(),
        "distance_ran": DoubleType(),
        "number_of_strides": DoubleType()
    }

    # Iterate through DataFrames and cast columns to correct datatypes
    for name, df in dfs.items():
        for col_name, col_type in common_schema.items():
            if col_name in df.columns:
                # Trim spaces for string columns
                if isinstance(col_type, StringType):
                    df = df.withColumn(col_name, trim(df[col_name]))
                # Cast to the correct datatype
                df = df.withColumn(col_name, df[col_name].cast(col_type))
        dfs[name] = df

    return dfs

def save_dataframes_to_parquet(dfs, parquet_dir):
    """
    Save the DataFrames to Parquet files.
    """
    for name, df in dfs.items():
        output_path = os.path.join(parquet_dir, f"{name}.parquet")
        logging.info(f"Saving {name} DataFrame to Parquet at {output_path}...")
        df.write.mode("overwrite").parquet(output_path)
        logging.info(f"{name} DataFrame saved successfully.")

def main(reload_db: bool):
    """
    Main driver function to:
      1) Initialize environment (Spark, config, logging, etc.)
      2) Load data from PostgreSQL to Parquet
      3) Possibly reload that Parquet for further transformations
      4) Merge results & sectionals, do advanced GPS enhancements
      5) Save final datasets
    """
    spark = None
    try:
        # 1) Initialize environment
        spark, jdbc_url, jdbc_properties, queries, parquet_dir, log_file = initialize_environment()
        logging.info("Environment initialized successfully.")

        if reload_db:
            # 2) Load data from PostgreSQL (only if needed)
            logging.info("Starting data load from PostgreSQL.")
            dfs = load_data_from_postgresql(spark, jdbc_url, jdbc_properties, queries, parquet_dir)
            dfs = cleanse_data(spark, dfs)
            save_dataframes_to_parquet(dfs, parquet_dir)
        else:
            logging.info("Skipping data load from PostgreSQL. Using existing Parquet files.")
            # 3) Reload Parquet data if needed
            logging.info("Reloading parquet data into Spark DataFrames.")
            dfs = reload_parquet_files(spark, parquet_dir)
            dfs = cleanse_data(spark, dfs)

        # Invalidate cache to ensure Spark reads the latest data
        spark.catalog.clearCache()

        dfs_names = list(dfs.keys())
        logging.info(f"DataFrame names: {dfs_names}")

        # Ensure "results" and "sectionals" DataFrames are available
        if "results" not in dfs or "sectionals" not in dfs:
            raise ValueError("Both 'results' and 'sectionals' DataFrames must be available for merging.")

        # 4) Merge Equibase results with Sectionals data
        logging.info("Merging Equibase results with Sectionals to create a unified dataset.")
        merged_results_sectionals = merge_results_sectionals_with_agg_features(
            spark, dfs["results"], dfs["sectionals"], parquet_dir
        )
        save_parquet(spark, merged_results_sectionals, "merged_results_sectionals", parquet_dir)

        # 5) Merge GPS data with merged_results_sectionals (if you do that)
        logging.info("Merging GPS data with sectionals for aggregator.")
        gpspoint_df = dfs.get("gpspoint")
        if gpspoint_df is None:
            raise ValueError("The 'gpspoint' DataFrame must be available for merging GPS data.")
        results_sectionals_gps_merged = results_sectionals_gps_merged_agg_features(spark, merged_results_sectionals, gpspoint_df, parquet_dir)
        
        #######################################
        # SAVE FINAL MERGED DATAFRAME
        #######################################               
        save_parquet(spark, results_sectionals_gps_merged, "results_sectionals_gps_merged", parquet_dir)
        
        logging.info("Data pipeline completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred in main: {e}", exc_info=True)

    finally:
        if spark:
            spark.stop()
            logging.info("Spark session stopped.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the data pipeline.")
    parser.add_argument(
        "--reload_db",
        action="store_true",
        help="Reload data from the database instead of using existing Parquet files."
    )
    args = parser.parse_args()
    main(reload_db=args.reload_db)