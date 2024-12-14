import os
import logging
import argparse
from pyspark.sql.functions import col, upper, trim, row_number
from pyspark.sql.window import Window
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql, reload_parquet_files
from src.data_preprocessing.data_prep1.sql_queries import sql_queries
from src.data_preprocessing.data_prep1.merge_gps_sectional import merge_gps_sectionals
from src.data_preprocessing.data_prep1.data_utils import (
    save_parquet, gather_statistics, initialize_environment,
    load_config, initialize_logging, initialize_spark, drop_duplicates_with_tolerance,
    identify_and_impute_outliers, identify_and_remove_outliers,
    identify_missing_and_outliers
)

def load_postgresql_data(spark, jdbc_url, jdbc_properties, queries, parquet_dir):
    load_data_from_postgresql(spark, jdbc_url, jdbc_properties, queries, parquet_dir)
    sectional_results, results, gpspoint = reload_parquet_files(spark, parquet_dir)
    print("Sectional Results DataFrame Schema:")
    sectional_results.printSchema()
    print("Results DataFrame Schema:")
    results.printSchema()
    print("GPS DataFrame Schema:")
    gpspoint.printSchema()

    return merge_gps_sectionals(spark, sectional_results, gpspoint, parquet_dir)
    
def rebuild_master_df(spark, parquet_dir):
    sectional_results_df, gps_df = reload_parquet_files(spark, parquet_dir)
    matched_df = merge_gps_sectionals(spark, sectional_results_df, gps_df, parquet_dir)
    save_parquet(spark, matched_df, "matched_df", parquet_dir)
    return matched_df

def process_data(spark, parquet_dir, df_name):
    """
    Process data to remove duplicates where sec_time_stamp maps to multiple gps_time_stamp.
    df_name: Name of the DataFrame parquet file to process (without .parquet extension).
    """
    df_path = os.path.join(parquet_dir, f"{df_name}.parquet")
    df = spark.read.parquet(df_path)

    master_df = drop_duplicates_with_tolerance(df)
    gather_statistics(master_df, "master_df")
    save_parquet(spark, master_df, "master_df", parquet_dir)
    return master_df

def manage_outliers_and_missing_data(spark, parquet_dir, df_name, cols, handle_outliers):
    """
    manage_outliers usage:
    python main_model_data_prep.py --manage_outliers df_name col1 col2 ... action
    """
    df_path = os.path.join(parquet_dir, f"{df_name}.parquet")
    df = spark.read.parquet(df_path)

    for column in cols:
        if handle_outliers == 'remove':
            df = identify_and_remove_outliers(df, column)
        elif handle_outliers == 'impute':
            logging.info(f"Imputing outliers for column: {column}")
            df = identify_and_impute_outliers(df, column)

    save_parquet(spark, df, "master_df", parquet_dir)
    spark.catalog.clearCache()

def identify_columns(spark, parquet_dir, df_name, cols):
    """
    identify usage:
    python main_model_data_prep.py --identify df_name col1 col2 col3 ...
    """
    df_path = os.path.join(parquet_dir, f"{df_name}.parquet")
    df = spark.read.parquet(df_path)
    identify_missing_and_outliers(spark, parquet_dir, df, cols)

def merge_results(spark, parquet_dir):
    sectionals_results = spark.read.parquet(os.path.join(parquet_dir, "sectionals_results.parquet"))
    sectionals_results.printSchema()
    print("Sectional Results DataFrame count: {}".format(sectionals_results.count()))
    gpspoint = spark.read.parquet(os.path.join(parquet_dir, "gpspoint.parquet"))
    gpspoint.printSchema()
    print("GPS DataFrame count: {}".format(gpspoint.count()))
    
    gpspoint = gpspoint.withColumn("saddle_cloth_number", upper(trim(col("saddle_cloth_number"))))
    sectionals_results = sectionals_results.withColumn("saddle_cloth_number", upper(trim(col("saddle_cloth_number"))))

    sectionals_results_subset = sectionals_results.select(
        "horse_id", "official_fin", "course_cd", "race_date", "race_number",
        "saddle_cloth_number", "post_pos", "speed_rating", "turf_mud_mark",
        "weight", "morn_odds", "avgspd", "surface", "trk_cond", "class_rating",
        "weather", "wps_pool", "stk_clm_md", "todays_cls", "net_sentiment"
    )

    join_keys = ["course_cd", "race_date", "race_number", "saddle_cloth_number"]
    window_spec = Window.partitionBy(*join_keys).orderBy(col("time_stamp").asc())
    gpspoint = gpspoint.withColumn("row_number", row_number().over(window_spec))

    matched_df = gpspoint.join(sectionals_results_subset, on=join_keys, how="left")
    save_parquet(spark, matched_df, "matched_df", parquet_dir)
    matched_df.printSchema()   
    print("Matched Results DataFrame count: {}".format(matched_df.count()))

def main():
    spark = None
    master_df = None
    matched_df = None

    spark, jdbc_url, jdbc_properties, queries, parquet_dir, log_file = initialize_environment()

    parser = argparse.ArgumentParser(description="Run EQB and TPD ingestion pipeline.")
    parser.add_argument('--rebuild', action='store_true', help="Rebuild master DataFrame.")
    parser.add_argument('--process', nargs=1, help="Process data on a specified df_name to remove sec_time_stamp duplicates. Usage: --process df_name")
    parser.add_argument('--identify', nargs='+', help="Usage: --identify df_name col1 col2 col3 ...")
    parser.add_argument('--manage_outliers', nargs='+', help="Usage: --manage_outliers df_name col1 col2 col3 ... impute/remove")
    parser.add_argument('--results', action='store_true', help="Merge Sectional Results with Matched_df.")
    args = parser.parse_args()

    if args.rebuild:
        matched_df = rebuild_master_df(spark, parquet_dir)
    elif args.process:
        # args.process should be a single-element list containing df_name
        if len(args.process) != 1:
            print("Error: --process requires exactly one argument: df_name")
            exit(1)
        df_name = args.process[0]
        process_data(spark, parquet_dir, df_name=df_name)
    elif args.identify:
        # args.identify: [df_name, col1, col2, ...]
        if len(args.identify) < 2:
            print("Error: --identify requires at least df_name and one column.")
            exit(1)
        df_name = args.identify[0]
        cols = args.identify[1:]
        identify_columns(spark, parquet_dir, df_name, cols)
    elif args.manage_outliers:
        # args.manage_outliers: [df_name, col1, col2, ..., action]
        if len(args.manage_outliers) < 3:
            print("Error: --manage_outliers requires at least df_name, one column, and an action (impute/remove).")
            exit(1)
        df_name = args.manage_outliers[0]
        action = args.manage_outliers[-1]
        cols = args.manage_outliers[1:-1]
        manage_outliers_and_missing_data(spark, parquet_dir, df_name, cols, handle_outliers=action)
    elif args.results:
        merge_results(spark, parquet_dir)
    else:
        matched_df = load_postgresql_data(spark, jdbc_url, jdbc_properties, queries, parquet_dir)
    
    logging.info("Ingestion job succeeded")
    return spark

if __name__ == "__main__":
    spark = main()
    spark.stop()