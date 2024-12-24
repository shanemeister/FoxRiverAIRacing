# data_loader.py

import os
import logging
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def load_data_from_postgresql(spark, jdbc_url, jdbc_properties, queries, parquet_dir):
    """
    Load data from PostgreSQL for all queries in the 'queries' dict,
    write them to parquet, and return a dictionary of DataFrames keyed by query name.
    """
    dfs = {}
    for name, query in queries.items():
        logging.info(f"Loading {name} data from PostgreSQL...")
        try:
            print(f"JDBC URL: {jdbc_url}*******************************************************************")
            df = spark.read.jdbc(
                url=jdbc_url,
                table=f"({query}) AS subquery",
                properties=jdbc_properties
            )
            output_path = os.path.join(parquet_dir, f"{name}.parquet")
            logging.info(f"Saving {name} DataFrame to Parquet at {output_path}...")
            df.write.mode("overwrite").parquet(output_path)
            dfs[name] = df
            logging.info(f"{name} data loaded and saved successfully.")
        except Exception as e:
            logging.error(f"Error loading {name} data: {e}")
            raise
    return dfs

def reload_parquet_files(spark, parquet_dir, queries):
    """
    Reload all DataFrames from their Parquet files based on the keys in 'queries'.
    Returns a dictionary of DataFrames keyed by the same names as in 'queries'.
    """
    logging.info("Reloading Parquet files into Spark DataFrames for transformation...")
    reloaded_dfs = {}
    for name in queries.keys():
        path = os.path.join(parquet_dir, f"{name}.parquet")
        if not os.path.exists(path):
            logging.warning(f"No parquet file found for {name} at {path}, skipping...")
            continue
        df = spark.read.parquet(path)
        reloaded_dfs[name] = df
    logging.info("Parquet files reloaded successfully.")
    return reloaded_dfs

def load_named_parquet_files(spark, df_names, parquet_dir):
    logging.info("Reloading Parquet files into Spark DataFrames for transformation...")
    reloaded_dfs = {}
    for name in df_names:
        path = os.path.join(parquet_dir, f"{name}.parquet")
        if not os.path.exists(path):
            logging.warning(f"No parquet file found for {name} at {path}, skipping...")
            continue
        df = spark.read.parquet(path)
        reloaded_dfs[name] = df
        logging.info(f"Loaded DataFrame for {name} from {path}.")
    logging.info("Parquet files reloaded successfully.")
    return reloaded_dfs

def merge_results_sectionals(spark, results_df, sectionals_df, parquet_dir):
    # Define join condition explicitly
    condition = (
        (results_df["course_cd"] == sectionals_df["course_cd"]) &
        (results_df["race_date"] == sectionals_df["race_date"]) &
        (results_df["race_number"] == sectionals_df["race_number"]) &
        (results_df["saddle_cloth_number"] == sectionals_df["saddle_cloth_number"])
    )

    # List out which columns you want from sectionals to avoid duplicates
    # The join keys already exist in results_df, so we don't need them again from sectionals.
    sectionals_cols = [c for c in sectionals_df.columns if c not in ["course_cd", "race_date", "race_number", "saddle_cloth_number"]]

    # Optionally rename these columns to avoid conflicts, if any have the same name as in results
    # For safety, just alias them with a prefix
    sectionals_selected = [col(c).alias(f"sectionals_{c}") for c in sectionals_cols]

    # Perform the join and select from results plus renamed sectionals columns
    merged_df = results_df.join(sectionals_df, condition, "inner") \
                          .select(results_df["*"], *sectionals_selected)
                          
    # Assigns a sequential gate_index based on the sorted sectionals_gate_numeric for each race and horse.
    # Define the race identifier columns
    race_id_cols = ["course_cd", "race_date", "race_number", "horse_id"]
    
    # Define window specification partitioned by race and horse, ordered by sectionals_gate_numeric
    window_spec = Window.partitionBy(*race_id_cols).orderBy("sectionals_gate_numeric")
    
    # Assign a sequential gate_index
    merged_df = merged_df.withColumn("gate_index", row_number().over(window_spec))

    return merged_df