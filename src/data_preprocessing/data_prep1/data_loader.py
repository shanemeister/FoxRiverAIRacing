# data_loader.py

import os
import logging

def load_data_from_postgresql(spark, jdbc_url, jdbc_properties, queries, parquet_dir):
    dfs = {}
    for name, query in queries.items():
        logging.info(f"Loading {name} data from PostgreSQL...")
        try:
            df = spark.read.jdbc(url=jdbc_url, table=f"({query}) AS subquery", properties=jdbc_properties)
            output_path = os.path.join(parquet_dir, f"{name}.parquet")
            logging.info(f"Saving {name} DataFrame to Parquet at {output_path}...")
            df.write.mode("overwrite").parquet(output_path)
            dfs[name] = df
            logging.info(f"{name} data loaded and saved successfully.")
        except Exception as e:
            logging.error(f"Error loading {name} data: {e}")
            raise
    return dfs

def reload_parquet_files(spark, parquet_dir):
    logging.info("Reloading Parquet files into Spark DataFrames for transformation...")
    sectional_results = spark.read.parquet(os.path.join(parquet_dir, "sectional_results.parquet"))
    results = spark.read.parquet(os.path.join(parquet_dir, "results.parquet"))
    gpspoint = spark.read.parquet(os.path.join(parquet_dir, "gpspoint.parquet"))
    logging.info("Parquet files reloaded successfully.")
    return sectional_results, results, gpspoint