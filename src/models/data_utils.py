import os
import logging

def save_parquet(df, name, parquet_dir):
    """
    Save a PySpark DataFrame as a Parquet file.
    
    :param df: PySpark DataFrame to save
    :param name: Name of the DataFrame (used for the file name)
    :param parquet_dir: Directory to save the Parquet file
    :return: None
    """
    output_path = os.path.join(parquet_dir, f"{name}.parquet")
    logging.info(f"Saving {name} DataFrame to Parquet at {output_path}...")
    logging.info(f"Schema of {name} DataFrame:")
    df.printSchema()
    df.write.mode("overwrite").parquet(output_path)    
    