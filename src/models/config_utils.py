# config_utils.py

import os
import logging
import configparser
from pyspark.sql import SparkSession

def load_config(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

def initialize_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logging.info("Environment setup initialized.")

def initialize_spark(jdbc_driver_path):
    spark = SparkSession.builder \
        .appName("Horse Racing Data Processing") \
        .config("spark.driver.extraClassPath", jdbc_driver_path) \
        .config("spark.executor.extraClassPath", jdbc_driver_path) \
        .config("spark.driver.memory", "64g") \
        .config("spark.executor.memory", "32g") \
        .config("spark.executor.memoryOverhead", "8g") \
        .config("spark.sql.debug.maxToStringFields", "1000") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logging.info("Spark session created successfully.")
    return spark