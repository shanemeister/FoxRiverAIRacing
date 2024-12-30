import os
import logging
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import (
    col, lit, struct, collect_list, sort_array, asc, when, upper, trim, min as F_min, regexp_replace,
    sum as F_sum, avg as F_avg, max as F_max,row_number, first, last,
    udf
)
from pyspark.sql.types import ArrayType, StructType, StructField, DoubleType, StringType, IntegerType, DateType
from src.data_preprocessing.data_prep1.data_utils import save_parquet
    

def load_data_from_postgresql(spark, jdbc_url, jdbc_properties, queries, parquet_dir):
    """
    Load data from PostgreSQL for all queries in the 'queries' dict,
    write them to parquet, and return a dictionary of DataFrames keyed by query name.
    """
    dfs = {}
    for name, query in queries.items():
        logging.info(f"Loading {name} data from PostgreSQL...")
        try:
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

def reload_parquet_files(spark, parquet_dir):
    """
    Reload all DataFrames from their Parquet files in the specified directory.
    Returns a dictionary of DataFrames keyed by the file names (without .parquet extension).
    """
    logging.info("Reloading Parquet files into Spark DataFrames for transformation...")
    reloaded_dfs = {}
    for file_name in os.listdir(parquet_dir):
        if file_name.endswith('.parquet'):
            name = file_name.replace('.parquet', '')
            path = os.path.join(parquet_dir, file_name)
            try:
                # Check if the Parquet file is not empty
                if not os.path.exists(path) or os.path.getsize(path) == 0:
                    logging.warning(f"Parquet file {path} is empty or does not exist, skipping...")
                    continue
                df = spark.read.parquet(path)
                reloaded_dfs[name] = df
            except Exception as e:
                logging.error(f"Error reading Parquet file {path}: {e}")
                continue
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
    sectionals_selected = [col(c).alias(f"sec_{c}") for c in sectionals_cols]

    # Perform the join and select from results plus renamed sectionals columns
    merged_df = results_df.join(sectionals_df, condition, "left") \
                          .select(results_df["*"], *sectionals_selected)
                          
    # Assigns a sequential gate_index based on the sorted sectionals_gate_numeric for each race and horse.
    # Define the race identifier columns
    race_id_cols = ["course_cd", "race_date", "race_number", "saddle_cloth_number"]
    
    # Define window specification partitioned by race and horse, ordered by sectionals_gate_numeric
    window_spec = Window.partitionBy(*race_id_cols).orderBy("sectionals_gate_numeric")
    
    # Assign a sequential gate_index
    merged_df = merged_df.withColumn("gate_index", row_number().over(window_spec))

    return merged_df

def merge_results_sectionals_with_agg_features(
    spark,
    results_df: DataFrame,
    sectionals_df: DataFrame,
    parquet_dir: str
) -> DataFrame:
    """
    Merges `results_df` (one row per horse) with aggregated + snapshot features
    derived from `sectionals_df` (per-gate data).

    Returns a single row per (course_cd, race_date, race_number, saddle_cloth_number) 
    with:
      - All columns from `results_df`
      - Aggregated columns (sum/avg of gate-level fields)
      - 'first', 'median', and 'final' gate snapshots (e.g. running_time, distance_back)

    Args:
        spark        : SparkSession
        results_df   : DataFrame with one row per horse (Equibase results).
        sectionals_df: DataFrame with multi-row gate data (sectionals).
        parquet_dir  : Directory where you might write/read parquet files (optional usage).

    Returns:
        DataFrame: A single row per horse–race with aggregator columns and gate snapshots.
    """

    # ------------------------------------------------------------------------------
    # 1) Create a DataFrame that does BOTH: 
    #    (A) aggregator columns (sum, max, etc.)
    #    (B) array of gates for picking out first, median, final.
    # ------------------------------------------------------------------------------

    results_df = results_df.withColumn("saddle_cloth_number", regexp_replace(trim(upper(col("saddle_cloth_number"))), '\s+$', ''))
    sectionals_df = sectionals_df.withColumn("saddle_cloth_number", regexp_replace(trim(upper(col("saddle_cloth_number"))), '\s+$', ''))
    
    results_df = (
        results_df
        .withColumn("race_number", col("race_number").cast("integer"))
        .withColumn("race_date", col("race_date").cast("date"))
        .withColumn("course_cd", regexp_replace(trim(upper(col("course_cd"))), '\s+$', ''))
        )

    sectionals_df = (
        sectionals_df
        .withColumn("race_number", col("race_number").cast("integer"))
        .withColumn("race_date", col("race_date").cast("date"))
        .withColumn("course_cd", regexp_replace(trim(upper(col("course_cd"))), '\s+$', ''))
        )

    grouped_sectionals = (
        sectionals_df
        .groupBy("course_cd", "race_date", "race_number", "saddle_cloth_number")
        .agg(
            # (A) aggregator columns
            F.avg("sectional_time").alias("avg_sectional_time"),
            F.max("running_time").alias("max_running_time"),           # final race time might be a large 'running_time'
            F.sum("number_of_strides").alias("total_strides"),
            F.sum("distance_ran").alias("total_distance_ran"),
            F.max("distance_back").alias("max_distance_back"),

            # (B) collect all gates in an array -> used for (first, median, final)
            collect_list(
                struct(
                    col("gate_name").alias("gate_name"),
                    col("gate_numeric").alias("gate_numeric"),
                    col("sectional_time").alias("sectional_time"),
                    col("running_time").alias("running_time"),
                    col("distance_back").alias("distance_back"),
                    col("distance_ran").alias("distance_ran"),
                    col("number_of_strides").alias("number_of_strides")
                )
            ).alias("gates_array")
        )
    )

    # We might want to compute 'avg_stride_length': total_distance_ran / total_strides
    grouped_sectionals = grouped_sectionals.withColumn(
        "avg_stride_length",
        (col("total_distance_ran") / col("total_strides"))
    )

    # ------------------------------------------------------------------------------
    # 2) Sort gates_array by gate_numeric so we can reliably pick [0], [mid], [-1]
    # ------------------------------------------------------------------------------
    grouped_sectionals = grouped_sectionals.withColumn(
        "gates_array",
        sort_array(col("gates_array"), asc=True)
    )

    # ------------------------------------------------------------------------------
    # 3) Define a Python UDF to pick out (first, median, final) gate's fields
    # ------------------------------------------------------------------------------
    def pick_gate_snapshots(gates_list):
        """
        gates_list: a Python list of dict-like objects with:
            gate_numeric, gate_name, sectional_time, running_time, distance_back, distance_ran, number_of_strides

        Returns a tuple of length e.g. 6 or more, containing:
          first_running_time, first_distance_back,
          median_running_time, median_distance_back,
          final_running_time, final_distance_back
          (Add more if you like!)
        """
        if not gates_list:
            return (-999.0, -999.0, -999.0, -999.0, -999.0, -999.0)

        # sort by gate_numeric already done above, so the list is in ascending order
        n = len(gates_list)

        # FIRST gate
        first_gate = gates_list[0]

        # MEDIAN gate
        mid_idx = n // 2  # integer division
        median_gate = gates_list[mid_idx]

        # FINAL gate
        final_gate = gates_list[-1]

        return (
            first_gate["running_time"],
            first_gate["distance_back"],
            median_gate["running_time"],
            median_gate["distance_back"],
            final_gate["running_time"],
            final_gate["distance_back"]
        )

    # Create a return schema for the UDF as an array or struct of doubles
    pick_gate_snapshots_udf = F.udf(pick_gate_snapshots, 
        returnType=F.ArrayType(DoubleType())
    )

    grouped_sectionals = grouped_sectionals.withColumn(
        "snapshot_array",
        pick_gate_snapshots_udf(col("gates_array"))
    )

    # ------------------------------------------------------------------------------
    # 4) Expand the snapshot_array into named columns
    # ------------------------------------------------------------------------------
    grouped_sectionals = grouped_sectionals.select(
        "*",
        col("snapshot_array").getItem(0).alias("first_running_time"),
        col("snapshot_array").getItem(1).alias("first_distance_back"),
        col("snapshot_array").getItem(2).alias("median_running_time"),
        col("snapshot_array").getItem(3).alias("median_distance_back"),
        col("snapshot_array").getItem(4).alias("final_running_time"),
        col("snapshot_array").getItem(5).alias("final_distance_back"),
    ).drop("snapshot_array", "gates_array")

    # ------------------------------------------------------------------------------
    # 5) Now we have aggregator columns + first/median/final columns
    #    -> join with results_df
    # ------------------------------------------------------------------------------
    join_condition = (
        (results_df["course_cd"] == grouped_sectionals["course_cd"]) &
        (results_df["race_date"] == grouped_sectionals["race_date"]) &
        (results_df["race_number"] == grouped_sectionals["race_number"]) &
        (results_df["saddle_cloth_number"] == grouped_sectionals["saddle_cloth_number"])
    )

    # Left join so we keep all results
    merged_df = (
        results_df
        .join(grouped_sectionals, on=join_condition, how="left")
        .select(
            results_df["*"],
            col("avg_sectional_time"),
            col("max_running_time").alias("total_running_time"),
            col("total_strides"),
            col("total_distance_ran"),
            col("max_distance_back"),
            col("avg_stride_length"),
            col("first_running_time"),
            col("first_distance_back"),
            col("median_running_time"),
            col("median_distance_back"),
            col("final_running_time"),
            col("final_distance_back")
        )
    )
    
    # Convert distance from Furlongs (F) to meters if dist_unit is F
    merged_df = merged_df.withColumn(
        "distance_meters",
        when(upper(trim(col("dist_unit"))) == "F", ((col("distance") / 100)) * lit(201.168))
        .otherwise(lit(None))
    )

    merged_df.printSchema()
    merged_df.select("distance", "dist_unit", "distance_meters").show(10, truncate=False)

    # Drop the old columns
    merged_df = merged_df.drop("distance", "dist_unit")
    
    return merged_df