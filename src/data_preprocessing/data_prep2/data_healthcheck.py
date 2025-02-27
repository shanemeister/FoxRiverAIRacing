import logging
import pprint
import os
import sys
import time
import configparser

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, isnan, length, trim, when, sum as F_sum
)
from pyspark.sql.types import DecimalType

def time_series_data_healthcheck(data, datetime_col=None):
    """
    Perform a health check on a dataset to prepare it for time-series analysis.

    This version is optimized to minimize the number of Spark actions.

    Parameters:
    - data: PySpark DataFrame, the dataset to check.
    - datetime_col: str, optional, the column representing datetime (for time-series analysis).

    Returns:
    - A dictionary summarizing the health check results.
    """

    healthcheck_report = {}

    # -------------------------------------------------------
    # 1) Cache / persist the DataFrame to speed repeated ops
    # -------------------------------------------------------
    data.cache()

    # -------------------------------------------------------
    # 2) Check for the datetime_col in schema + null counts
    #    (No separate pass needed; we'll do it with all col missing counts)
    # -------------------------------------------------------
    if datetime_col:
        if datetime_col not in data.columns:
            healthcheck_report["datetime_col"] = f"Error: {datetime_col} not found in data."
        else:
            # We'll store the null count after we do the overall missing values check below
            healthcheck_report["datetime_col"] = {"column": datetime_col}

    # -------------------------------------------------------
    # 3) Identify columns by type (no Spark action, just schema)
    # -------------------------------------------------------
    dtypes_map = dict(data.dtypes)  # {colName: "string"/"double"/...}

    decimal_columns = []
    categorical_columns = []
    numerical_columns = []
    datetime_columns = []

    for field in data.schema.fields:
        col_name = field.name
        spark_type = field.dataType

        # Decimal columns
        if isinstance(spark_type, DecimalType):
            decimal_columns.append(col_name)
        
        # Categorical columns (assuming string type)
        if field.dataType.simpleString() == "string":
            categorical_columns.append(col_name)

        # Numerical columns
        # The simplest approach is to check name or to see if they are integral or fractional
        if spark_type.simpleString() in ("double", "float", "int", "long", "short"):
            numerical_columns.append(col_name)

        # Date/time columns
        if spark_type.simpleString() in ("date", "timestamp"):
            datetime_columns.append(col_name)

    healthcheck_report["decimal_columns"] = {
        "columns": decimal_columns,
        "suggested_action": "Convert to float for scaling or normalization."
    }
    healthcheck_report["categorical_columns"] = {
        "columns": categorical_columns,
        "suggested_action": "Consider StringIndexer and OneHotEncoder for these columns."
    }
    healthcheck_report["numerical_columns"] = {
        "columns": numerical_columns,
        "suggested_action": "Consider scaling (MinMaxScaler, StandardScaler) for these columns."
    }
    healthcheck_report["datetime_columns"] = {
        "columns": datetime_columns,
        "suggested_action": "Ensure proper time indexing and feature extraction."
    }

    # -------------------------------------------------------
    # 4) Missing Value Counts (single pass)
    #    We'll build an expression for each column
    # -------------------------------------------------------
    agg_expressions = []
    for col_name, dtype_str in data.dtypes:
        if dtype_str in ["double", "float"]:
            # Missing = isNull or isNaN
            expr_missing = F_sum(when(col(col_name).isNull() | isnan(col(col_name)), 1).otherwise(0)).alias(f"{col_name}_missing")
        elif dtype_str == "string":
            # Missing = isNull or empty/whitespace
            expr_missing = F_sum(when(col(col_name).isNull() | (length(trim(col(col_name))) == 0), 1).otherwise(0)).alias(f"{col_name}_missing")
        else:
            # For others, just check isNull
            expr_missing = F_sum(when(col(col_name).isNull(), 1).otherwise(0)).alias(f"{col_name}_missing")
        agg_expressions.append(expr_missing)

    # Now do a single aggregation
    missing_df = data.select(agg_expressions)
    missing_counts_row = missing_df.collect()[0].asDict()

    # Parse into a dictionary: { col_name: count_of_missing }
    missing_value_counts = {}
    for k, v in missing_counts_row.items():
        # k is like "colname_missing"
        # v is the integer count
        real_col = k.replace("_missing", "")
        missing_value_counts[real_col] = v

    # Filter out columns with 0 missing
    missing_value_counts_filtered = {c: cnt for c, cnt in missing_value_counts.items() if cnt > 0}
    healthcheck_report["missing_values"] = missing_value_counts_filtered

    # If we had a datetime_col, fill in its null_count from missing_value_counts
    if datetime_col and datetime_col in missing_value_counts:
        healthcheck_report["datetime_col"]["null_count"] = missing_value_counts[datetime_col]

    # -------------------------------------------------------
    # 5) Numeric stats in one pass using data.summary()
    #    summary() => returns rows for each statistic: count, mean, stddev, min, max
    # -------------------------------------------------------
    numeric_stats_dict = {}
    if numerical_columns:
        # 'describe' or 'summary("count","min","max","mean","stddev")' both produce a similar result
        # summary() can also produce quantiles, but let's stick to these basics
        desc_df = data.select(numerical_columns).summary("count", "mean", "stddev", "min", "max")
        # desc_df has rows like: summary, col1, col2, ...
        # We'll collect it into memory
        summary_rows = desc_df.collect()

        # We'll parse them into a dictionary structure
        # Example row => Row(summary='min', col1='0.0', col2='-5.0', ...)
        # We'll do numeric_stats_dict[col1]['min'] = ...
        # We'll do numeric_stats_dict[col1]['max'] = ...
        # etc.
        # Initialize nested dict
        for c in numerical_columns:
            numeric_stats_dict[c] = {}

        for row in summary_rows:
            # row.summary => "count"/"mean"/"stddev"/"min"/"max"
            stat_name = row["summary"]
            for c in numerical_columns:
                val_str = row[c]  # it's a string in summary
                # Convert to float if possible
                if val_str is not None:
                    try:
                        val_f = float(val_str)
                    except ValueError:
                        val_f = None
                else:
                    val_f = None

                numeric_stats_dict[c][stat_name] = val_f

        healthcheck_report["numerical_statistics"] = numeric_stats_dict
    else:
        healthcheck_report["numerical_statistics"] = "No numerical columns found."

    # -------------------------------------------------------
    # 6) Data Types (no Spark action needed, just from data.dtypes)
    # -------------------------------------------------------
    healthcheck_report["data_types"] = data.dtypes  # list of tuples (colName, dtype)

    # -------------------------------------------------------
    # 7) Row count (one pass)
    # -------------------------------------------------------
    row_count = data.count()
    if row_count == 0:
        healthcheck_report["data_status"] = "Error: Dataset is empty."
    else:
        healthcheck_report["data_status"] = f"Dataset contains {row_count} rows."

    return healthcheck_report

def dataframe_summary(spark, df, cols=None):
    """
    Provides a detailed summary of a PySpark DataFrame, including:
    - Schema
    - First 5 rows
    - Shape (number of rows and columns)
    - Summary statistics (min, max, mean, stddev, etc.)
    - Column NA counts
    - NA percentages
    - Summary of specified columns (if cols is provided)
    """
    # Get the schema as a list of column names
    valid_cols = set(df.columns)

    if cols:
        # Validate the provided columns
        invalid_cols = [col for col in cols if col not in valid_cols]
        if invalid_cols:
            print(f"Warning: The following columns are not found in the DataFrame and will be ignored: {invalid_cols}")
        # Only keep valid columns
        cols = [col for col in cols if col in valid_cols]
        if not cols:
            raise ValueError("No valid columns provided for summary.")

    # 1. Show the schema
    print("\n--- DataFrame Schema ---")
    df.printSchema()

    # 2. Show the first 5 rows
    print("\n--- First 5 Rows ---")
    df.show(5, truncate=False)

    # 3. Get the shape of the DataFrame
    print("\n--- DataFrame Shape ---")
    num_rows = df.count()
    num_cols = len(df.columns)
    print(f"Number of rows: {num_rows}, Number of columns: {num_cols}")

    # 4. Generate summary statistics
    print("\n--- Summary Statistics ---")
    df_summary = df.select(cols).summary()
    df_summary.show(truncate=False)

    # 5. NA counts and percentages for each column
    print("\n--- Column NA Counts and Percentages ---")
    na_counts = {}
    for c in cols:
        # Check the data type of the column
        data_type = dict(df.dtypes)[c]

        if data_type in ["double", "float"]:  # Numeric types
            na_count = df.filter(col(c).isNull() | isnan(col(c))).count()
        else:  # Non-numeric types
            na_count = df.filter(col(c).isNull()).count()

        na_counts[c] = na_count

    # Display NA counts and percentages
    for col_name, count_na in na_counts.items():
        na_percentage = (count_na / num_rows) * 100 if num_rows > 0 else 0
        print(f"{col_name}: {count_na} NAs ({na_percentage:.2f}%)")

    # 6. Detailed summary for specific columns if provided
    if cols:
        for col_name in cols:
            print(f"\n--- Detailed Summary for Column: {col_name} ---")
            df.select(col_name).describe().show(truncate=False)

    print("\n--- Report Complete ---")