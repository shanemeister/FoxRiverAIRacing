from pyspark.sql.functions import col, count, when, isnan
from pyspark.sql.types import StringType, NumericType
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import VectorUDT
from pyspark.sql.functions import col, count, when, isnan, sum as spark_sum
from pyspark.sql.types import NumericType, DecimalType, DateType, TimestampType, StringType

def time_series_data_healthcheck(data, datetime_col=None):
    """
    Perform a health check on a dataset to prepare it for time-series analysis.
    
    Parameters:
    - data: PySpark DataFrame, the dataset to check.
    - datetime_col: str, optional, the column representing datetime (for time-series analysis).
    
    Returns:
    - A dictionary summarizing the health check results.
    """
    healthcheck_report = {}

    # 1. Check for datetime column
    if datetime_col:
        if datetime_col not in data.columns:
            healthcheck_report["datetime_col"] = f"Error: {datetime_col} not found in data."
        else:
            datetime_null_count = data.filter(col(datetime_col).isNull()).count()
            healthcheck_report["datetime_col"] = {
                "column": datetime_col,
                "null_count": datetime_null_count
            }

    # 2. Missing values
    missing_value_counts = {}
    for col_name, dtype in data.dtypes:
        if dtype in ["double", "float"]:  # Numeric columns
            missing_value_counts[col_name] = data.filter(isnan(col(col_name)) | col(col_name).isNull()).count()
        else:  # Other columns
            missing_value_counts[col_name] = data.filter(col(col_name).isNull()).count()
    healthcheck_report["missing_values"] = {k: v for k, v in missing_value_counts.items() if v > 0}

    # 3. Decimal Columns
    decimal_columns = [col_name for col_name, dtype in data.dtypes if isinstance(data.schema[col_name].dataType, DecimalType)]
    healthcheck_report["decimal_columns"] = {
        "columns": decimal_columns,
        "suggested_action": "Convert to float for scaling or normalization."
    }

    # 4. Categorical columns
    categorical_columns = [col_name for col_name, dtype in data.dtypes if dtype == "string"]
    healthcheck_report["categorical_columns"] = {
        "columns": categorical_columns,
        "suggested_action": "Consider StringIndexer and OneHotEncoder for these columns."
    }

    # 5. Numerical columns
    numerical_columns = [col_name for col_name, dtype in data.dtypes if dtype in ["double", "float", "int"]]
    healthcheck_report["numerical_columns"] = {
        "columns": numerical_columns,
        "suggested_action": "Consider scaling (MinMaxScaler, StandardScaler) for these columns."
    }

    # 6. Date/Time Columns
    datetime_columns = [col_name for col_name, dtype in data.dtypes if dtype in ["date", "timestamp"]]
    healthcheck_report["datetime_columns"] = {
        "columns": datetime_columns,
        "suggested_action": "Ensure proper time indexing and feature extraction."
    }

    # 7. Column data types summary
    data_types = data.dtypes
    healthcheck_report["data_types"] = data_types

    # 8. Basic statistics for numerical columns
# Numerical Statistics
    try:
        if numerical_columns:
            numerical_stats = {}
            for col_name in numerical_columns:
                summary = data.selectExpr(
                    f"min({col_name}) as min",
                    f"max({col_name}) as max",
                    f"mean({col_name}) as mean",
                    f"stddev({col_name}) as stddev"
                ).collect()[0]
                numerical_stats[col_name] = {key: summary[key] for key in summary.asDict()}
            healthcheck_report["numerical_statistics"] = numerical_stats
        else:
            healthcheck_report["numerical_statistics"] = "No numerical columns found."
    except Exception as e:
        healthcheck_report["numerical_statistics"] = f"Error while computing statistics: {str(e)}"
    # 9. Check for potential issues in data
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