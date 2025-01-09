import os
import logging
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from pyspark.sql.functions import (col, count, row_number, abs, unix_timestamp,  
                                   when, lit, min as F_min, max as F_max , upper, trim,
                                   row_number, mean as F_mean, countDistinct, last, first, when)
from src.inference.inference_sql_queries import sql_queries
from src.data_preprocessing.data_prep1.data_loader import load_data_from_postgresql

def impute_date_of_birth_with_median(df):
    """  
    Impute date_of_birth with the median value (or a default if no data exists).   
    """
    race_df = race_df.withColumn("date_of_birth_ts", F.col("date_of_birth").cast("timestamp").cast("long"))
    median_window = Window.orderBy("date_of_birth_ts")

    median_ts = race_df.filter(F.col("date_of_birth_ts").isNotNull()).approxQuantile("date_of_birth_ts", [0.5], 0)[0]
    if median_ts is None:
        median_date = F.lit("2000-01-01").cast("date")
    else:
        median_date = F.from_unixtime(F.lit(median_ts)).cast("date")

    race_df = race_df.withColumn(
        "date_of_birth",
        F.when(F.col("date_of_birth").isNull(), median_date).otherwise(F.col("date_of_birth"))
    ).drop("date_of_birth_ts")
    print("3a. Missing date_of_birth values imputed with median date.")
    return df

def load_base_inference_data(spark, jdbc_url, jdbc_properties, parquet_dir):
    """
    Load Parquet file used to train
    """
    queries = sql_queries()
    for name, query in queries.items():
        if name == "infer_results":
            race_df = spark.read.jdbc(
                url=jdbc_url,
                table=f"({query}) AS subquery",
                properties=jdbc_properties
            )
            race_df.printSchema()
    
    # output_path = os.path.join(parquet_dir, f"{name}.parquet")
    # logging.info(f"Saving {name} DataFrame to Parquet at {output_path}...")
    # race_df.write.mode("overwrite").parquet(output_path)
    # logging.info(f"{name} data loaded and saved successfully.")
    
    # Check for Dups:
    primary_keys = ["course_cd", "race_date", "race_number", "horse_id"]
    duplicates = (
        race_df.groupBy(*primary_keys)
        .agg(F.count("*").alias("cnt"))
        .filter(F.col("cnt") > 1)
    )

    dup_count = duplicates.count()
    if dup_count > 0:
        print(f"Found {dup_count} duplicate primary key combinations.")
        duplicates.show()
        raise ValueError(f"Duplicates found: {dup_count}. Deduplication required.")

    print(f"Duplicate count on PK: {dup_count}")
    
    # 2. Convert Decimal Columns to Double
    decimal_cols = ["weight", "power", "distance", "morn_odds", 
                    "class_rating", "all_earnings", "cond_earnings", 
                "jock_win_percent", "jock_itm_percent", "trainer_itm_percent", 
                    "trainer_win_percent", "jt_win_percent", "jt_itm_percent",
                    "jock_win_track", "jock_itm_track", "trainer_win_track", "trainer_itm_track",
                    "jt_win_track", "jt_itm_track"]
    for col_name in decimal_cols:
        race_df = race_df.withColumn(col_name, F.col(col_name).cast("double"))
    logging.info("Decimal columns converted to double.")
    print("2. Decimal columns converted to double.")
    
    # 3b. Create age_at_race_day
    race_df = race_df.withColumn(
        "age_at_race_day",
        F.datediff(F.col("race_date"), F.col("date_of_birth")) / 365.25
    )
    logging.info("Created age_at_race_day.")
    print("3b. Created age_at_race_day.")
    
    # 3c. Impute categorical and numeric columns -- ensure no whitespace in categorical columns
    categorical_defaults = {"weather": "UNKNOWN", "turf_mud_mark": "MISSING", "trk_cond": "UNKNOWN"}
    # Fill missing values for categorical defaults
    race_df = race_df.fillna(categorical_defaults)
    # Impute med with NONE
    race_df = race_df.withColumn("med", when(col("med") == "", "NONE").otherwise(col("med")))
    # Impute turf_mud_mark with MISSING
    race_df = race_df.withColumn("turf_mud_mark",when(col("turf_mud_mark") == "", "MISSING").otherwise(col("turf_mud_mark")))
    
    # 11a.) Convert distance from Furlongs (F) to meters if dist_unit is F
    #    1 Furlong ≈ 201.168 meters.
    #    Drop distance and dist_unit.

    race_df = race_df.withColumn(
        "distance_meters",
        when(upper(trim(col("dist_unit"))) == "F", ((col("distance") / 100)) * lit(201.168))  # Convert furlongs to meters
        .when(upper(trim(col("dist_unit"))) == "M", col("distance"))  # Keep meters as-is
        .otherwise(lit(None))  # Set to None for other cases
    )
    
    race_df.select("distance", "dist_unit", "distance_meters").show(10, truncate=False)
    
    # Drop the old columns
    race_df = race_df.drop("distance", "dist_unit", "sa_dist_bk_gate4")

    # Drop the following columns that provide GPS coordinates and aggregate statistics:
    
    gps_cols = [
    "avg_acceleration", "avg_speed_3", "avg_speed_5", "speed_q1", "speed_q2",
    "speed_q3", "speed_q4", "total_dist_covered", "gps_avg_stride_length",
    "net_progress_gain", "running_time", "avg_beaten_3", "avg_beaten_5", "avg_fin_3",
    "avg_fin_5", "speed_improvement", "avgtime_gate1", "avgtime_gate2", "avgtime_gate3",
    "avgtime_gate4","total_distance_ran"
]
    race_df = race_df.drop(*gps_cols)
   
    # Impute numeric columns with the mean

    impute_cols = [
        "all_starts", "all_win", "all_place", "cond_earnings", "all_earnings", "all_fourth", "all_show",
        "cond_starts", "cond_win", "cond_place", "cond_fourth", "cond_show",
        "jock_itm_percent", "jock_itm_track", "jock_win_percent", "jock_win_track", 
        "jt_itm_percent", "jt_itm_track", "jt_win_percent", "jt_win_track",
        "trainer_itm_percent", "trainer_itm_track", "trainer_win_percent", "trainer_win_track",
    ]
    for col_name in impute_cols:
        mean_value = race_df.select(F_mean(col(col_name))).first()[0]
        if mean_value is not None:
            race_df = race_df.fillna({col_name: mean_value})
    else:
        logging.warning(f"Mean value for column {col_name} is None. Skipping imputation for this column.")
        
    # Imppute missing values with 0
    zero_fill_cols = ["net_sentiment", "pstyerl", "morn_odds", "days_off"]
    race_df = race_df.fillna({col_name: 0 for col_name in zero_fill_cols})
    
    # -1 would have meaning in net_sentiment for sure, and 0 might mean something as well for days_off 
    negative_fill_cols = ["net_sentiment", "pstyerl", "morn_odds", "days_off"]
    race_df = race_df.fillna({col_name: -1 for col_name in negative_fill_cols})

    # Impute categorical columns with the "MISSING"
    cat_cols = ["surface", "trk_cond", "weather", "race_type", "stk_clm_md"]
    for col_name in cat_cols:
        race_df = race_df.fillna({col_name: "MISSING"})
    
    # filtered_df = race_df.filter(col("days_off").isNull()).select("layoff_cat", "days_off")

    # # Show the filtered DataFrame
    # filtered_df.show(74)    
    
    return race_df
        