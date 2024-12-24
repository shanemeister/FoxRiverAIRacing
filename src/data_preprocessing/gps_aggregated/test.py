import os
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from pyspark.sql.functions import expr
from src.data_preprocessing.data_prep1.data_utils import initialize_logging, initialize_spark, sql_queries

def test_sedona_integration(spark):
    try:
        # Create a simple DataFrame with POINT geometries
        test_df = spark.createDataFrame([
            (1, -84.609207, 38.045248),
            (2, -84.608981, 38.047742)
        ], ["id", "longitude", "latitude"])

        # Add a geometry column and project to metric CRS (EPSG:3857)
        test_df = test_df.withColumn("geometry", expr("ST_Transform(ST_Point(longitude, latitude), 3857)"))
        test_df.createOrReplaceTempView("test_points")

        # Perform a buffer operation using Sedona's ST_Buffer
        buffered_df = spark.sql("""
            SELECT 
                id, 
                ST_Buffer(geometry, 1000) AS buffer_geom
            FROM 
                test_points
        """)

        buffered_df.show(truncate=False)
        print("Sedona integration test passed.")
    except Exception as e:
        print(f"Sedona integration test failed: {e}")

def create_dummy_parquet_files(parquet_dir, spark):
    os.makedirs(parquet_dir, exist_ok=True)
    
    # Create dummy gpspoint.parquet
    gps_data = [
        ("course1", "2023-10-10", 1, "SCN1", "H1", -84.609207, 38.045248),
        ("course1", "2023-10-10", 1, "SCN2", "H2", -84.608981, 38.047742)
    ]
    gps_schema = ["course_cd", "race_date", "race_number", "saddle_cloth_number", "horse_id", "longitude", "latitude"]
    gps_df = spark.createDataFrame(gps_data, schema=gps_schema)
    gps_df.write.mode("overwrite").parquet(os.path.join(parquet_dir, "gpspoint.parquet"))
    
    # Create dummy routes.parquet
    routes_data = [
        ("course1", "RUNNING_LINE", [(-84.6100, 38.0450), (-84.6070, 38.0480)]),
        ("course1", "WINNING_LINE", [(-84.6100, 38.0450), (-84.6070, 38.0480)])
    ]
    from pyspark.sql.types import ArrayType, StructType, StructField, DoubleType
    routes_schema = ["course_cd", "line_type", "coordinates"]
    routes_df = spark.createDataFrame(routes_data, schema=routes_schema)
    routes_df.write.mode("overwrite").parquet(os.path.join(parquet_dir, "routes.parquet"))
    
    print("Dummy Parquet files created successfully.")

def calculate_distance_with_sedona(spark, gps_df, routes_df):
    # Ensure both DataFrames have the geometry column in the same CRS
    # Project to metric CRS (EPSG:3857) if not already
    gps_df = gps_df.withColumn("geometry", expr("ST_Transform(ST_Point(longitude, latitude), 3857)"))
    routes_df = routes_df.withColumn("route_geometry", expr("ST_Transform(ST_LineString(coordinates), 3857)"))  # Adjust 'coordinates' column as needed

    # Register DataFrames as Temp Views
    gps_df.createOrReplaceTempView("gps_points")
    routes_df.createOrReplaceTempView("routes")

    # Perform a spatial join to calculate distance
    distance_df = spark.sql("""
        SELECT 
            g.course_cd, 
            g.race_date, 
            g.race_number, 
            g.saddle_cloth_number, 
            g.horse_id,
            r.line_type,
            ST_Distance(g.geometry, r.route_geometry) AS distance_to_line_m
        FROM 
            gps_points g
        JOIN 
            routes r 
        ON 
            g.course_cd = r.course_cd
        WHERE 
            r.line_type IN ('RUNNING_LINE', 'WINNING_LINE')
    """)

    # Pivot the distance metrics
    from pyspark.sql.functions import first
    distance_pivot_df = distance_df.groupBy(
        "course_cd", "race_date", "race_number", "saddle_cloth_number", "horse_id"
    ).pivot("line_type", ["RUNNING_LINE", "WINNING_LINE"]).agg(first("distance_to_line_m"))

    distance_pivot_df = distance_pivot_df.withColumnRenamed("RUNNING_LINE", "distance_to_running_line_m") \
                                         .withColumnRenamed("WINNING_LINE", "distance_to_winning_line_m")

    return distance_pivot_df

def integrate_distance_metrics(main_df, distance_pivot_df):
    enriched_df = main_df.join(
        distance_pivot_df,
        on=["course_cd", "race_date", "race_number", "saddle_cloth_number", "horse_id"],
        how="left"
    )
    return enriched_df

# Main function
def main():
    try:
        # Initialize Spark session
        jdbc_driver_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/jdbc/postgresql-42.7.4.jar"
        sedona_jar_abs_path = "/home/exx/sedona/apache-sedona-1.7.0-bin/sedona-spark-shaded-3.4_2.12-1.7.0.jar"
        
        # Paths to GeoTools JAR files
        geotools_jar_paths = [
            "/home/exx/anaconda3/envs/mamba_env/envs/tf_310/lib/python3.10/site-packages/pyspark/jars/geotools-wrapper-1.1.0-25.2.jar",
            "/home/exx/anaconda3/envs/mamba_env/envs/tf_310/lib/python3.10/site-packages/pyspark/jars/sedona-python-adapter-3.0_2.12-1.2.0-incubating.jar",
            "/home/exx/anaconda3/envs/mamba_env/envs/tf_310/lib/python3.10/site-packages/pyspark/jars/sedona-viz-3.0_2.12-1.2.0-incubating.jar",
        ]
        
        # Initialize Spark session
        spark = initialize_spark(jdbc_driver_path, sedona_jar_abs_path, geotools_jar_paths)
        
        # Example usage of ST_Transform with correct CRS code
        from sedona.sql.types import GeometryType
        from pyspark.sql.functions import col

        # Create a DataFrame with a sample point
        data = [("POINT (-84.609207 38.045248)",)]
        df = spark.createDataFrame(data, ["wkt"])

        # Register the DataFrame as a temporary view
        df.createOrReplaceTempView("points")

        # Use ST_Transform with the correct CRS code
        transformed_df = spark.sql("""
            SELECT ST_Transform(ST_GeomFromWKT(wkt), 'EPSG:4326', 'EPSG:3857') AS geom
            FROM points
        """)

        # Show the results
        transformed_df.show(truncate=False)
        
    except Exception as e:
        print(f"An error occurred during processing: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()
            print("Spark session stopped.")

if __name__ == "__main__":
    main()