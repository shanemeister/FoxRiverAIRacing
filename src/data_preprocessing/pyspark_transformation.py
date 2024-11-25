from pyspark.sql import SparkSession
import configparser
import logging
import os


def setup_logging(script_dir, log_dir=None):
    """Sets up logging configuration to write logs to a file and the console."""
    try:
        # Default log directory
        if not log_dir:
            log_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs'
        
        # Ensure the log directory exists
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'SparkPy_load.log')

        # Create a logger and clear existing handlers
        logger = logging.getLogger()
        if logger.hasHandlers():
            logger.handlers.clear()

        logger.setLevel(logging.INFO)

        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Define a common format
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        logger.info("Logging has been set up successfully.")
    except Exception as e:
        print(f"Failed to set up logging: {e}", file=sys.stderr)
        sys.exit(1)

def read_config(config_file_path="config.ini"):
    """Read database configuration from config.ini."""
    config = configparser.ConfigParser()
    config.read(config_file_path)
    if 'database' not in config:
        raise KeyError("Database configuration missing in config.ini")
    return config['database']


def test_connection(script_dir):
    """Test PySpark connection to PostgreSQL."""
    setup_logging(script_dir)

    # Read configuration
    db_config = read_config()

    # JDBC URL for PostgreSQL
    jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    logging.info(f"JDBC URL: {jdbc_url}")

    # Ensure the password is fetched from the config
    if "password" not in db_config or not db_config["password"]:
        logging.error("Database password is missing in the configuration.")
        return

    # JDBC Properties
    jdbc_properties = {
        "user": db_config["user"],
        "password": db_config["password"],  # Use password from config.ini
        "driver": "org.postgresql.Driver"
    }

    # Path to PostgreSQL JDBC Driver
    jdbc_driver_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/jdbc/postgresql-42.7.4.jar"

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("TestPySparkConnection") \
        .config("spark.jars", jdbc_driver_path) \
        .config("spark.driver.memory", "64g") \
        .getOrCreate()

    logging.info("Spark session created successfully.")

    try:
        # Read a small table to test the connection
        table_name = "gpspoint"  # Replace with an existing table in your database
        # query = "(SELECT * FROM sectionals WHERE course_cd = 'AQU') AS subquery"
        #df = spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_properties)
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)

        # Show the schema and first few rows
        logging.info("Connection successful. Table schema:")
        df.printSchema()
        logging.info("Sample data:")
        df.show(5)

    except Exception as e:
        logging.error(f"Failed to connect to the database or read the table: {e}")
    finally:
        # Ensure Spark session is stopped to free resources
        spark.stop()
        logging.info("Spark session stopped.")
        
def test_connection_with_partitioning(script_dir):
    """Test PySpark connection to PostgreSQL with partitioning."""
    setup_logging(script_dir)

    # Read configuration
    db_config = read_config()

    # JDBC URL for PostgreSQL
    jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    logging.info(f"JDBC URL: {jdbc_url}")

    # Ensure the password is fetched from the config
    if "password" not in db_config or not db_config["password"]:
        logging.error("Database password is missing in the configuration.")
        return

    # JDBC Properties
    jdbc_properties = {
        "user": db_config["user"],
        "password": db_config["password"],  # Use password from config.ini
        "driver": "org.postgresql.Driver"
    }

    # Path to PostgreSQL JDBC Driver
    jdbc_driver_path = "/home/exx/myCode/horse-racing/FoxRiverAIRacing/jdbc/postgresql-42.7.4.jar"

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("TestPySparkConnectionWithPartitioning") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()

    logging.info("Spark session created successfully.")

    try:
        # Read a partitioned table
        table_name = "sectionals"
        partition_column = "course_cd"  # Partitioning on `course_cd`
        lower_bound = 0  # Adjust to the lowest value of `course_cd` in your dataset
        upper_bound = 50  # Adjust to the highest value of `course_cd` in your dataset
        num_partitions = 10  # Number of partitions to create

        logging.info(f"Reading table {table_name} with partitioning on {partition_column}.")
        
        # Read the table with partitioning
        df = spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=jdbc_properties,
            column=partition_column,
            lowerBound=lower_bound,
            upperBound=upper_bound,
            numPartitions=num_partitions
        )

        # Show the schema and first few rows
        logging.info("Connection successful with partitioning. Table schema:")
        df.printSchema()
        logging.info("Sample data:")
        df.show(5)

        # Perform some processing (optional)
        df.groupBy("course_cd").count().show()

    except Exception as e:
        logging.error(f"Failed to connect to the database or read the table: {e}")
    finally:
        # Ensure Spark session is stopped to free resources
        spark.stop()
        logging.info("Spark session stopped.")
        
        
def main():
    """Main function to execute all updates."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    test_connection(script_dir)

if __name__ == "__main__":
    main()