#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, when, udf, current_timestamp, date_format, abs, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from cassandra.cluster import Cluster
import uuid
import logging
import time
import sys


#Pre-processing the data
# Function to configure logging
def configure_logging():
    """Configures logging for the script."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to clean categorical data
def clean_categorical_data(df, columns):
    """Cleans categorical data in the DataFrame."""
    for column in columns:
        df = df.withColumn(column, when(col(column).isNull(), "unknown").otherwise(col(column)))
    return df

# Function to clean numerical data
def clean_numerical_data(df, columns):
    """Cleans numerical data in the DataFrame."""
    for column in columns:
        df = df.withColumn(column, when(col(column).isNull(), lit(0)).otherwise(abs(col(column))))
    return df

# Function to clean transaction time data
def clean_transaction_time(df, column):
    """Cleans transaction time data in the DataFrame."""
    default_time = "Sun Jan 00 00:00:00 IST 0000"
    df = df.withColumn(column, when(col(column).isNull(), default_time).otherwise(col(column)))
    return df

# Function to generate UUID
def generate_uuid():
    """Generates a UUID."""
    return str(uuid.uuid4())


# Function to wait for Cassandra to be ready
def wait_for_cassandra():
    """Waits for Cassandra to be ready."""
    retries = 5
    delay = 10  # seconds

    while retries > 0:
        try:
            cluster = Cluster(['cassandra'])
            session = cluster.connect()
            session.execute("SELECT now() FROM system.local")  # A simple query to check Cassandra connectivity
            cluster.shutdown()
            return True
        except Exception as e:
            logging.error(f"Failed to connect to Cassandra: {e}")
            retries -= 1
            if retries > 0:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)

    logging.error("Failed to connect to Cassandra after retries, exiting...")
    return False

# Function to create Cassandra schema
def create_cassandra_schema():
    """Creates the Cassandra schema."""
    cluster = Cluster(['cassandra'])
    session = cluster.connect()

    keyspace_query = """
    CREATE KEYSPACE IF NOT EXISTS transaction_data 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """
    session.execute(keyspace_query)

    session.set_keyspace('Weather_forecast')

    create_table = """
    CREATE TABLE IF NOT EXISTS Weather_forecast_table(
        timestamp TIMESTAMP,
        temperature_2m FLOAT,
        relative_humidity_2m FLOAT,
        dew_point_2m FLOAT,
        apparent_temperature FLOAT,
        precipitation FLOAT,
        rain FLOAT,
        snowfall FLOAT,
        snow_depth FLOAT,
        pressure_msl FLOAT,
        surface_pressure FLOAT,
        sunshine_duration FLOAT,
        shortwave_radiation FLOAT,
        direct_radiation FLOAT,
        diffuse_radiation FLOAT,
        direct_normal_irradiance FLOAT,
        global_tilted_irradiance FLOAT,
        terrestrial_radiation FLOAT,
        PRIMARY KEY (timestamp);    
    """
    try:
        session.execute(create_table)
        logging.info("Table created Successfully")
    except Exception as e:
        logging.error(f"Error creating table: {e}")

    cluster.shutdown()

#Aggregating the data
# Function to write to Cassandra
def write_to_cassandra(df, epoch_id):
    """Writes DataFrame to Cassandra."""
    create_cassandra_schema()
    df.select(
        col("timestamp"),
        col("temperature_2m").alias("temperature_2m"),
        col("relative_humidity_2m").alias("relative_humidity_2m"),
        col("dew_point_2m").alias("dew_point_2m"),
        col("apparent_temperature").alias("apparent_temperature"),
        col("precipitation").alias("precipitation"),
        col("rain").alias("rain"),
        col("snowfall").alias("snowfall"),
        col("snow_depth").alias("snow_depth"),
        col("pressure_msl").alias("pressure_msl"),
        col("surface_pressure").alias("surface_pressure"),
        col("sunshine_duration").alias("sunshine_duration"),
        col("shortwave_radiation").alias("shortwave_radiation"),
        col("direct_radiation").alias("direct_radiation"),
        col("diffuse_radiation").alias("diffuse_radiation"),
        col("direct_normal_irradiance").alias("direct_normal_irradiance"),
        col("global_tilted_irradiance").alias("global_tilted_irradiance"),
        col("terrestrial_radiation").alias("terrestrial_radiation")
    ).write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="Weather_forecast_table", keyspace="Weather_forecast") \
        .save()

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.kafka:kafka-clients:3.7.0") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Configure logging
configure_logging()

# Check if Cassandra is ready before proceeding
if not wait_for_cassandra():
    sys.exit(1)  # Exit the script if Cassandra is not ready

# Kafka configuration
kafka_topic = "Weather_forecast"
kafka_bootstrap_servers = "broker:29092"

csv_schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("temperature_2m", FloatType()),
    StructField("relative_humidity_2m", FloatType()),
    StructField("dew_point_2m", FloatType()),
    StructField("apparent_temperature", FloatType()),
    StructField("precipitation", FloatType()),
    StructField("rain", FloatType()),
    StructField("snowfall", FloatType()),
    StructField("snow_depth", FloatType()),
    StructField("pressure_msl", FloatType()),
    StructField("surface_pressure", FloatType()),
    StructField("sunshine_duration", FloatType()),
    StructField("shortwave_radiation", FloatType()),
    StructField("direct_radiation", FloatType()),
    StructField("diffuse_radiation", FloatType()),
    StructField("direct_normal_irradiance", FloatType()),
    StructField("global_tilted_irradiance", FloatType()),
    StructField("terrestrial_radiation", FloatType())
])


# Read from Kafka stream
kafka_raw_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse CSV data
parsed_data = kafka_raw_data \
    .selectExpr("CAST(value AS STRING) as csv_data") \
    .select(split("csv_data", ",").alias("csv_array"))

# Apply CSV schema
for i, field in enumerate(csv_schema.fields):
    parsed_data = parsed_data.withColumn(field.name, col("csv_array")[i].cast(field.dataType))

# Clean categorical and numerical data

numerical_columns = ["timestamp", "temperature_2m", "sunshine_duration", "direct_radiation"]


parsed_data = clean_numerical_data(parsed_data, numerical_columns)

# Clean transaction time and generate UUID
parsed_data = clean_transaction_time(parsed_data, "timestamp")
generate_uuid_udf = udf(generate_uuid, StringType())
parsed_data = parsed_data.withColumn("timestamp", generate_uuid_udf())

# Write to Cassandra
parsed_query = parsed_data \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_cassandra) \
    .start()

# Wait for termination
parsed_query.awaitTermination()

# Stop Spark session
spark.stop()

