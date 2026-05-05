import dlt
import re
from pyspark.sql.functions import *


# Common function for cleaning columns

def clean_columns(df):
    clean_cols = [
        re.sub(r'[^a-zA-Z0-9_]', '_', col).strip('_')
        for col in df.columns
    ]
    return df.toDF(*clean_cols)
# 1️ Transactions Bronze
@dlt.table(name="transactions_bronze_1")
def transactions_bronze():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.schemaLocation", "s3://manasa-banking-data/schema/transactions/")
        .load("s3://manasa-banking-data/Initial_data/transactions/")
        .select("*", "_metadata")  
    )

    df = clean_columns(df)

    # Add metadata + timestamp
    df = df.withColumn("source_file", col("_metadata.file_path")) \
           .withColumn("file_name", regexp_extract(col("_metadata.file_path"), r'([^/]+$)', 1)) \
           .withColumn("ingest_timestamp", current_timestamp())

    return df



# 2️ Customer Profile Bronze

@dlt.table(name="customer_profile_bronze")
def customer_profile_bronze():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("cloudFiles.schemaLocation", "s3://manasa-banking-data/schema/customers/")
        .load("s3://manasa-banking-data/Initial_data/customer_data/")
        .select("*", "_metadata")   
    )

    df = clean_columns(df)

    df = df.withColumn("source_file", col("_metadata.file_path")) \
           .withColumn("file_name", regexp_extract(col("_metadata.file_path"), r'([^/]+$)', 1)) \
           .withColumn("ingest_timestamp", current_timestamp())

    return df



# 3️ Device Sessions Bronze

@dlt.table(name="device_sessions_bronze")
def device_sessions_bronze():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("cloudFiles.schemaLocation", "s3://manasa-banking-data/schema/devices/")
        .load("s3://manasa-banking-data/Initial_data/device_session/")
        .select("*", "_metadata")   
    )

    df = clean_columns(df)

    df = df.withColumn("source_file", col("_metadata.file_path")) \
           .withColumn("file_name", regexp_extract(col("_metadata.file_path"), r'([^/]+$)', 1)) \
           .withColumn("ingest_timestamp", current_timestamp())

    return df