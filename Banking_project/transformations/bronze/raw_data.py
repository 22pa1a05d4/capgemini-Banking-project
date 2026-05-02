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


# 1️ Transactions Bronze (UPDATED)

@dlt.table(name="transactions_bronze_1")
def transactions_bronze():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("cloudFiles.schemaEvolutionMode", "rescue") 
        .load("s3://manasa-banking-data/Initial_data/transactions/")
    )

    df = clean_columns(df)

    

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
        .load("s3://manasa-banking-data/Initial_data/customer_data/")
    )

    return clean_columns(df)


# 3️ Device Sessions Bronze

@dlt.table(name="device_sessions_bronze")
def device_sessions_bronze():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load("s3://manasa-banking-data/Initial_data/device_session/")
    )
    

    return clean_columns(df)