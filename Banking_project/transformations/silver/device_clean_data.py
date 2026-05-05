import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

@dlt.table(name="bankingdatabase.silver.device_sessions_quarantine")
def device_sessions_quarantine():

    df = dlt.read("device_sessions_bronze")

    invalid_df = df.filter(
        col("session_id").isNull() |
        col("customer_id").isNull() |
        col("device_id").isNull()
    )

    return invalid_df.withColumn("error_reason", lit("Invalid session data"))

@dlt.table(name="device_sessions_silver")
def device_sessions_silver():

    df = dlt.read("device_sessions_bronze")
    df = df.filter(
        col("session_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("device_id").isNotNull()
    )
    df = df.fillna({
        "location_country": "UNKNOWN",
        "location_city": "UNKNOWN"
    })
    df = df.withColumn("device_type", upper(col("device_type")))
    df = df.withColumn("location_country", initcap(col("location_country")))
    df = df.withColumn("location_city", initcap(col("location_city")))
    df = df.withColumn("is_new_device", col("is_new_device").cast("boolean"))
    df = df.withColumn("is_new_location", col("is_new_location").cast("boolean"))
    df = df.withColumn(
        "login_timestamp",
        to_timestamp(col("login_timestamp"), "yyyy-MM-dd HH:mm:ss")
    )
    df = df.withColumn(
        "logout_timestamp",
        to_timestamp(col("logout_timestamp"), "yyyy-MM-dd HH:mm:ss")
    )
    df = df.filter(
        col("login_timestamp").isNotNull() &
        col("logout_timestamp").isNotNull()
    )
    df = df.withColumn(
        "session_duration_seconds",
        unix_timestamp(col("logout_timestamp")) - unix_timestamp(col("login_timestamp"))
    )
    df=df.withColumn("ingest_timestamp",current_timestamp())
    df = df.filter(col("session_duration_seconds") >= 0)
    window_spec = Window.partitionBy("session_id").orderBy(col("login_timestamp").desc())
    df = df.withColumn("rn", row_number().over(window_spec)) \
           .filter(col("rn") == 1) \
           .drop("rn")
    df = df.drop("browser", "os")
    return df