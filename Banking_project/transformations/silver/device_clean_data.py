from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window


@dp.materialized_view(name="device_sessions_silver")
def device_sessions_silver():

    df = spark.read.table("device_sessions_bronze")

    
    # 1. Remove null critical fields
    
    df = df.filter(
        col("session_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("device_id").isNotNull()
    )

    
    # 2. Fill null values
    
    df = df.fillna({
        "location_country": "UNKNOWN",
        "location_city": "UNKNOWN"
    })

    
    # 3. Normalize text columns
    
    df = df.withColumn("device_type", upper(col("device_type")))
    df = df.withColumn("location_country", initcap(col("location_country")))
    df = df.withColumn("location_city", initcap(col("location_city")))

    
    # 4. Convert boolean columns
    
    df = df.withColumn("is_new_device", col("is_new_device").cast("boolean"))
    df = df.withColumn("is_new_location", col("is_new_location").cast("boolean"))

    
    # 5. Convert timestamps (STRICT FORMAT)
    
    df = df.withColumn(
        "login_timestamp",
        to_timestamp(col("login_timestamp"), "yyyy-MM-dd HH:mm:ss")
    )

    df = df.withColumn(
        "logout_timestamp",
        to_timestamp(col("logout_timestamp"), "yyyy-MM-dd HH:mm:ss")
    )



    # Remove invalid timestamps
    df = df.filter(
        col("login_timestamp").isNotNull() &
        col("logout_timestamp").isNotNull()
    )

    
    # 6. Session duration (optional but useful)
    
    df = df.withColumn(
        "session_duration_seconds",
        unix_timestamp(col("logout_timestamp")) - unix_timestamp(col("login_timestamp"))
    )

    # Remove invalid sessions
    df = df.filter(col("session_duration_seconds") >= 0)

    
    # 7. Deduplicate (latest session)
    
    window_spec = Window.partitionBy("session_id").orderBy(col("login_timestamp").desc())

    df = df.withColumn("rn", row_number().over(window_spec)) \
           .filter(col("rn") == 1) \
           .drop("rn")
    df=df.drop("ingest_ts","browser","os")
    return df
