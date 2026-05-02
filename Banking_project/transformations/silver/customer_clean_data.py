from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window


@dp.materialized_view(name="customer_profile_silver")
def customer_profile_silver():

    df = spark.read.table("customer_profile_bronze")


    df = df.filter(
        col("customer_id").isNotNull() &
        col("account_id").isNotNull() &
        col("email").isNotNull()
    )
  
    df = df.withColumn(
        "email",
        lower(regexp_replace(col("email"), "@.*", "@gmail.com"))
    )


    df = df.withColumn(
        "account_balance",
        col("account_balance").cast("double")
    )

    # Remove invalid balances
    df = df.filter(col("account_balance").isNotNull())


    df = df.withColumn(
        "date_of_birth",
        to_date(col("date_of_birth"), "yyyy-MM-dd")
    ).withColumn(
        "account_opening_date",
        to_date(col("account_opening_date"), "yyyy-MM-dd")
    )

    # Remove invalid dates
    df = df.filter(
        col("date_of_birth").isNotNull() &
        col("account_opening_date").isNotNull()
    )

    
    # 5. Standardization
    
    df = df.withColumn("kyc_status", upper(col("kyc_status")))
    df = df.withColumn("account_type", upper(col("account_type")))

    df = df.withColumn("home_country", initcap(col("home_country"))) \
           .withColumn("home_city", initcap(col("home_city")))

    
    # 6. Age calculation
    
    df = df.withColumn(
        "age",
        floor(datediff(current_date(), col("date_of_birth")) / 365)
    )

    
    # 7. Strict filters (DATA QUALITY)
    
    df = df.filter(col("age") >= 18)
    df = df.filter(col("account_opening_date") < current_date())
    df = df.filter(col("account_balance") >= 0)

    
    # 8. Deduplication (Customer level)
    
    window_cust = Window.partitionBy("customer_id").orderBy(col("account_opening_date").desc())

    df = df.withColumn("rn", row_number().over(window_cust)) \
           .filter(col("rn") == 1) \
           .drop("rn")

    
    # 9. Deduplication (Account level)
    
    window_acc = Window.partitionBy("account_id").orderBy(col("account_opening_date").desc())

    df = df.withColumn("rn", row_number().over(window_acc)) \
           .filter(col("rn") == 1) \
           .drop("rn")
    df=df.drop("ingest_ts","phone")
    return df
