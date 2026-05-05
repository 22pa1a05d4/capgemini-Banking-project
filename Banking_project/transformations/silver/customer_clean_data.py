# from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import dlt

@dlt.table(name="bankingdatabase.silver.customer_profile_quarantine_st")
def customer_profile_quarantine_st():

    df = dlt.read_stream("customer_profile_bronze")

    invalid_df = df.filter(
        col("customer_id").isNull() |
        col("account_id").isNull() |
        col("email").isNull() |
        col("account_balance").isNull()
    )

    return invalid_df.withColumn("error_reason", lit("Invalid mandatory fields"))


@dlt.table(name="customer_profile_silver")
def customer_profile_silver():

    df = dlt.read("customer_profile_bronze")
    df = df.filter(
        col("customer_id").isNotNull()
        & col("account_id").isNotNull()
        & col("email").isNotNull()
    )

    df = df.withColumn(
        "email", lower(regexp_replace(col("email"), "@.*", "@gmail.com"))
    )

    df = df.withColumn("account_balance", col("account_balance").cast("double"))

    # Remove invalid balances
    df = df.filter(col("account_balance").isNotNull())

    df = df.withColumn(
        "date_of_birth", to_date(col("date_of_birth"), "yyyy-MM-dd")
    ).withColumn(
        "account_opening_date", to_date(col("account_opening_date"), "yyyy-MM-dd")
    )

    # Remove invalid dates
    df = df.filter(
        col("date_of_birth").isNotNull() & col("account_opening_date").isNotNull()
    )

    # 5. Standardization

    df = df.withColumn("kyc_status", upper(col("kyc_status")))
    df = df.withColumn("account_type", upper(col("account_type")))
    df=df.withColumn("ingest_timestamp",current_timestamp())
    df = df.withColumn("home_country", initcap(col("home_country"))).withColumn(
        "home_city", initcap(col("home_city"))
    )

    # 6. Age calculation

    df = df.withColumn(
        "age", floor(datediff(current_date(), col("date_of_birth")) / 365)
    )

    df = df.filter(col("age") >= 18)
    df = df.filter(col("account_opening_date") < current_date())

    window_acc = Window.partitionBy("account_id").orderBy(
        col("account_opening_date").desc()
    )

    df = (
        df.withColumn("rn", row_number().over(window_acc))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    df = df.drop("phone")
    return df
