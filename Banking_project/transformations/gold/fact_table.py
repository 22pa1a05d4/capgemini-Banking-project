from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window
@dp.materialized_view(name="bankingdatabase.gold.fact_transactions",
                      partition_cols=["txn_date"]
                      )
def fact_transactions():
    # Read silver tables
    txn = spark.read.option("skipChangeCommits", "true").table("cdc_transactions_hist")\
        .filter(col("__END_AT").isNull())
    cust = spark.read.table("cdc_customer_profile_hist") \
            .filter(col("__END_AT").isNull())
    # Select only KPI-required customer columns
    cust_df = cust.select(
        "customer_id",
        "account_id",
        "account_type",
        "account_balance",
        "kyc_status",
        "home_country",
        "home_city"
    )
    # Join customer data
    df = txn.join(
        cust_df,
        ["customer_id", "account_id"],
        "left"
    )
    # Handle NULL transaction type
    df = df.withColumn(
        "txn_type",
        when(col("txn_type").isNull(), "UNKNOWN")
        .otherwise(upper(col("txn_type")))
    )
    # Handle NULL channel
    df = df.withColumn(
        "channel",
        when(col("channel").isNull(), "UNKNOWN")
        .otherwise(upper(col("channel")))
    )
    # Handle NULL status
    df = df.withColumn(
        "status",
        when(col("status").isNull(), "UNKNOWN")
        .otherwise(upper(col("status")))
    )
    # Handle NULL KYC
    df = df.withColumn(
        "kyc_status",
        when(col("kyc_status").isNull(), "MINIMAL")
        .otherwise(upper(col("kyc_status")))
    )
    # Handle NULL account type
    df = df.withColumn(
        "account_type",
        when(col("account_type").isNull(), "UNKNOWN")
        .otherwise(upper(col("account_type")))
    )
    # Handle NULL from_account
    df = df.withColumn(
        "from_account",
        when(
            col("txn_type").isin("CREDIT", "DEPOSIT"),
            "EXTERNAL_SOURCE"
        ).otherwise(col("from_account"))
    )
    # Handle NULL to_account
    df = df.withColumn(
        "to_account",
        when(
            col("txn_type").isin("WITHDRAWAL", "DEBIT"),
            "CASH_OR_MERCHANT"
        ).otherwise(col("to_account"))
    )
    # Final NULL handling
    df = df.fillna({
        "from_account": "UNKNOWN",
        "to_account": "UNKNOWN",
        "location_country": "UNKNOWN",
        "location_city": "UNKNOWN",
        "home_country": "UNKNOWN",
        "home_city": "UNKNOWN",
        "ip_address": "UNKNOWN_IP"
    })
    # Create transaction date
    df = df.withColumn(
        "txn_date",
        to_date(col("txn_timestamp"))
    )
    # Create transaction hour
    df = df.withColumn(
        "txn_hour",
        hour(col("txn_timestamp"))
    )
    # Create night transaction indicator
    df = df.withColumn(
        "night_indicator",
        when(col("txn_hour").between(0,4), 1)
        .otherwise(0)
    )
    # Create high amount flag
    df = df.withColumn(
        "high_amount_flag",
        when(col("amount") >= 400000, 1)
        .otherwise(0)
    )
    # Create geo mismatch flag
    df = df.withColumn(
        "geo_mismatch_flag",
        when(
            (col("home_country") != col("location_country")),
            1
        ).otherwise(0)
    )
    # Customer transaction velocity window
    velocity_window = Window.partitionBy(
        "customer_id"
    ).orderBy(
        col("txn_timestamp")
    )
    # Get previous transaction timestamp
    df = df.withColumn(
        "previous_txn_timestamp",
        lag("txn_timestamp").over(velocity_window)
    )
    # Calculate transaction gap minutes
    df = df.withColumn(
        "txn_gap_minutes",
        (
            unix_timestamp(col("txn_timestamp")) -
            unix_timestamp(col("previous_txn_timestamp"))
        ) / 60
    )
    # Handle first transaction
    df = df.withColumn(
        "txn_gap_minutes",
        when(
            col("txn_gap_minutes").isNull(),
            99999
        ).otherwise(col("txn_gap_minutes"))
    )
    # Beneficiary analysis window
    beneficiary_window = Window.partitionBy(
        "customer_id",
        "to_account"
    ).orderBy(
        col("txn_timestamp")
    )
    # Previous beneficiary transaction time
    df = df.withColumn(
        "previous_beneficiary_time",
        lag("txn_timestamp").over(beneficiary_window)
    )
    # New beneficiary flag
    df = df.withColumn(
        "new_beneficiary_flag",
        when(
            (col("txn_type") == "TRANSFER") &
            (
                col("previous_beneficiary_time").isNull() |
                (
                    datediff(
                        to_date(col("txn_timestamp")),
                        to_date(col("previous_beneficiary_time"))
                    ) > 90
                )
            ),
            1
        ).otherwise(0)
    )
    # Daily transaction window
    daily_window = Window.partitionBy(
        "customer_id",
        "txn_date"
    )
    # Daily transaction count
    df = df.withColumn(
        "daily_txn_count",
        count("txn_id").over(daily_window)
    )
    # Daily transaction amount
    df = df.withColumn(
        "daily_total_amount",
        sum("amount").over(daily_window)
    )
    # Daily unique beneficiaries
    beneficiary_df = df.groupBy(
        "customer_id",
        "txn_date"
    ).agg(
        countDistinct("to_account").alias("daily_unique_beneficiaries")
    )
    # Join beneficiary metrics
    df = df.join(
        beneficiary_df,
        ["customer_id", "txn_date"],
        "left"
    )
    # Rolling 7-day transaction window
    rolling_window = Window.partitionBy(
        "customer_id"
    ).orderBy(
        unix_timestamp(col("txn_timestamp"))
    ).rangeBetween(-604800, 0)
    # Rolling 7-day amount
    df = df.withColumn(
        "rolling_7day_amount",
        sum("amount").over(rolling_window)
    )
    # IP analysis window
    ip_window = Window.partitionBy(
        "customer_id",
        "ip_address"
    ).orderBy(
        col("txn_timestamp")
    )
    # Previous IP city
    df = df.withColumn(
        "previous_ip_city",
        lag("location_city").over(ip_window)
    )
    # Previous IP country
    df = df.withColumn(
        "previous_ip_country",
        lag("location_country").over(ip_window)
    )
    # Previous IP transaction timestamp
    df = df.withColumn(
        "previous_ip_txn_time",
        lag("txn_timestamp").over(ip_window)
    )
    # Handle NULL previous IP location
    df = df.withColumn(
        "previous_ip_city",
        when(col("previous_ip_city").isNull(), col("location_city"))
        .otherwise(col("previous_ip_city"))
    )
    df = df.withColumn(
        "previous_ip_country",
        when(col("previous_ip_country").isNull(), col("location_country"))
        .otherwise(col("previous_ip_country"))
    )
    # Final KPI-ready fact table
    return df.select(

        # Core transaction keys
        "txn_id",
        "customer_id",
        "account_id",
        "device_id",

        # Transaction details
        "txn_timestamp",
        "txn_date",
        "txn_hour",
        "txn_type",
        "amount",
        "channel",
        "status",
        "from_account",
        "to_account",

        # Customer details
        "account_type",
        "account_balance",
        "kyc_status",

        # IP and location details
        "ip_address",
        "home_country",
        "home_city",
        "location_country",
        "location_city",

        # KPI analytical columns
        "high_amount_flag",
        "night_indicator",
        "geo_mismatch_flag",
        "txn_gap_minutes",
        "new_beneficiary_flag",
        "daily_txn_count",
        "daily_total_amount",
        "daily_unique_beneficiaries",
        "rolling_7day_amount",
        "previous_ip_city",
        "previous_ip_country"
    )
