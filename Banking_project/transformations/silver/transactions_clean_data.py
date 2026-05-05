from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
import dlt

@dlt.table(name="bankingdatabase.silver.transactions_quarantine")
def transactions_quarantine():

    df = dlt.read("transactions_bronze_1")

    invalid_df = df.filter(
        col("txn_id").isNull() |
        col("account_id").isNull() |
        col("customer_id").isNull() |
        (col("amount") <= 0)
    )

    return invalid_df.withColumn("error_reason", lit("Invalid transaction data"))

@dlt.table(name="transactions_silver")
def transactions_silver():

    df = dlt.read("transactions_bronze_1")
    # 1. Remove null
    df = df.filter(
        col("txn_id").isNotNull() &
        col("account_id").isNotNull() &
        col("customer_id").isNotNull()
    )
    # 2. Fill null values
    df = df.fillna({
        "location_country": "UNKNOWN",
        "location_city": "UNKNOWN",
        "merchant_category": "UNKNOWN"
    })
    # 3. Convert amount → DOUBLE and filter > 0
    df = df.withColumn("amount", col("amount").cast(DecimalType(18,2)))
    df = df.filter(col("amount").isNotNull())
    df = df.filter(col("amount") > 0)
    df = df.withColumn(
        "normalised_amount",
    when(col("currency") == "USD", col("amount") * 94)  
    .when(col("currency") == "EUR", col("amount") * 111)  
    .when(col("currency") == "INR", col("amount"))  
    .otherwise(None)
)
    # 4. Convert timestamps 
    df = df.withColumn(
        "txn_timestamp",
        to_timestamp(col("txn_timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSS")
    )

    df=df.withWatermark("txn_timestamp","15 minutes")
    # Remove invalid timestamps
    df = df.filter(col("txn_timestamp").isNotNull())
    df=df.withColumn("ingest_timestamp",current_timestamp())
    df = df.withColumn("txn_type", upper(col("txn_type")))
    df = df.withColumn("channel", upper(col("channel")))
    df = df.withColumn("status", upper(col("status")))

    
    # 6. Drop unwanted columns
    
    df = df.drop("location_lat", "location_lon")

    
    window_txn = Window.partitionBy("txn_id").orderBy(col("txn_timestamp").desc())

    df = df.withColumn("rn", row_number().over(window_txn)) \
           .filter(col("rn") == 1) \
           .drop("rn")

    
    # 8. Account consistency flag
    
    df = df.withColumn(
        "account_match_flag",
        when(col("txn_type").isin("DEPOSIT","CREDIT"),"Not_Applicable")
        .when(col("account_id") == col("from_account"), "SAME_ACCOUNT")
        .otherwise("SUSPICIOUS")
    )

    # 9. Transaction validation flag 
    
    df = df.withColumn(
        "txn_validation_flag",
        when(
            (col("txn_type") == "WITHDRAWAL") &
            col("from_account").isNotNull() &
            col("to_account").isNull(),
            "VALID"
        ).when(
            (col("txn_type") == "TRANSFER") &
            col("from_account").isNotNull() &
            col("to_account").isNotNull(),
            "VALID"
        ).when(
            (col("txn_type") == "DEBIT") &
            col("from_account").isNotNull() &
            col("merchant_id").isNotNull() &
            col("to_account").isNull(),
            "VALID"
        ).when(
            (col("txn_type") == "CREDIT") &
            col("to_account").isNotNull() &
            col("from_account").isNull(),
            "VALID"
        ).when(
            (col("txn_type") == "DEPOSIT") &
            col("to_account").isNotNull() &
            col("from_account").isNull(),
            "VALID"
        ).otherwise("INVALID")
    )

    return df
