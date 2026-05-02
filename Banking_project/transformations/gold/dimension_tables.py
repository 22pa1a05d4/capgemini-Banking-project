from pyspark import pipelines as dp
from pyspark.sql.functions import *


# =========================================
# DIM_CUSTOMERS
# =========================================
@dp.materialized_view(name="bankingdatabase.gold.dim_customers")
def dim_customers():

    df = spark.read.table("customer_profile_silver")

    return df.select(

        # Customer keys
        "customer_id",
        "account_id",

        # Customer details
        "customer_name",
        "age",

        # Account details
        "account_type",
        "account_balance",
        "kyc_status",
        "is_active",

        # Home location
        "home_country",
        "home_city",

        # Account dates
        "account_opening_date"
    )


# =========================================
# DIM_DEVICES
# =========================================
@dp.materialized_view(name="bankingdatabase.gold.dim_devices")
def dim_devices():

    df = spark.read.table("device_sessions_silver")

    return df.select(

        # Session/device keys
        "session_id",
        "customer_id",
        "device_id",

        # Device details
        "device_type",
        "ip_address",

        # Location details
        "location_country",
        "location_city",

        # Risk indicators
        "is_new_device",
        "is_new_location",

        # Session details
        "login_timestamp",
        "logout_timestamp",
        "session_duration_seconds"
    ).dropDuplicates()


# =========================================
# DIM_MERCHANTS
# =========================================
@dp.materialized_view(name="bankingdatabase.gold.dim_merchants")
def dim_merchants():

    df = spark.read.table("transactions_silver")

    return df.select(

        # Merchant details
        "merchant_id",
        "merchant_category"

    ).filter(
        col("merchant_id").isNotNull()
    ).dropDuplicates()
