import dlt
from pyspark.sql.functions import col

# -------------------------
# CUSTOMER CDC
# -------------------------
dlt.create_target_table("cdc_customer_profile_hist")

@dlt.view
def cdc_customer_profile_source_view():
    return spark.readStream.option("skipChangeCommits", "true").table("bankingdatabase.silver.customer_profile_silver")

dlt.apply_changes(
    target="cdc_customer_profile_hist",
    source="cdc_customer_profile_source_view",
    keys=["customer_id", "account_id"],
    sequence_by=col("ingest_timestamp"),
    stored_as_scd_type=2
)

# -------------------------
# DEVICE CDC
# -------------------------
dlt.create_target_table("cdc_device_sessions_hist")

@dlt.view
def cdc_device_sessions_source_view():
    return spark.readStream.option("skipChangeCommits", "true").table("bankingdatabase.silver.device_sessions_silver")

dlt.apply_changes(
    target="cdc_device_sessions_hist",
    source="cdc_device_sessions_source_view",
    keys=["session_id"],
    sequence_by=col("ingest_timestamp"),
    stored_as_scd_type=2
)

# -------------------------
# TRANSACTIONS CDC
# -------------------------
dlt.create_target_table("cdc_transactions_hist")

@dlt.view
def cdc_transactions_source_view():
    return spark.readStream.option("skipChangeCommits", "true").table("bankingdatabase.silver.transactions_silver")

dlt.apply_changes(
    target="cdc_transactions_hist",
    source="cdc_transactions_source_view",
    keys=["txn_id"],
    sequence_by=col("ingest_timestamp"),
    stored_as_scd_type=2
)