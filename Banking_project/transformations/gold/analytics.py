import dlt
from pyspark.sql.functions import *


def get_fact():
    return dlt.read("bankingdatabase.gold.fact_transactions")


# =========================================
# 1. HIGH RISK NEW BENEFICIARY
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_high_risk_new_beneficiary")
def kpi_high_risk_new_beneficiary():

    df = get_fact()

    return df.filter(
        (col("high_amount_flag") == 1) &
        (col("new_beneficiary_flag") == 1) &
        (col("kyc_status").isin("MINIMAL", "LOW"))
    ).groupBy("customer_id").agg(
        count("*").alias("txn_count"),
        sum("amount").alias("total_amount")
    )


# =========================================
# 2. NIGHT + GEO + HIGH AMOUNT
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_night_geo_high_amount")
def kpi_night_geo_high_amount():

    df = get_fact()

    return df.filter(
        (col("night_indicator") == 1) &
        (col("geo_mismatch_flag") == 1) &
        (col("high_amount_flag") == 1)
    ).groupBy("customer_id").agg(
        count("*").alias("txn_count"),
        sum("amount").alias("total_amount")
    )


# =========================================
# 3. TRANSACTION VELOCITY
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_transaction_velocity")
def kpi_transaction_velocity():

    df = get_fact()

    return df.filter(
        col("txn_gap_minutes") < 5
    ).groupBy("customer_id").agg(
        count("*").alias("rapid_txns"),
        avg("txn_gap_minutes").alias("avg_gap")
    )


# =========================================
# 4. MULTIPLE BENEFICIARIES
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_multiple_beneficiaries")
def kpi_multiple_beneficiaries():

    df = get_fact()

    return df.filter(
        (col("daily_unique_beneficiaries") > 5) &
        (col("daily_total_amount") > 200000)
    ).groupBy("customer_id", "txn_date").agg(
        max("daily_unique_beneficiaries").alias("beneficiaries"),
        max("daily_total_amount").alias("total_amount")
    )


# =========================================
# 5. IP LOCATION ANOMALY
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_ip_location_anomaly")
def kpi_ip_location_anomaly():

    df = get_fact()

    return df.filter(
        (
            (col("location_city") != col("previous_ip_city")) |
            (col("location_country") != col("previous_ip_country"))
        )
    ).groupBy("customer_id").agg(
        count("*").alias("ip_anomaly_count")
    )


# =========================================
# 6. SPENDING SPIKE
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_spending_spike")
def kpi_spending_spike():

    df = get_fact()

    return df.filter(
        (col("rolling_7day_amount") > 500000) &
        (col("account_balance") < 50000)
    ).groupBy("customer_id").agg(
        max("rolling_7day_amount").alias("weekly_spend"),
        avg("account_balance").alias("avg_balance")
    )


# =========================================
# 7. FAILED HIGH RISK TRANSACTIONS
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_failed_high_risk")
def kpi_failed_high_risk():

    df = get_fact()

    return df.filter(
        (col("status") == "FAILED") &
        (col("high_amount_flag") == 1) &
        (col("night_indicator") == 1)
    ).groupBy("customer_id").agg(
        count("*").alias("failed_txns"),
        sum("amount").alias("attempted_amount")
    )