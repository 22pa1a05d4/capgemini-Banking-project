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
        (col("location_city") != col("previous_ip_city")) |
        (col("location_country") != col("previous_ip_country"))
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


# =========================================
# 8. CUSTOMER RISK SUMMARY
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_customer_risk_summary")
def kpi_customer_risk_summary():
    df = get_fact()
    return df.groupBy("customer_id").agg(
        count("*").alias("total_txns"),
        sum("amount").alias("total_amount"),
        sum("high_amount_flag").alias("high_amount_txns"),
        sum("night_indicator").alias("night_txns"),
        sum("geo_mismatch_flag").alias("geo_mismatch_txns"),
        avg("txn_gap_minutes").alias("avg_txn_gap")
    )


# =========================================
# 9. DAILY SUMMARY
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_daily_summary")
def kpi_daily_summary():
    df = get_fact()
    return df.groupBy("txn_date").agg(
        count("*").alias("total_txns"),
        sum("amount").alias("total_amount"),
        countDistinct("customer_id").alias("active_customers"),
        avg("amount").alias("avg_txn_amount")
    )


# =========================================
# 10. CHANNEL SUMMARY
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_channel_summary")
def kpi_channel_summary():
    df = get_fact()
    return df.groupBy("channel").agg(
        count("*").alias("txn_count"),
        sum("amount").alias("total_amount"),
        sum("high_amount_flag").alias("high_risk_txns")
    )


# =========================================
# 11. LOCATION SUMMARY
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_location_summary")
def kpi_location_summary():
    df = get_fact()
    return df.groupBy("location_country", "location_city").agg(
        count("*").alias("txn_count"),
        sum("geo_mismatch_flag").alias("geo_risk_count"),
        sum("amount").alias("total_amount")
    )


# =========================================
# 12. DEVICE SUMMARY
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_device_summary")
def kpi_device_summary():
    df = get_fact()
    return df.groupBy("device_id").agg(
        count("*").alias("txn_count"),
        countDistinct("customer_id").alias("unique_users"),
        sum("amount").alias("total_amount")
    )


# =========================================
# 13. KYC SUMMARY
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_kyc_summary")
def kpi_kyc_summary():
    df = get_fact()
    return df.groupBy("kyc_status").agg(
        count("*").alias("txn_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )


# =========================================
# 14. BENEFICIARY SUMMARY
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_beneficiary_summary")
def kpi_beneficiary_summary():
    df = get_fact()
    return df.groupBy("customer_id").agg(
        countDistinct("to_account").alias("unique_beneficiaries"),
        max("daily_unique_beneficiaries").alias("max_daily_beneficiaries")
    )
# =========================================
# 15. RBI DAILY ACCOUNT TYPE SUMMARY
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_rbi_daily_account_summary")
def kpi_rbi_daily_account_summary():
    df = get_fact()

    return df.groupBy("txn_date", "account_type").agg(
        count("*").alias("txn_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_txn_amount"),
        countDistinct("customer_id").alias("active_customers")
    )


# =========================================
# 16. RBI MONTHLY ACCOUNT TYPE SUMMARY
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_rbi_monthly_account_summary")
def kpi_rbi_monthly_account_summary():
    df = get_fact()

    df = df.withColumn("txn_month", date_format(col("txn_date"), "yyyy-MM"))

    return df.groupBy("txn_month", "account_type").agg(
        count("*").alias("txn_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_txn_amount"),
        countDistinct("customer_id").alias("active_customers")
    )


# =========================================
# 17. RBI SAVINGS vs CURRENT COMPARISON
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_rbi_savings_current_comparison")
def kpi_rbi_savings_current_comparison():
    df = get_fact()

    return df.groupBy("account_type").agg(
        count("*").alias("total_txns"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_txn_amount"),
        max("amount").alias("max_txn_amount")
    )


# =========================================
# 18. RBI MONTHLY TREND (ALL ACCOUNTS)
# =========================================
@dlt.table(name="bankingdatabase.gold.kpi_rbi_monthly_trend")
def kpi_rbi_monthly_trend():
    df = get_fact()

    df = df.withColumn("txn_month", date_format(col("txn_date"), "yyyy-MM"))

    return df.groupBy("txn_month").agg(
        count("*").alias("total_txns"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_txn_amount"),
        countDistinct("customer_id").alias("active_customers")
    )