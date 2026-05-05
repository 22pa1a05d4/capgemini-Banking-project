import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# COMMON FACT FUNCTION

def get_fact():
    return spark.read.table("bankingdatabase.gold.fact_transactions")



# 1. NEW BENEFICIARY + HIGH AMOUNT

@dlt.table(name="bankingdatabase.gold.kpi_new_beneficiary_high_amt_detail")
def kpi_new_beneficiary_high_amt_detail():

    df = get_fact()

    return df.filter(
        (col("new_beneficiary_flag") == 1) &
        (col("high_amount_flag") == 1)
    ).select(
        "txn_id",
        "customer_id",
        "account_id",
        "txn_date",
        "amount",
        "kyc_status",
        "channel"
    )

# 2. RAPID FUND DISTRIBUTION

@dlt.table(name="bankingdatabase.gold.kpi_rapid_distribution")
def kpi_rapid_distribution():

    df = get_fact()

    incoming_df = df.filter(
        col("txn_type").isin("CREDIT", "DEPOSIT")
    ).select(
        "customer_id",
        col("txn_timestamp").alias("credit_time")
    )

    outgoing_df = df.filter(
        col("txn_type") == "TRANSFER"
    ).select(
        "customer_id",
        "txn_timestamp",
        "to_account",
        "amount"
    )

    joined_df = incoming_df.join(outgoing_df, "customer_id").filter(
        (unix_timestamp("txn_timestamp") - unix_timestamp("credit_time") <= 1800) &
        (unix_timestamp("txn_timestamp") - unix_timestamp("credit_time") >= 0)
    )

    rapid_df = joined_df.groupBy("customer_id", "credit_time").agg(
        countDistinct("to_account").alias("unique_beneficiaries"),
        sum("amount").alias("total_outgoing_amount")
    )

    return rapid_df.filter(col("unique_beneficiaries") >= 3) \
        .groupBy("customer_id").agg(
            count("*").alias("event_count"),
            sum("total_outgoing_amount").alias("total_amount")
        )



# 3. SAVINGS ABNORMAL

@dlt.table(name="bankingdatabase.gold.kpi_savings_abnormal")
def kpi_savings_abnormal():
    df = get_fact()

    return df.filter(
        (col("account_type") == "SAVINGS") &
        (
            ((col("daily_total_amount") > 200000) &
             (~col("channel").isin("BRANCH", "ONLINE", "POS"))) |
            (col("daily_txn_count") >= 10) |
            (col("rolling_7day_amount") >= 1000000)
        )
    ).groupBy("customer_id","txn_date").agg(
        count("*").alias("event_count"),
        sum("daily_total_amount").alias("total_amount")
    )



# 4. CURRENT OVERACTIVITY

@dlt.table(name="bankingdatabase.gold.kpi_current_overactivity_detail")
def kpi_current_overactivity_detail():

    df = get_fact()

    df = df.withColumn(
        "channel_limit_flag",
        when((col("channel") == "ATM") & (col("daily_total_amount") > 200000), 1)
        .when((col("channel") == "ONLINE") & (col("daily_total_amount") > 2000000), 1)
        .when((col("channel") == "POS") & (col("daily_total_amount") > 500000), 1)
        .when((col("channel") == "MOBILE") & (col("daily_total_amount") > 300000), 1)
        .otherwise(0)
    )

    return df.filter(
        (col("account_type") == "CURRENT") &
        (col("channel_limit_flag") == 1)
    ).select(
        "customer_id",
        "account_id",
        "txn_id",
        "txn_date",
        "channel",
        "daily_txn_count",
        "daily_total_amount"
    )

@dlt.table(name="bankingdatabase.gold.kpi_transaction_velocity")
def kpi_transaction_velocity():
    df=get_fact()
    velocity_df=df.groupBy(
        window(col("txn_timestamp"),"5 minutes"),
        col("customer_id")
    ).count()
    return velocity_df.filter(col("count")>5)


# 5. NIGHT VELOCITY

@dlt.table(name="bankingdatabase.gold.kpi_night_velocity")
def kpi_night_velocity():

    df = get_fact()
    return df.filter(
        (col("new_beneficiary_flag") == 1) &
        (col("high_amount_flag") == 1)
    ).select(
        "txn_id",
        "customer_id",
        "account_id",
        "txn_date",
        "amount",
        "kyc_status",
        "channel"
    )

# 6. IMPOSSIBLE TRAVEL
@dlt.table(name="bankingdatabase.gold.kpi_impossible_travel_detail")
def kpi_impossible_travel_detail():

    df = get_fact()

    # Window
    ip_window = Window.partitionBy("customer_id", "ip_address").orderBy("txn_timestamp")

    df = df.withColumn("prev_city", lag("location_city").over(ip_window)) \
           .withColumn("prev_country", lag("location_country").over(ip_window)) \
           .withColumn("prev_time", lag("txn_timestamp").over(ip_window))

    # Time difference
    df = df.withColumn(
        "time_diff_minutes",
        (unix_timestamp("txn_timestamp") - unix_timestamp("prev_time")) / 60
    )

    return df.filter(
        (col("ip_address").isNotNull()) &
        (col("time_diff_minutes") <= 10) &
        (col("location_country") != col("prev_country"))
    ).select(
        "txn_id",
        "customer_id",
        "ip_address",
        "txn_timestamp",
        "location_city",
        "location_country",
        "prev_city",
        "prev_country",
        "time_diff_minutes"
    )


# 7. KYC PENDING HIGH AMOUNT

@dlt.table(name="bankingdatabase.gold.kpi_kyc_pending_high_amt_detail")
def kpi_kyc_pending_high_amt_detail():

    df = get_fact()

    return df.filter(
        (col("kyc_status") == "PENDING") &
        (col("daily_total_amount") > 200000) &
        (~col("channel").isin("BRANCH"))
    ).select(
        "customer_id",
        "account_id",
        "txn_id",
        "txn_date",
        "daily_total_amount",
        "kyc_status",
        "channel"
    )


# KPI PERCENTAGES (SCALABLE)

@dlt.table(name="bankingdatabase.gold.kpi_percentages")
def kpi_percentages():

    df = get_fact()
 
    # Total transactions
    total_txns = df.agg(count("*").alias("total_txns"))

    # KPI counts (using aggregated KPI tables)
    k1 = spark.read.table("bankingdatabase.gold.kpi_new_beneficiary_high_amt_detail").agg(count("*").alias("k1"))
    k2 = spark.read.table("bankingdatabase.gold.kpi_rapid_distribution").agg(count("*").alias("k2"))
    k3 = spark.read.table("bankingdatabase.gold.kpi_savings_abnormal").agg(count("*").alias("k3"))
    k4 = spark.read.table("bankingdatabase.gold.kpi_current_overactivity_detail").agg(count("*").alias("k4"))
    k5 = spark.read.table("bankingdatabase.gold.kpi_night_velocity").agg(count("*").alias("k5"))
    k6 = spark.read.table("bankingdatabase.gold.kpi_impossible_travel_detail").agg(count("*").alias("k6"))
    k7 = spark.read.table("bankingdatabase.gold.kpi_kyc_pending_high_amt_detail").agg(count("*").alias("k7"))

    # Combine
    df_all = total_txns \
        .crossJoin(k1).crossJoin(k2).crossJoin(k3).crossJoin(k4) \
        .crossJoin(k5).crossJoin(k6).crossJoin(k7)

    # Percentages
    return df_all.select(
        (col("k1") / col("total_txns")).alias("p1"),
        (col("k2") / col("total_txns")).alias("p2"),
        (col("k3") / col("total_txns")).alias("p3"),
        (col("k4") / col("total_txns")).alias("p4"),
        (col("k5") / col("total_txns")).alias("p5"),
        (col("k6") / col("total_txns")).alias("p6"),
        (col("k7") / col("total_txns")).alias("p7")
    )


# HYBRID RISK SCORE 

@dlt.table(name="bankingdatabase.gold.system_risk_score")
def system_risk_score():

    df = spark.read.table("bankingdatabase.gold.kpi_percentages")

    # Static weights
    df = df.withColumn("w1", lit(0.15)) \
           .withColumn("w2", lit(0.20)) \
           .withColumn("w3", lit(0.10)) \
           .withColumn("w4", lit(0.10)) \
           .withColumn("w5", lit(0.15)) \
           .withColumn("w6", lit(0.15)) \
           .withColumn("w7", lit(0.10)) 

    # Total KPI %
    df = df.withColumn(
        "total_pct",
        col("p1")+col("p2")+col("p3")+col("p4")+
        col("p5")+col("p6")+col("p7")
    )

    # Dynamic weights
    df = df.withColumn("d1", col("p1")/col("total_pct")) \
           .withColumn("d2", col("p2")/col("total_pct")) \
           .withColumn("d3", col("p3")/col("total_pct")) \
           .withColumn("d4", col("p4")/col("total_pct")) \
           .withColumn("d5", col("p5")/col("total_pct")) \
           .withColumn("d6", col("p6")/col("total_pct")) \
           .withColumn("d7", col("p7")/col("total_pct")) 

    # Hybrid weights
    df = df.withColumn("h1", 0.7*col("w1") + 0.3*col("d1")) \
           .withColumn("h2", 0.7*col("w2") + 0.3*col("d2")) \
           .withColumn("h3", 0.7*col("w3") + 0.3*col("d3")) \
           .withColumn("h4", 0.7*col("w4") + 0.3*col("d4")) \
           .withColumn("h5", 0.7*col("w5") + 0.3*col("d5")) \
           .withColumn("h6", 0.7*col("w6") + 0.3*col("d6")) \
           .withColumn("h7", 0.7*col("w7") + 0.3*col("d7")) 

    # Final risk score
    df = df.withColumn(
        "risk_score",
        col("p1")*col("h1") +
        col("p2")*col("h2") +
        col("p3")*col("h3") +
        col("p4")*col("h4") +
        col("p5")*col("h5") +
        col("p6")*col("h6") +
        col("p7")*col("h7") 
    )

    # Classification
    df = df.withColumn(
        "risk_level",
        when(col("risk_score") >= 0.6, "HIGH")
        .when(col("risk_score") >= 0.3, "MEDIUM")
        .otherwise("LOW")
    )

    return df.select("risk_score", "risk_level")



@dlt.table(name="bankingdatabase.gold.rbi_daily_report")
def rbi_daily_report():

    df = get_fact()

    return df.groupBy("txn_date", "channel").agg(
        count("*").alias("txn_count"),
        sum("amount").alias("total_amount")
    )
@dlt.table(name="bankingdatabase.gold.rbi_monthly_report")
def rbi_monthly_report():

    df = get_fact()

    df = df.withColumn("txn_month", date_format(col("txn_date"), "yyyy-MM"))

    return df.groupBy("txn_month", "channel").agg(
        count("*").alias("txn_count"),
        sum("amount").alias("total_amount")
    )
@dlt.table(name="bankingdatabase.gold.rbi_daily_pivot")
def rbi_daily_pivot():

    df = get_fact()

    return df.groupBy("txn_date").pivot("channel").agg(
        sum("amount")
    )