###   weekly script , you will find the Enhanced_RFM below ###############

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
 
# Initialize Spark session
spark = SparkSession.builder.appName("WeekDelayAnalysis").getOrCreate()
 
# -----------------------------
# 1. Load and Preprocess Data
# -----------------------------
def load_transactions(table_name: str):
    """
    Load transaction data, remove duplicates, filter relevant transactions,
    and extract date-
    attributes.
    """
    transactions = (
    spark.table(table_name)
    .dropDuplicates(["microfilm_ref_number"])
    .withColumn("transaction_date", F.to_date(F.col("DATE_CREATE"), "yyyy-MM-dd"))
    .filter(F.trim("TRANSACTION_WORDING").isin("Purchase", "Withdrawal", "Cash Advance", "Cash Adv"))
        )
    
    #filter on date
    transactions = transactions.filter(F.col("transaction_date") >= "2018-01-01")
    
    # Correct Year and Week Calculation
    transactions = transactions.withColumn("year", F.year("transaction_date"))
    # Correct Year and Week Calculation
    transactions = transactions.withColumn("week_of_year", F.weekofyear("transaction_date")) \
        .withColumn("iso_year",
        F.when((F.col("week_of_year") == 1) & (F.month("transaction_date") == 12), F.col("year") + 1)  # December belongs to next year
        .when((F.col("week_of_year") >= 52) & (F.month("transaction_date") == 1), F.col("year") - 1)  # January belongs to previous year
        .otherwise(F.year("transaction_date"))  # Otherwise, use the same year
        ).drop("year")
    
    # Rename `iso_year` to `year` for consistency
    transactions = transactions.withColumnRenamed("iso_year", "year")
    
 
    return transactions



# -------------------------------
# 1.1 Get the Shadow account
# -------------------------------
def get_shadow_account(transactions):
    df_link = spark.table("staging_zone.hpsv3_accounts_link").select(F.col("ENTITY_ID").alias("CARD_NUMBER"), F.col("ACCOUNT_NUMBER").alias("SHADOW_ACCOUNT_NBR")).distinct()
    transactions = transactions.join(df_link,on = 'CARD_NUMBER', how = 'left')
    return transactions


# -----------------------------
# 2. Aggregate Weekly Data
# -----------------------------
def aggregate_weekly_data(transactions):
    """ Aggregate transaction data at a weekly level per client. """
    return (
        transactions.groupBy("SHADOW_ACCOUNT_NBR", "year", "week_of_year")
        .agg(
            F.count("MICROFILM_REF_NUMBER").alias("trx_count"),
            F.sum("LOCAL_AMOUNT").alias("total_amount"),
            F.min("transaction_date").alias("min_trx_date"),
            F.max("transaction_date").alias("max_trx_date"),
        )
    )
 
# -----------------------------
# 3. Compute Delay in Days on Transacted Weeks Only
# -----------------------------
def compute_delay_days(data):
    """ Compute delay in days based on previous transaction max date. """
    window_spec = Window.partitionBy("SHADOW_ACCOUNT_NBR").orderBy("year", "week_of_year")
 
    data = data.withColumn("prev_max_trx_date", F.lag("max_trx_date").over(window_spec))
    
    return data.withColumn(
        "delay_days",
        F.datediff(F.col("min_trx_date"), F.col("prev_max_trx_date"))
    )
    
# ----------------------------- #
# 7. Identify the first transaction for each customer.
# ----------------------------- #
def identify_count_transaction(data):
    """
    Identify the first transaction for each customer.
    """
    windowSpec = Window.partitionBy("SHADOW_ACCOUNT_NBR").orderBy("year", "week_of_year")
    
    data = data.withColumn(
                    "first_non_null_trx_date",
                    F.first(F.when(F.col("trx_count") > 0, F.col("min_trx_date")), ignorenulls=True).over(windowSpec)
                            )
    data = data.withColumn("Time_since_first_transaction", F.datediff("min_trx_date","first_non_null_trx_date"))
    
    data = data.withColumn(
                "Count_Transactions_back", F.row_number().over(windowSpec))
    


    return data
# -----------------------------
# 4. Compute Rolling 24-Week Stats (Only for Transacted Weeks)
# -----------------------------
def compute_rolling_stats(data):
    """ Compute rolling 24-week average and standard deviation of delay days. """
    rolling_window = (
        Window.partitionBy("SHADOW_ACCOUNT_NBR")
        .orderBy("year", "week_of_year")
        .rowsBetween(-23, -0)
    )
 
    # Compute average delay gaps  and standard deviation where trx_count > 0
    data = data.withColumn(
        "avg_delay_gaps",
        F.when((F.col("Count_Transactions_back") > 2),\
               F.round(F.avg("delay_days").over(rolling_window), 2))
    ).withColumn(
        "stddev_gaps",
        F.when((F.col("Count_Transactions_back") > 2),\
               F.round(F.stddev("delay_days").over(rolling_window), 2))
    )
 
    return data
 
# -----------------------------
# 5. Compute Deviation from Average delay gaps 
# -----------------------------
'''def compute_delay_deviation(data):
    """ Compute deviation from avg delay gaps. """
    return data.withColumn(
        "delay_deviation",
        F.when(F.col("avg_delay_gaps").isNotNull(),
               F.round((F.col("delay_days") - F.col("avg_delay_gaps")) / F.col("stddev_gaps"), 4))
        .otherwise(None)
    )
 '''
# -----------------------------
# 6. Generate Full Week Series
# -----------------------------
def generate_full_weeks(start_year=2018, end_year=2025, end_month=1):
    """ Generate a full series of weeks per year for complete data alignment. """
    weeks_list = [
        (year, week)
        for year in range(start_year, end_year + 1)
        for week in range(1, (datetime(year, end_month, 31).isocalendar()[1] if year == end_year else 52) + 1)
    ]
    return spark.createDataFrame(weeks_list, ["year", "week_of_year"])
 
# -----------------------------
# 7. Fill Missing Weeks and Forward Fill Calculated Values
# -----------------------------
def fill_missing_weeks(weekly_agg, full_weeks):
    """ Ensure all client-week pairs exist by cross-joining client codes with full weeks, filling forward missing values. """
    client_codes = weekly_agg.select("SHADOW_ACCOUNT_NBR").distinct()
    full_series = client_codes.crossJoin(full_weeks)
 
    # Join with existing weekly aggregates
    result = full_series.join(weekly_agg, ["SHADOW_ACCOUNT_NBR", "year", "week_of_year"], "left")
 
    # Forward fill missing calculations
    window_spec = Window.partitionBy("SHADOW_ACCOUNT_NBR").orderBy("year", "week_of_year")
 
    result = result.withColumn("trx_count", F.coalesce(F.col("trx_count"), F.lit(0))) \
        .withColumn("Time_since_first_transaction", F.last("Time_since_first_transaction", ignorenulls=True).over(window_spec)) \
        .withColumn("Count_Transactions_back", F.last("Count_Transactions_back", ignorenulls=True).over(window_spec)) \
        .withColumn("avg_delay_gaps", F.last("avg_delay_gaps", ignorenulls=True).over(window_spec)) \
        .withColumn("stddev_gaps", F.last("stddev_gaps", ignorenulls=True).over(window_spec)) \
       
 
    return result
 
# -----------------------------
# 8. Compute Week Start & End Dates
# -----------------------------
def compute_week_dates(data):
    """ Compute the start and end date (Sunday) of each transaction week. """
    data = data.withColumn(
        "start_week_date",
        F.expr(
            """date_add(to_date(concat(year, '-01-01')), 
            CAST(((week_of_year) * 7 - pmod(dayofweek(to_date(concat(year, '-01-01')))-2, 7)) AS INT))"""
        ),
    )
    return data.withColumn("max_week_date", F.date_add(F.col("start_week_date"), 6))
 
# -----------------------------
# 9. Write Final Data
# -----------------------------
def organize_write(data):
    """ Save final dataset to the table. """
    data = data.filter(F.col('start_week_date') >= "2022-06-01")
    
    columns = [
        'SHADOW_ACCOUNT_NBR', 'year', 'week_of_year', 'max_week_date', 
        # 'trx_count', "total_amount",
        'min_trx_date', 'max_trx_date', 'delay_days',
        'Time_since_first_transaction',
        'Count_Transactions_back', 
        'avg_delay_gaps', 
        'stddev_gaps']
    
  #  ''', 'delay_deviation'''
    
    data.select(columns).write.mode('overwrite').saveAsTable("analytics_zone_retail_cross_selling.hps_shadow_cc_weekly_transactions_delay")
    print("___ Table of Delay Days Agg Written Successfully")
    
# -----------------------------
# 10. Main Execution Pipeline
# -----------------------------
def main():
    # Load data
    transactions = load_transactions("staging_zone.hpsv3_transaction_hist")
    #get shadow account
    transactions = get_shadow_account(transactions)
    
    # Aggregate data weekly (only transacted weeks)
    weekly_agg = aggregate_weekly_data(transactions)  
 
    # Compute delay days
    weekly_agg = compute_delay_days(weekly_agg)
    
    # Identify first transaction
    weekly_agg = identify_count_transaction(weekly_agg)
 
    # Compute rolling statistics
    weekly_agg = compute_rolling_stats(weekly_agg)
 
    # Compute deviation from rolling average
    # weekly_agg = compute_delay_deviation(weekly_agg)
 
    # Generate full weeks
    full_weeks = generate_full_weeks()
 
    # Fill missing weeks and forward fill values
    final_result = fill_missing_weeks(weekly_agg, full_weeks)
 
    # Compute week dates
    final_result = compute_week_dates(final_result)
 
    # Write final result
    organize_write(final_result)

    
# Run the script
if __name__ == "__main__":
    main()