from __future__ import print_function
import warnings
warnings.filterwarnings('ignore')

from pyspark.sql.functions import col
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext ,Row, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, monotonically_increasing_id, current_date, date_format
from pyspark.sql.types import StructType, StructField, FloatType, StringType, ArrayType, IntegerType
from pyspark.sql import Row
from datetime import date
from dateutil.relativedelta import relativedelta
import datetime as dt
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import substring
from pyspark.sql.functions import countDistinct, collect_set

import re
from __future__ import print_function
import os
import sys
import numpy as np
import pyspark.pandas as ps
import matplotlib.pyplot as plt

#Starting the session
spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .config("spark.sql.parquet.datetimeRebaseModeInWrite","LEGACY")\
    .config("spark.sql.legacy.timeParserPolicy","LEGACY")\
    .config("spark.sql.execution.arrow.enabled", "true")\
    .getOrCreate()


def load_transaction_data():
    """
    Loads the transaction history table and filters out fee transactions.
    Returns a DataFrame with unique transactions.
    """
    df = spark.table("staging_zone.hpsv3_transaction_hist")
 
    transactions = (
        df.dropDuplicates(["microfilm_ref_number"])
        .withColumn("transaction_date", F.to_date(F.col("DATE_CREATE"), "yyyy-MM-dd"))
        .filter(~F.trim("TRANSACTION_WORDING").like("%FEES"))
    )
    transactions = transactions.filter(F.col("TRANSACTION_DATE") >= "2022-06-30")
 
    return transactions

def get_shadow_account(transactions):
    df_link = spark.table("staging_zone.hpsv3_accounts_link").select(F.col("ENTITY_ID").alias("CARD_NUMBER"), F.col('OPENING_DATE'), F.col("ACCOUNT_NUMBER").alias("SHADOW_ACCOUNT_NBR")).distinct()
    transactions = transactions.join(df_link,on = 'CARD_NUMBER', how = 'left')
    transactions = transactions.withColumn("Year", F.year(transactions["transaction_date"])).withColumn(
            "Month", F.month(transactions["transaction_date"])
        )
    df = transactions.select('SHADOW_ACCOUNT_NBR', 'MICROFILM_REF_NUMBER','Year', 'Month', 'TRANSACTION_DATE', 'TRANSACTION_SIGN', 'LOCAL_AMOUNT','EXPIRY_DATE', 'TRANSACTION_WORDING')
    return transactions


    # Create two columns purchase and deposit that takes 1 and 0 based on the transaction sign value.
    #The same is done for the amount
def debit_deposit(df)
    df_ = (df.withColumn('frequency_purchase', F.when(col('TRANSACTION_SIGN') == 'D', 1).otherwise(0))
             .withColumn('frequency_deposit', F.when(col('TRANSACTION_SIGN') == 'C', 1).otherwise(0))
             .withColumn('amount_purchase', F.when(col('TRANSACTION_SIGN') == 'D', col('LOCAL_AMOUNT')).otherwise(0))
             .withColumn('amount_deposit', F.when(col('TRANSACTION_SIGN') == 'C', col('LOCAL_AMOUNT')).otherwise(0)))
    return df_

year_list = [2022, 2023, 2024]


def aggregate_transactions(df_):
    """
    Aggregates transactions to compute total amount and count of transactions per customer, year, and month.
    Returns a summarized DataFrame.
    """
    # transactions = transactions.withColumn("Year", F.year(transactions["transaction_date"])).withColumn(
    #     "Month", F.month(transactions["transaction_date"])
    # )
    sub_grouped = df_.groupBy('SHADOW_ACCOUNT_NBR','Year','Month').agg(F.last('TRANSACTION_DATE'),F.sum('frequency_purchase').alias('purchases_trx_count'),
                                                                     F.sum('frequency_deposit').alias('deposits_trx_count'),F.sum('LOCAL_AMOUNT').alias('local_amount'),
                                                                     F.sum('amount_purchase').alias('total_amount_purchase'),F.sum('amount_deposit').alias('total_amount_deposit'))
    sub_grouped = sub_grouped.withColumn('trx_count', F.col('purchases_trx_count')+F.col('deposits_trx_count'))
 
    return sub_grouped
 


## General frequency rolling averge, standard deviation and deviation
def compute_rolling_stats(df):
    """ Compute rolling 24-week average and standard deviation of delay days. """
    rolling_window = (
        Window.partitionBy("SHADOW_ACCOUNT_NBR")
        .orderBy("year", "Month")
        .rowsBetween(-5, -0)
    )
 
    # Compute average delay gaps  and standard deviation where trx_count > 0
    df = df.withColumn(
        "rolling_avg_frequency",
               F.round(F.avg("trx_count").over(rolling_window), 2)
    ).withColumn(
        "stddev_frequency",
               F.round(F.stddev_pop("trx_count").over(rolling_window), 2)
    )
 
    return df


def freq_deviation(df):
    df = df.withColumn('frequency_deviation',
                       F.when(F.col("rolling_avg_frequency").isNotNull(), F.round((F.col("trx_count") - F.col("rolling_avg_frequency")) / F.col("stddev_frequency"), 4)))
    return df    


## Purchase frequency rolling averge, standard deviation and deviation
def compute_rolling_stats_P(df):
    """ Compute rolling 24-week average and standard deviation of delay days. """
    rolling_window = (
        Window.partitionBy("SHADOW_ACCOUNT_NBR")
        .orderBy("year", "Month")
        .rowsBetween(-5, -0)
    )
 
    # Compute average delay gaps  and standard deviation where trx_count > 0
    df = df.withColumn(
        "rolling_avg_frequency_P",
               F.round(F.avg("purchases_trx_count").over(rolling_window), 2)
    ).withColumn(
        "stddev_frequency_P",
               F.round(F.stddev_pop("purchases_trx_count").over(rolling_window), 2)
    )
 
    return df


def freq_deviation_P(df):
    df = df.withColumn('frequency_deviation_P',
                       F.when(F.col("rolling_avg_frequency_P").isNotNull(), F.round((F.col("purchases_trx_count") - F.col("rolling_avg_frequency_P")) / F.col("stddev_frequency_P"), 4)))
    return df



## Deposit frequency rolling averge, standard deviation and deviation
def compute_rolling_stats_D(df):
    """ Compute rolling 24-week average and standard deviation of delay days. """
    rolling_window = (
        Window.partitionBy("SHADOW_ACCOUNT_NBR")
        .orderBy("year", "Month")
        .rowsBetween(-5, -0)
    )
 
    # Compute average delay gaps  and standard deviation where trx_count > 0
    df = df.withColumn(
        "rolling_avg_frequency_D",
               F.round(F.avg("deposits_trx_count").over(rolling_window), 2)
    ).withColumn(
        "stddev_frequency_D",
               F.round(F.stddev_pop("deposits_trx_count").over(rolling_window), 2)
    )
 
    return df


def freq_deviation_D(df):
    df = df.withColumn('frequency_deviation_D',
                       F.when(F.col("rolling_avg_frequency_D").isNotNull(), F.round((F.col("deposits_trx_count") - F.col("rolling_avg_frequency_D")) / F.col("stddev_frequency_D"), 4)))
    return df



## Same for monetary
def compute_rolling_amount_stats(df):
    """ Compute rolling 24-week average and standard deviation of monetary. """
    rolling_window = (
        Window.partitionBy("SHADOW_ACCOUNT_NBR")
        .orderBy("year", "Month")
        .rowsBetween(-5, -0)
    )
 
    df = df.withColumn(
        "rolling_avg_amount",
               F.round(F.avg("local_amount").over(rolling_window), 2)
    ).withColumn(
        "stddev_amount",
               F.round(F.stddev_pop("local_amount").over(rolling_window), 2)
    )
 
    return df


def amount_deviation(df):
    df = df.withColumn('amount_deviation',
                       F.when(F.col("rolling_avg_amount").isNotNull(), F.round((F.col("local_amount") - F.col("rolling_avg_amount")) / F.col("stddev_amount"), 4)))
    return df



def compute_rolling_amount_stats_P(df):
    """ Compute rolling 24-week average and standard deviation of delay days. """
    rolling_window = (
        Window.partitionBy("SHADOW_ACCOUNT_NBR")
        .orderBy("year", "Month")
        .rowsBetween(-5, -0)
    )
 
    df = df.withColumn(
        "rolling_avg_amount_P",
               F.round(F.avg("total_amount_purchase").over(rolling_window), 2)
    ).withColumn(
        "stddev_amount_P",
               F.round(F.stddev_pop("total_amount_purchase").over(rolling_window), 2)
    )
 
    return df


def amount_deviation_P(df):
    df = df.withColumn('amount_deviation_P',
                       F.when(F.col("rolling_avg_amount_P").isNotNull(), F.round((F.col("total_amount_purchase") - F.col("rolling_avg_amount_P")) / F.col("stddev_amount_P"), 4)))
    return df



def compute_rolling_amount_stats_D(df):
    """ Compute rolling 24-week average and standard deviation of delay days. """
    rolling_window = (
        Window.partitionBy("SHADOW_ACCOUNT_NBR")
        .orderBy("year", "Month")
        .rowsBetween(-5, -0)
    )
 
    df = df.withColumn(
        "rolling_avg_amount_D",
               F.round(F.avg("total_amount_deposit").over(rolling_window), 2)
    ).withColumn(
        "stddev_amount_D",
               F.round(F.stddev_pop("total_amount_deposit").over(rolling_window), 2)
    )
 
    return df


def amount_deviation_D(df):
    df = df.withColumn('amount_deviation_D',
                       F.when(F.col("rolling_avg_amount_D").isNotNull(), F.round((F.col("total_amount_deposit") - F.col("rolling_avg_amount_D")) / F.col("stddev_amount_D"), 4)))
    return df



def create_reference_data(sub_grouped):
    """
    Generates a reference DataFrame containing all months for each customer.
    Ensures missing months are filled with zero transactions.
    Returns the joined DataFrame.
    """
    month_df = sub_grouped.select("Month").distinct().orderBy(F.desc("Month"))
    customer_df = sub_grouped.select("SHADOW_ACCOUNT_NBR").distinct()
    year_df = sub_grouped.select("Year").distinct()
 
    ref_df = customer_df.crossJoin(month_df).crossJoin(year_df)
    joined_df = sub_grouped.join(ref_df, on=["SHADOW_ACCOUNT_NBR", "Month", "Year"], how="outer").fillna(0)
 
    return joined_df


#Calculate consecutive zero months for purchase
def calculate_consecutive_zero_purchases(df):
    """
    Identifies consecutive zero transaction periods for each customer.
    Returns a DataFrame with an additional column: 'consecutive_zero_months'.
    """
 
    # Combine Year and Month into a single date column for easier comparison
    df = df.withColumn('Date_purchase', F.concat(col('Year') , F.lit("-") , F.col('Month'), F.lit("-01") ))

    # Define window specification
    windowSpec = Window.partitionBy('SHADOW_ACCOUNT_NBR').orderBy('Year', 'Month')
    # Apply lag function with a condition to ignore zeros
    df = df.withColumn('prev_purchases_trx_date', 
                       F.last(F.when(col('purchases_trx_count') != 0, col('Date_purchase')), ignorenulls= True).over(windowSpec))


    # Calculate the difference in months
    df = df.withColumn('zero_purchase_counter', F.months_between(F.col('Date_purchase'), F.col('prev_purchases_trx_date')))
 
    return df

#Calculate consecutive zero months for deposits
def calculate_consecutive_zero_deposits(df):
    """
    Identifies consecutive zero transaction periods for each customer.
    Returns a DataFrame with an additional column: 'consecutive_zero_months'.
    """
 
    # Combine Year and Month into a single date column for easier comparison
    df = df.withColumn('Date_deposit', F.concat(col('Year') , F.lit("-") , F.col('Month'), F.lit("-01") ))

    # Define window specification
    windowSpec = Window.partitionBy('SHADOW_ACCOUNT_NBR').orderBy('Year', 'Month')
    # Apply lag function with a condition to ignore zeros
    df = df.withColumn('prev_deposits_trx_date', 
                       F.last(F.when(col('deposits_trx_count') != 0, col('Date_deposit')), ignorenulls= True).over(windowSpec))


    # Calculate the difference in months
    df = df.withColumn('zero_deposits_counter', F.months_between(F.col('Date_deposit'), F.col('prev_deposits_trx_date')))
 
    return df

#Calculate general consecutive zero months
def calculate_consecutive_zero_transactions(df):
    """
    Identifies consecutive zero transaction periods for each customer.
    Returns a DataFrame with an additional column: 'consecutive_zero_months'.
    """
 
    # Combine Year and Month into a single date column for easier comparison
    df = df.withColumn('Date', F.concat(col('Year') , F.lit("-") , F.col('Month'), F.lit("-01") ))

    # Define window specification
    windowSpec = Window.partitionBy('SHADOW_ACCOUNT_NBR').orderBy('Year', 'Month')
    # Apply lag function with a condition to ignore zeros
    df = df.withColumn('prev_trx_date', 
                       F.last(F.when(((col('deposits_trx_count') != 0)|(col('purchases_trx_count')!= 0)) & ((col('zero_purchase_counter').isNotNull())|(col('zero_deposits_counter').isNotNull())), 
                                     col('Date')), ignorenulls= True).over(windowSpec))


    # Calculate the difference in months
    df = df.withColumn('zero_transactions_counter', F.months_between(F.col('Date'), F.col('prev_trx_date')))
 
    return df

#Transacting or not for the next 6 months
def next_six_months(df):
    """
    Identifies customers who make at least one transaction within 6 months after a zero period.
    Returns a DataFrame with an additional column: 'is_revivor'.
    """
    # Define the 6-month forward-looking window
    window_spec = Window.partitionBy("SHADOW_ACCOUNT_NBR").orderBy("Year", "Month").rowsBetween(1, 6)
 
    # Sum transactions in the next 6 months
    df = (df.withColumn("6_months_purchase_trx_count", F.sum("purchases_trx_count").over(window_spec))
         .withColumn("6_months_deposits_trx_count", F.sum("deposits_trx_count").over(window_spec))
         .withColumn("next_6_months_trx_count", F.sum("purchases_trx_count").over(window_spec) + F.sum("deposits_trx_count").over(window_spec)))

 
    # Flag customers who have at least one transaction in the next 6 months
    df = df.withColumn("transacting_next_6_months", F.when(F.col("next_6_months_trx_count") > 0, 1).otherwise(0))
 
    return df

if __name__ == "__main__":
    # Load and preprocess data
    transactions = load_transaction_data()
    
    #get shadow account
    transactions = get_shadow_account(transactions)
    subgrouped = aggregate_transactions(df_)
    df2 = create_reference_data(subgrouped)
    df_stopper = calculate_consecutive_zero_purchases(df2)
    df_stopper = calculate_consecutive_zero_deposits(df_stopper)
    df_stopper = calculate_consecutive_zero_transactions(df_stopper)
    df_stopper2 = next_six_months(df_stopper)
    df2 = compute_rolling_stats(df_stopper2)
    df2 = freq_deviation(df2)
    df2 = compute_rolling_stats_P(df2)
    df2 = freq_deviation_P(df2)
    df2 = compute_rolling_stats_D(df2)
    df2 = freq_deviation_D(df2)
    df2 = compute_rolling_amount_stats(df2)
    df2 = amount_deviation(df2)
    df2 = compute_rolling_amount_stats_P(df2)
    df2 = amount_deviation_P(df2)
    df2 = compute_rolling_amount_stats_D(df2)
    df2 = amount_deviation_D(df2)
    df2.write.mode("overwrite").saveAsTable("analytics_zone_retail_cross_selling.hps_shadow_acc_agg_stopper_non_fees_version_3")