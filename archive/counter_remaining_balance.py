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
from pyspark.sql.functions import col, when, lag, row_number

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



# Getting consecutive 0 transactions with balances greater than or equal to a threshold

# Add a column, containing True if there's enough balance but no transactions
# the parameter threshold is the threshold of the utilized rate. for example if we put 0.92, it means that the customer still has at least 8% of his limit.

from pyspark.sql.functions import col, when, lag, row_number

def counter_remaining_balance(stopper, threshold):
    df = stopper.withColumn("utilized_rate", col("opening_balance") / (col("credit_limit") + 1))

    df = df.withColumn("balance_and_inactive", 
                when((col("local_amount") <= 0)&(col('utilized_rate') < threshold)&(col('utilized_rate').isNotNull())&(col('zero_transactions_counter').isNotNull()),True).otherwise(False))
    #Add 1 in the column balance_and_inactive if he transacted or have enough balance 
    df = df.withColumn('consecutive_flags', when(col('balance_and_inactive')==False,1).otherwise(0))
    #Creating a window
    wf = Window.orderBy("SHADOW_ACCOUNT_NBR", "Year", "Month").rowsBetween(Window.unboundedPreceding,-1)
    #creating group if there ar many consecutive 0
    df = df.withColumn('group',F.sum('consecutive_flags').over(wf))
    w_count = Window.partitionBy('group', 'SHADOW_ACCOUNT_NBR').orderBy("Year", "Month")
    #do a cumulative sum depending the on the group
    df = df.withColumn('counter_raw', when(col('balance_and_inactive')==True,row_number().over(w_count)))
    # df = df.withColumn('counter_raw', when(col('balance_and_inactive') == True, F.sum('consecutive_flags').over(w_count)))
    df = df.withColumn('balance_and_inactivity_counter', when((col('counter_raw').isNull())& (col('zero_transactions_counter').isNotNull()),0).otherwise(col('counter_raw')))
    return df




## Filling forward the credit limit value to nulls after churning
## Creating the credit limit segments
def limit_segment(df):
    window_spec = Window.partitionBy('SHADOW_ACCOUNT_NBR').orderBy('Year','Month').rowsBetween(Window.unboundedPreceding,0)

    from pyspark.sql.functions import when

    df = df.withColumn(
        "credit_limit_segment",
        when((col("credit_limit") >= 1000) & (col("credit_limit") <= 5000), "1k-5k")
        .when((col("credit_limit") > 5000) & (col("credit_limit") <= 50000), "5k-50k")
        .when((col("credit_limit") > 50000) & (col("credit_limit") <= 100000), "50k-100k")
        .when((col("credit_limit") > 100000) & (col("credit_limit") <= 500000), "100k-500k")
        .when((col("credit_limit") > 500000) & (col("credit_limit") <= 2000000), " 500k+")
        .otherwise("others")
    )
    
    df = df.withColumn('credit_limit_segment',
                                        F.last('credit_limit_segment',ignorenulls=True).over(window_spec))
    
    return df





# def counter_remaining_balance(stopper):
#     df_ = stopper.withColumn("balance_and_inactive", 
#                             when(((col("local_amount") <= 0)&(col('utilized_rate') < 0.92)&(col('zero_transactions_counter').isNotNull())),True).otherwise(False))
#     #Add 1 in the column balance_and_inactive if he transacted or have enough balance 
#     df_ = df_.withColumn('consecutive_flags', when(col('balance_and_inactive')==False,1).otherwise(0))
#     #Creating a window
#     wf = Window.orderBy("SHADOW_ACCOUNT_NBR", "Year", "Month").rowsBetween(Window.unboundedPreceding,-1)
#     #creating group if there ar many consecutive 0
#     df_ = df_.withColumn('group',F.sum('consecutive_flags').over(wf))
#     w_count = Window.partitionBy('group').orderBy("SHADOW_ACCOUNT_NBR", "Year", "Month")
#     #do a cumulative sum depending the on the group
#     df_ = df_.withColumn('counter_raw', when(col('balance_and_inactive')==True,row_number().over(w_count)))

#     df_ = df_.withColumn(
#         "balance_and_inactivity_counter",
#         when(
#             (col("counter_raw").isNull()) & 
#             (col("zero_transactions_counter").isNotNull()) & 
#             (col("credit_limit").isNotNull()), 0
#         ).otherwise(
#             when(
#                 (col("counter_raw").isNull()) & 
#                 (col("zero_transactions_counter").isNotNull()) & 
#                 (col("credit_limit").isNull()), col("zero_transactions_counter")
#             ).otherwise(col("counter_raw"))
#         )
#     )


    # return df_


#Zero purchase transactions counter considering the remaining balance 
def counter_remaining_balance_P(df):
    df = df.withColumn("balance_and_inactive_P", 
                            when((col("total_amount_purchase") <= 0)&(col('utilized_rate') < 0.92)&(col('zero_purchase_counter').isNotNull()),True).otherwise(False))
    #Add 1 in the column balance_and_inactive if he transacted or have enough balance 
    df = df.withColumn('consecutive_flags_P', when(col('balance_and_inactive_P')==False,1).otherwise(0))
    #Creating a window
    wf = Window.orderBy("SHADOW_ACCOUNT_NBR", "Year", "Month").rowsBetween(Window.unboundedPreceding,-1)
    #creating group if there ar many consecutive 0
    df = df.withColumn('groupP',F.sum('consecutive_flags_P').over(wf))
    w_count = Window.partitionBy('groupP', 'SHADOW_ACCOUNT_NBR').orderBy("Year", "Month")
    #do a cumulative sum depending the on the group
    df = df.withColumn('counter_raw_P', when(col('balance_and_inactive_P')==True,row_number().over(w_count)))
    df = df.withColumn('balance_and_inactivity_P_counter', when((col('counter_raw_P').isNull())& (col('zero_purchase_counter').isNotNull()),0).otherwise(col('counter_raw_P')))

    df = df.drop('groupP','consecutive_flags_P','group','consecutive_flags')
    return df


#Zero deposit transactions counter considering the remaining balance 
def counter_remaining_balance_D(df):
    df = df.withColumn("balance_and_inactive_D", 
                            when((col("total_amount_deposit") <= 0)&(col('utilized_rate') < 0.92)&(col('zero_deposits_counter').isNotNull()),True).otherwise(False))
    #Add 1 in the column balance_and_inactive if he transacted or have enough balance 
    df = df.withColumn('consecutive_flags_D', when(col('balance_and_inactive_D')==False,1).otherwise(0))
    #Creating a window
    wf = Window.orderBy("SHADOW_ACCOUNT_NBR", "Year", "Month").rowsBetween(Window.unboundedPreceding,-1)
    #creating group if there ar many consecutive 0
    df = df.withColumn('groupD',F.sum('consecutive_flags_D').over(wf))
    w_count = Window.partitionBy('groupD', 'SHADOW_ACCOUNT_NBR').orderBy("Year", "Month")
    #do a cumulative sum depending the on the group
    df = df.withColumn('counter_raw_D', when(col('balance_and_inactive_D')==True,row_number().over(w_count)))
    df = df.withColumn('balance_and_inactivity_D_counter', when((col('counter_raw_D').isNull())& (col('zero_deposits_counter').isNotNull()),0).otherwise(col('counter_raw_D')))

    df = df.drop('groupD','consecutive_flags_D','groupD','groupP','consecutive_flagsP', 'consecutive_flagsD','counter_raw_D', 'counter_raw', 'counter_raw_P')
    return df





if __name__ == "__main__":
    df_balances = spark.table("staging_zone.hpsv3_cr_term_hist")

    # Add additional calculated or formatted columns if needed
    df_balances = df_balances.withColumn("Remaining_Balance", F.col("credit_limit") - F.col("closing_balance")) \
                             .withColumn("year", F.year(F.col("statement_date"))) \
                             .withColumn("month", F.month(F.col("statement_date")))

    df_balances = df_balances.select(
        F.col("shadow_account_nbr"),
        F.col("client_code").alias("client_code"),
        F.col("statement_date").alias("statement_date"),
        F.col("year").alias("year"),
        F.col("month").alias("month"),
        F.col("credit_limit").alias("credit_limit"),
        F.col("OPENING_BALANCE").alias("opening_balance"),
        F.col("closing_balance").alias("closing_balance"),
        F.col("Remaining_Balance").alias("remaining_balance"),
        F.col("CURRENT_BALANCE_PURCHASE").alias("current_balance_purchase"),
        F.col("CURRENT_BALANCE_CASH").alias("current_balance_cash"),
        F.col("CURRENT_BALANCE_TRANSFER").alias("current_balance_transfer"),
        F.col("CURRENT_BALANCE_ECOM").alias("current_balance_ecom"),
        F.col("CURRENT_BALANCE_CHEQUE").alias("current_balance_cheque"),
        F.col("CURRENT_BALANCE_INTEREST").alias("current_balance_interest"),
        F.col("CURRENT_BALANCE_INSTALLMENT").alias("current_balance_installment"),
        F.col("CURRENT_BALANCE_FEES").alias("current_balance_fees")
    )
    



    # read the stopper df
    df_stopper = spark.table("analytics_zone_retail_cross_selling.hps_shadow_acc_agg_stopper_non_fees_version_3")
    df_stopper = df_stopper.join(df_balances, on = ['shadow_account_nbr', 'year', 'month'], how = 'left')
    df_link = spark.table("staging_zone.hpsv3_accounts_link")
    df_link = df_link.withColumnRenamed('ENTITY_ID', 'SHADOW_ACCOUNT_NBR')
    df_link = df_link.select('SHADOW_ACCOUNT_NBR', 'OPENING_DATE')
    df_stopper = df_stopper.join(df_link, on = 'SHADOW_ACCOUNT_NBR', how = 'left')
    df = counter_remaining_balance(df_stopper, 0.92)
    df = counter_remaining_balance_P(df)
    df = counter_remaining_balance_D(df)
    df = limit_segment(df)
    # Filter on the date
    df = df.withColumn("Date", date_format(df_stopper["Date"], "yyyy-MM-dd"))
    df = df.filter(col('Date')<='2024-12-30')
    df = df.filter(col('Date')>='2022-06-01')
    df.write.mode("overwrite").saveAsTable("analytics_zone_retail_cross_selling.hps_shadow_acc_agg_stopper_remaining_non_fees_test2")