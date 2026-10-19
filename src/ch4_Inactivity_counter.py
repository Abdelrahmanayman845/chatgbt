from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import col, when, lag, row_number
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
                       F.last(F.when(col('purchases_trx_count').isNotNull(), col('Date_purchase'))).over(windowSpec))


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
    df = (df.withColumn("6_months_purchase_trx_count", F.sum("purchases_trx_count").over(window_spec)))

 
    # Flag customers who have at least one transaction in the next 6 months
    df = df.withColumn("transacting_next_6_months", F.when(F.col("6_months_purchase_trx_count") > 0, 1).otherwise(0))
 
    return df