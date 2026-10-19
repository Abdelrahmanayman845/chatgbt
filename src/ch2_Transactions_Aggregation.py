from pyspark.sql import functions as F
from pyspark.sql import Column
 

def debit_deposit(df):
    """
    Create debit and credit frequency and amount indicator columns.
 
    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame with TRANSACTION_SIGN and LOCAL_AMOUNT.
 
    Returns
    -------
    DataFrame
        DataFrame with new columns for purchase/deposit frequencies and amounts.
    """
    return (
        df.withColumn(
            'frequency_purchase',
            F.when(F.col('TRANSACTION_SIGN') == 'D', 1).otherwise(0)
        )

        .withColumn(
            'amount_purchase',
            F.when(F.col('TRANSACTION_SIGN') == 'D', F.col('BILLING_AMOUNT')).otherwise(0)
        )
    )


def aggregate_transactions(df_):
    """
    Aggregate transactions to compute total amount and count of transactions
    per customer, year, and month.
 
    Parameters
    ----------
    df_ : DataFrame
        Input Spark DataFrame with debit/credit indicators.
 
    Returns
    -------
    DataFrame
        Aggregated DataFrame with transaction counts and sums.
    """
    grouped = (
        df_.groupBy('SHADOW_ACCOUNT_NBR', 'Year', 'Month')
        .agg(
            F.sum('frequency_purchase').alias('purchases_trx_count'),
            F.sum('BILLING_AMOUNT').alias('total_amount'),
            F.sum('amount_purchase').alias('total_amount_purchase'),
            F.min("PROCESSING_DATE").alias("min_trx_date"),
            F.max("PROCESSING_DATE").alias("max_trx_date")
        )
    )
 
    grouped = grouped.withColumn(
        'trx_count',
        F.col('purchases_trx_count') 
    )
 
    return grouped

 
def build_month_dim(spark, start_date: str, end_date: str):
    """
    Build a small month dimension from start_date to end_date (inclusive),
    with one row per month.
 
    Parameters
    ----------
    spark : SparkSession
    start_date : str  (e.g. "2020-01-01")
    end_date   : str  (e.g. "2025-12-01")
 
    Returns
    -------
    DataFrame with columns: Year, Month
    """
    months_df = (
        spark.range(1)  # single-row dummy DF
        .select(
            F.sequence(
                F.to_date(F.lit(start_date)),
                F.to_date(F.lit(end_date)),
                F.expr("interval 1 month")
            ).alias("month_date")
        )
        .select(F.explode("month_date").alias("month_date"))
        .select(
            F.year("month_date").alias("Year"),
            F.month("month_date").alias("Month"),
        )
    )
 
    return months_df

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
 
def create_reference_data( spark, sub_grouped, start_date: str, end_date: str):
    """
    Generates a reference DataFrame containing all months for each customer,
    between start_date and end_date, filling missing months with nulls (or zero
    later via coalesce).
 
    Parameters
    ----------
    sub_grouped : DataFrame
        Aggregated transaction DataFrame. Must have:
        - SHADOW_ACCOUNT_NBR
        - Year
        - Month
    spark : SparkSession
    start_date : str, e.g. "2020-01-01"
    end_date   : str, e.g. "2025-12-01"
 
    Returns
    -------
    DataFrame
        Joined DataFrame with full customer × month grid.
    """
 
    # 1) Small month dimension (broadcastable)
    month_dim = build_month_dim(spark, start_date, end_date)
 
    # 2) Distinct customers (can be large)
    customer_df = sub_grouped.select("SHADOW_ACCOUNT_NBR").dropDuplicates()
 
    # 3) Cross join customers × months (months broadcast)
    #    This is the reference grid we want to guarantee.
    ref_df = customer_df.crossJoin(broadcast(month_dim))
 
    # 4) Left join sub_grouped onto the reference grid
    #    (or right join if you prefer the other direction)
    joined_df = (
        ref_df
        .join(
            sub_grouped,
            on=["SHADOW_ACCOUNT_NBR", "Year", "Month"],
            how="left"
        )
        #.filter(F.col("trx_count").isNotNull())     #i fixed it to prevent null  in the agg table (real trx)
    )
 
    return joined_df

# Create the column account status
def account_status(df):
    df = df.withColumn('trunced_closing', F.trunc('closing_date','MM'))
    df = df.withColumn('trunced_opening', F.trunc('opening_date','MM'))
    
    df = df.withColumn(

    'account_status',

    F.when(F.col('trunced_opening') > F.col('period_date'), 'Not joined')

     .when(

         (F.col('trunced_closing') <= F.col('period_date')),



         'Churn'

     )

     .when(F.col('purchases_trx_count') > 0, 'Transacted')

     .when(

         ((F.col('trunced_closing') >= F.col('period_date')) | F.col('trunced_closing').isNull()) &

         ((F.col('purchases_trx_count') == 0)|(F.col('purchases_trx_count').isNull())),

         'No transaction'

     )

     .otherwise(None)

    )

    return df

def utilized_rate(df):
    df = df.withColumn("utilized_rate", F.col("opening_balance") / (F.col("credit_limit") + 1))
    return df

def available_balance(df, threshold):

    df = df.withColumn(

        "available_credit",

        F.when(F.col("utilized_rate").isNull(), F.lit(None))  # keep null

         .when(F.col("utilized_rate") < threshold, 1)

         .otherwise(0)

    )

    return df

