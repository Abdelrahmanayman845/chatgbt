import pyspark.sql.functions  as F
from pyspark.sql import Window
from pyspark.sql.functions import col, when, lag, row_number
 
def get_rolling_window(
    partition_col="SHADOW_ACCOUNT_NBR",
    order_cols=("year", "Month"),
    lookback=5
):
    """
    Create a rolling window specification for computing rolling stats.
 
    Parameters
    ----------
    partition_col : str
        Column to partition by (typically account ID).
    order_cols : tuple
        Columns to order by (typically year, month).
    lookback : int
        Number of periods to look back (inclusive).
 
    Returns
    -------
    WindowSpec
        Spark window specification.
    """
    return (
        Window
        .partitionBy(partition_col)
        .orderBy(*order_cols)
        .rowsBetween(-lookback, 0)
    )
 

def compute_rolling_stats(
    df,
    value_col,
    rolling_avg_col,
    rolling_stddev_col,
    lookback=5
):
    """
    Add rolling average and standard deviation columns for a given value.
 
    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame.
    value_col : str
        Column to compute stats on.
    rolling_avg_col : str
        Name for the new rolling average column.
    rolling_stddev_col : str
        Name for the new rolling stddev column.
    lookback : int
        Rolling window lookback.
 
    Returns
    -------
    DataFrame
        DataFrame with rolling average and stddev columns.
    """
    window_spec = get_rolling_window(lookback=lookback)
 
    return (
        df.withColumn(
            rolling_avg_col,
            F.round(F.avg(value_col).over(window_spec), 2)
        ).withColumn(
            rolling_stddev_col,
            F.round(F.stddev_pop(value_col).over(window_spec), 2)
        )
    )
 

def compute_deviation(
    df,
    value_col,
    rolling_avg_col,
    rolling_stddev_col,
    deviation_col
):
    """
    Add standardized deviation column.
 
    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame.
    value_col : str
        Original value column.
    rolling_avg_col : str
        Column with rolling average.
    rolling_stddev_col : str
        Column with rolling stddev.
    deviation_col : str
        Name for the new deviation column.
 
    Returns
    -------
    DataFrame
        DataFrame with deviation column.
    """
    return df.withColumn(
        deviation_col,
        F.when(
            F.col(rolling_avg_col).isNotNull(),
            F.round(
                (F.col(value_col) - F.col(rolling_avg_col)) / F.col(rolling_stddev_col),
                4
            )
        )
    )

# ------------------------------
# Generic frequency
# ------------------------------
 
# def compute_rolling_freq_stats(df):
#     """
#     Rolling stats for local monetary amount.
#     """
#     return compute_rolling_stats(
#         df,
#         value_col="trx_count",
#         rolling_avg_col="rolling_avg_frequency",
#         rolling_stddev_col="stddev_frequency"
#     )
 

# def freq_deviation(df):
#     """
#     Standardized deviation for local monetary amount.
#     """
#     return compute_deviation(
#         df,
#         value_col="trx_count",
#         rolling_avg_col="rolling_avg_frequency",
#         rolling_stddev_col="stddev_frequency",
#         deviation_col="frequency_deviation"
#     )
 


# ------------------------------
# Purchase Frequency Functions
# ------------------------------
 
def compute_rolling_stats_P(df):
    """
    Rolling stats for purchase transaction count.
    """
    return compute_rolling_stats(
        df,
        value_col="purchases_trx_count",
        rolling_avg_col="rolling_avg_frequency_P",
        rolling_stddev_col="stddev_frequency_P"
    )
 

    
def freq_deviation_P(df):
    """
    Standardized deviation for purchase transaction count.
    """
    return compute_deviation(
        df,
        value_col="purchases_trx_count",
        rolling_avg_col="rolling_avg_frequency_P",
        rolling_stddev_col="stddev_frequency_P",
        deviation_col="frequency_deviation_P"
    )

# ------------------------------
# Deposit Frequency Functions
# ------------------------------
 
# def compute_rolling_stats_D(df):
#     """
#     Rolling stats for deposit transaction count.
#     """
#     return compute_rolling_stats(
#         df,
#         value_col="deposits_trx_count",
#         rolling_avg_col="rolling_avg_frequency_D",
#         rolling_stddev_col="stddev_frequency_D"
#     )
 

    
# def freq_deviation_D(df):
#     """
#     Standardized deviation for deposit transaction count.
#     """
#     return compute_deviation(
#         df,
#         value_col="deposits_trx_count",
#         rolling_avg_col="rolling_avg_frequency_D",
#         rolling_stddev_col="stddev_frequency_D",
#         deviation_col="frequency_deviation_D"
#     )


# ------------------------------
# Generic Monetary Amount
# ------------------------------
 
# def compute_rolling_amount_stats(df):
#     """
#     Rolling stats for local monetary amount.
#     """
#     return compute_rolling_stats(
#         df,
#         value_col="total_amount",
#         rolling_avg_col="rolling_avg_amount",
#         rolling_stddev_col="stddev_amount"
#     )
 

# def amount_deviation(df):
#     """
#     Standardized deviation for local monetary amount.
#     """
#     return compute_deviation(
#         df,
#         value_col="total_amount",
#         rolling_avg_col="rolling_avg_amount",
#         rolling_stddev_col="stddev_amount",
#         deviation_col="amount_deviation"
#     )
 

# ------------------------------
# Purchase Monetary Amount
# ------------------------------
 
def compute_rolling_amount_stats_P(df):
    """
    Rolling stats for total amount of purchases.
    """
    return compute_rolling_stats(
        df,
        value_col="total_amount_purchase",
        rolling_avg_col="rolling_avg_amount_P",
        rolling_stddev_col="stddev_amount_P"
    )
 

def amount_deviation_P(df):
    """
    Standardized deviation for total amount of purchases.
    """
    return compute_deviation(
        df,
        value_col="total_amount_purchase",
        rolling_avg_col="rolling_avg_amount_P",
        rolling_stddev_col="stddev_amount_P",
        deviation_col="amount_deviation_P"
    )
 

    
# ------------------------------
# Deposit Monetary Amount
# ------------------------------
 
# def compute_rolling_amount_stats_D(df):
#     """
#     Rolling stats for total amount of deposits.
#     """
#     return compute_rolling_stats(
#         df,
#         value_col="total_amount_deposit",
#         rolling_avg_col="rolling_avg_amount_D",
#         rolling_stddev_col="stddev_amount_D"
#     )
 

# def amount_deviation_D(df):
#     """
#     Standardized deviation for total amount of deposits.
#     """
#     return compute_deviation(
#         df,
#         value_col="total_amount_deposit",
#         rolling_avg_col="rolling_avg_amount_D",
#         rolling_stddev_col="stddev_amount_D",
#         deviation_col="amount_deviation_D"
#     )
 

# ------------------------------
# Generic recency
# ------------------------------
    
def rolling_rec_stats(df):
    window_spec = get_rolling_window(lookback=5)
    
    df = df.withColumn(
        "rolling_avg_recency",
        F.when(
            F.col("Count_Transactions_back") >= 2,
            F.round(F.avg("recency").over(window_spec), 2)
        )
    ).withColumn(
        "stddev_recency",
        F.when(
            F.col("Count_Transactions_back") >= 2,
            F.round(F.stddev("recency").over(window_spec), 2)
        )
    )
    
    w = Window.partitionBy('SHADOW_ACCOUNT_NBR').orderBy("period_date").rowsBetween(Window.unboundedPreceding, 0)
 
    df = df.withColumn(

    "stddev_recency",

    F.when(

        F.col("stddev_recency")==0,

        1

    ).otherwise(F.col("stddev_recency"))

    )
    
    return df


# Compute recency deviation from last gap

def deviation_recency(df):
    df = df.withColumn(
        "RD_from_last_gap",
        F.round((F.col("recency") - F.col("avg_delay_gaps")) / F.col("stddev_gaps"), 4)
    )
    return df


# exist 
# def compute_rolling_stats(data):
#     """ Compute rolling 24-week average and standard deviation of delay days. """
#     rolling_window = (
#         Window.partitionBy("SHADOW_ACCOUNT_NBR")
#         .orderBy("year", "month")
#         .rowsBetween(-5, 0)
#     )
 
#     # Compute average delay gaps  and standard deviation where trx_count > 0
#     data = data.withColumn(
#         "avg_delay_gaps",
#         F.when((F.col("Count_Transactions_back") > 2),\
#                F.round(F.avg("delay_days").over(rolling_window), 2))
#     ).withColumn(
#         "stddev_gaps",
#         F.when((F.col("Count_Transactions_back") > 2),\
#                F.round(F.stddev("delay_days").over(rolling_window), 2))
#     )
#     return data