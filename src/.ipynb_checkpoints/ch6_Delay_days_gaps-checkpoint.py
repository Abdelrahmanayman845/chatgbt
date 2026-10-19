from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import col, when, lag, row_number
   
# ----------------------------- #
#   compute_delay_days
# ----------------------------- #     
    
def compute_delay_days(data):
    """ Compute delay in days based on previous transaction max date. """
    window_spec = Window.partitionBy("SHADOW_ACCOUNT_NBR").orderBy("year", "month")
 
    data = data.withColumn("prev_max_trx_date", F.lag("max_trx_date").over(window_spec))
    
    data = data.withColumn(
        "delay_days",
        F.datediff(F.col("min_trx_date"), F.col("prev_max_trx_date"))
    )   
    return data    

# ----------------------------- #
#  Identify the first transaction for each customer.
# ----------------------------- #
def identify_count_transaction(data):
    """
    Identify the first transaction for each customer.
    """
    windowSpec = Window.partitionBy("SHADOW_ACCOUNT_NBR").orderBy("year", "month")
    
    data = data.withColumn(
                    "first_non_null_trx_date",
                    F.first(F.when(F.col("trx_count") > 0, F.col("min_trx_date")), ignorenulls=True).over(windowSpec)
                            )
    data = data.withColumn("Time_since_first_transaction", F.datediff("min_trx_date","first_non_null_trx_date"))
    
    data = data.withColumn(
                "Count_Transactions_back", F.row_number().over(windowSpec))
    
    return data
        

# -----------------------------
#  Compute compute_avg 6 gaps  (Only for Transacted records)
# -----------------------------
def compute_avg_last_5_gaps(data):
    """ Compute avg last 6 gaps available """
    rolling_window = (
        Window.partitionBy("SHADOW_ACCOUNT_NBR")
        .orderBy("year", "month")
        .rowsBetween(-5, -0)
    )
 
    # Compute average delay gaps  and standard deviation where trx_count > 0
    data = data.withColumn(
        "avg_delay_gaps",
        F.when((F.col("Count_Transactions_back") >= 2),\
               F.round(F.avg("delay_days").over(rolling_window), 2))
    ).withColumn(
        "stddev_gaps",
        F.when((F.col("Count_Transactions_back") >= 2),\
               F.round(F.stddev("delay_days").over(rolling_window), 2))
    )
 
    return data
