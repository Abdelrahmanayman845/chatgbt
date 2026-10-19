from pyspark.sql.functions import when
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import col, when, lag, row_number
    
# Define window specification for previous transaction date
# def previous_max_trx_date(df):
#     window_spec = Window.partitionBy("SHADOW_ACCOUNT_NBR").orderBy("year", "month")
#     df = df.withColumn("prev_max_trx_date", F.lag("max_trx_date").over(window_spec))
#     df = df.withColumn(
#         "prev_max_trx_date_filled",
#         F.last("prev_max_trx_date", ignorenulls=True).over(window_spec),
#     )
#     return df

def fill_missing_months(monthly_agg):
    """ Ensure all client-week pairs exist by cross-joining client codes with full weeks, filling forward missing values. """

    # Forward fill missing calculations
    window_spec = Window.partitionBy("SHADOW_ACCOUNT_NBR").orderBy("year", "month")
 
    monthly_agg = monthly_agg.withColumn("purchases_trx_count", F.coalesce(F.col("purchases_trx_count"), F.lit(0)))\
        .withColumn("trx_count", F.coalesce(F.col("purchases_trx_count"), F.lit(0)))\
        .withColumn("total_amount_purchase", F.coalesce(F.col("total_amount_purchase"), F.lit(0)))\
        .withColumn("Count_Transactions_back", F.last("Count_Transactions_back", ignorenulls=True).over(window_spec)) \
        .withColumn("delay_days", F.last("delay_days", ignorenulls=True).over(window_spec)) \
        .withColumn("avg_delay_gaps", F.last("avg_delay_gaps", ignorenulls=True).over(window_spec)) \
        .withColumn("stddev_gaps", F.last("stddev_gaps", ignorenulls=True).over(window_spec)) \
        .withColumn('prev_max_trx_date_filled', F.lag(F.col('max_trx_date'),1).over(window_spec))\
        .withColumn("prev_max_trx_date_filled",F.last("prev_max_trx_date_filled", ignorenulls=True).over(window_spec))
       
 
    return monthly_agg



# Add first month day column
def first_day_month(df):
    df = df.withColumn(
        "first_month_day",
        F.trunc(F.concat_ws("-", F.col("year"), F.col("month"), F.lit("01")).cast("date"), "MM")
    )

    return df



# Compute recency

## Account status
def recency(df):
    df = df.withColumn(
        "recency",
        when(
            (col("min_trx_date").isNotNull()) &
            (col("max_trx_date").isNotNull()) &
            (
                (F.month(col("min_trx_date")) != F.month(col("max_trx_date"))) &
                (F.year(col("min_trx_date")) == F.year(col("max_trx_date")))
            ),
            F.datediff(col("max_trx_date"), col("min_trx_date"))
        ).otherwise(
            when(
                (col("min_trx_date").isNotNull()) &
                (col("max_trx_date").isNotNull()) &
                (
                    (F.month(col("min_trx_date")) == F.month(col("max_trx_date"))) &
                    (F.year(col("min_trx_date")) == F.year(col("max_trx_date")))
                ),
                col("delay_days")
            ).otherwise(
                when(
                    (col("min_trx_date").isNull()) &
                    (col("max_trx_date").isNull()),
                    F.datediff(col("first_month_day"), col("prev_max_trx_date_filled"))
                ).otherwise(
                    when(
                        (col("min_trx_date").isNotNull()) &
                        (col("max_trx_date").isNotNull()) &
                        (
                            (F.month(col("min_trx_date")) != F.month(col("max_trx_date"))) &
                            (F.year(col("min_trx_date")) != F.year(col("max_trx_date")))
                        ),
                        F.datediff(col("max_trx_date"), col("min_trx_date"))
                    )
                )
            )
        )
    )

    return df

