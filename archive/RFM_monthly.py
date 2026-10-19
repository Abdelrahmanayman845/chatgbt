import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when

# Load the data
weekly_rfm_df = spark.table("analytics_zone_retail_cross_selling.hps_shadow_cc_weekly_transactions_delay")

'''# Add month column
weekly_rfm_df = weekly_rfm_df.withColumn(
    "month",
    F.month(
        F.when(F.col("max_trx_date").isNotNull(), F.col("max_trx_date"))
        .otherwise(F.col("max_week_date"))
    )
)
'''
'''# Define window specification for first transaction date of the month
window_spec_w = Window.partitionBy("SHADOW_ACCOUNT_NBR", "year", "month").orderBy("week_of_year")
weekly_rfm_df = weekly_rfm_df.withColumn(
    "first_trx_date_month",
    F.first(col("max_week_date"), ignorenulls=True).over(window_spec_w)
)'''

# Define window specification for previous transaction date
window_spec = Window.partitionBy("SHADOW_ACCOUNT_NBR").orderBy("year", "week_of_year")
weekly_rfm_df = weekly_rfm_df.withColumn("prev_max_trx_date", F.lag("max_trx_date").over(window_spec))
weekly_rfm_df = weekly_rfm_df.withColumn(
    "prev_max_trx_date_filled",
    F.last("prev_max_trx_date", ignorenulls=True).over(window_spec),
)

# Add first month day column
weekly_rfm_df = weekly_rfm_df.withColumn(
    "first_month_day",
    F.trunc(F.concat_ws("-", F.col("year"), F.col("month"), F.lit("01")).cast("date"), "MM")
)

# Compute recency
weekly_rfm_df = weekly_rfm_df.withColumn(
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

# Get the first record of each month for recency
window_spec = Window.partitionBy("SHADOW_ACCOUNT_NBR", "year", "month").orderBy("max_week_date")
weekly_rfm_df = weekly_rfm_df.withColumn("row_number", F.row_number().over(window_spec))
weekly_rfm_df = weekly_rfm_df.filter(F.col("row_number") == 1).drop("row_number")

# Define rolling window specification for 6 months average and standard deviation of recency
rolling_window = (
    Window.partitionBy("SHADOW_ACCOUNT_NBR")
    .orderBy("year", "month")
    .rowsBetween(-5, 0)
)

# Compute rolling average recency and standard deviation where transaction count > 2
weekly_rfm_df = weekly_rfm_df.withColumn(
    "rolling_avg_recency",
    F.when(
        F.col("Count_Transactions_back") > 2,
        F.round(F.avg("recency").over(rolling_window), 2)
    )
).withColumn(
    "stddev_recency",
    F.when(
        F.col("Count_Transactions_back") > 2,
        F.round(F.stddev("recency").over(rolling_window), 2)
    )
)

# Compute recency deviation
weekly_rfm_df = weekly_rfm_df.withColumn(
    "recency_deviation",
    F.when(
        F.col("rolling_avg_recency").isNotNull(),
        F.round((F.col("recency") - F.col("rolling_avg_recency")) / F.col("stddev_recency"), 4)
    ).otherwise(None)
)

# Compute recency deviation from last gap
weekly_rfm_df = weekly_rfm_df.withColumn(
    "RD_from_last_gap",
    F.round((F.col("recency") - F.col("avg_delay_gaps")) / F.col("stddev_gaps"), 4)
)

# Save the result to a table
weekly_rfm_df.write.mode("overwrite").saveAsTable("analytics_zone_retail_cross_selling.hps_shadow_cc_monthly_transactions_delay")
