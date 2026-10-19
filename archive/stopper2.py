from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, DataFrame, HiveContext, Row
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.functions import col
import pyspark.sql.functions  as F
from pyspark.sql import Window
from pyspark.sql.functions import col, when, lag, row_number


class StopperPipeline:
    
    def __init__(self, spark):
        
        self.spark = spark
        # self.config = config
        self.source_table = "analytics_zone_retail_cross_selling.ch1_accounts_prepared_view_2026_02_16" +"_cdc"
        self.target_table = "analytics_zone_retail_cross_selling.ch3_churn_final_view_2026_02_16_test" +"_cdc"
        
    def fill_missing_months(self, monthly_agg):
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

    def first_day_month(self,df):
        df = df.withColumn(
            "first_month_day",
            F.trunc(F.concat_ws("-", F.col("year"), F.col("month"), F.lit("01")).cast("date"), "MM")
        )

        return df
    
    def recency(self,df):
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
        
        
    def deviation_recency(self,df):
        df = df.withColumn(
            "RD_from_last_gap",
            F.round((F.col("recency") - F.col("avg_delay_gaps")) / F.col("stddev_gaps"), 4)
        )
        return df
    def next_six_months(self,df):
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
    
    def get_rolling_window(
    self,
    partition_col="SHADOW_ACCOUNT_NBR",
    order_cols=("year", "month"),
    lookback=5
):
        return (
            Window
            .partitionBy(partition_col)
            .orderBy(*order_cols)
            .rowsBetween(-lookback, -1)   # exclude current row
        )
 
 
    def compute_rolling_stats(
        self,
        df,
        value_col,
        rolling_avg_col,
        rolling_stddev_col,
        lookback=5
    ):

        window_spec = self.get_rolling_window(lookback=lookback)

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
        self,
        df,
        value_col,
        rolling_avg_col,
        rolling_stddev_col,
        deviation_col
    ):
        return df.withColumn(
            deviation_col,
            F.when(
                (F.col(rolling_stddev_col).isNotNull()) & (F.col(rolling_stddev_col) != 0),
                F.round(
                    (F.col(value_col) - F.col(rolling_avg_col)) / F.col(rolling_stddev_col),
                    4
                )
            )
        )


    def compute_rolling_stats_P(self, df):
        return self.compute_rolling_stats(
            df,
            value_col="purchases_trx_count",
            rolling_avg_col="rolling_avg_frequency_P",
            rolling_stddev_col="stddev_frequency_P"
        )


    def freq_deviation_P(self, df):
        return self.compute_deviation(
            df,
            value_col="purchases_trx_count",
            rolling_avg_col="rolling_avg_frequency_P",
            rolling_stddev_col="stddev_frequency_P",
            deviation_col="frequency_deviation_P"
        )

    def counter_remaining_balance_P(self,stopper,threshold):
        df = stopper.withColumn("utilized_rate", col("opening_balance") / (col("credit_limit") + 1))
        df = df.withColumn("balance_and_inactive_P", 
                                when((col("purchases_trx_count")==0)&(col('utilized_rate') < threshold)&(col('trunced_opening')<=col('period_date')),True).otherwise(False))
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

        df = df.withColumn(

        'balance_and_inactivity_p_counter',

        F.when(F.col('account_status') == 'Not joined', F.lit(None))

         .otherwise(F.col('balance_and_inactivity_p_counter'))

    )

        df = df.withColumn(

        'balance_and_inactivity_p_counter',

        F.when(F.col('account_status') == 'Not joined', F.lit(None))

         .otherwise(F.col('balance_and_inactivity_p_counter'))

        )

        df = df.withColumn(

            'balance_and_inactivity_p_counter',

            F.when((F.col('account_status') == 'Churn')&(F.col('balance_and_inactivity_p_counter') >= 0)&(F.col('trunced_closing') < F.col('period_date')), F.lit(None))

             .otherwise(F.col('balance_and_inactivity_p_counter'))

        )

        df = df.withColumn('balance_and_inactivity_p_counter',

                                         F.when((F.col('trx_count') == 0) & (F.col('utilized_rate') >= 0.95), 0)

                                         .otherwise(F.col('balance_and_inactivity_p_counter')))


        return df.drop('groupP','consecutive_flags_P','group','consecutive_flags','balance_and_inactive_P','counter_raw_P')
    
    def delay_days_balance(self,df):
        window_spec = Window.partitionBy("SHADOW_ACCOUNT_NBR").orderBy("year", "month")

        df = df.withColumn("last_valid_date",F.col("prev_max_trx_date"))


        df = df.withColumn("credit_counter", F.when((F.col("available_credit") != 1)&(F.col('trx_count')<1), 1).otherwise(0))


        w = Window.partitionBy("SHADOW_ACCOUNT_NBR").orderBy("year","month").rowsBetween(Window.unboundedPreceding, 0)

        df = df.withColumn(

            "grp_counter",

            F.sum(F.when(F.col("trx_count") > 0, 1).otherwise(0)).over(w)

        )
        w2 = Window.partitionBy('SHADOW_ACCOUNT_NBR').orderBy("year", "month")

        df = df.withColumn("grp_counter",F.lag('grp_counter',1).over(w2))

        w2 = Window.partitionBy('SHADOW_ACCOUNT_NBR','grp_counter').orderBy("year", "month")


        df = df.withColumn(

            "sum_streak",

            F.sum('credit_counter').over(w2)

        )


        df = df.withColumn(

            "last_valid_date",

            F.add_months(

                F.last("last_valid_date", ignorenulls=True).over(w),

                F.col("sum_streak")

            )

        )




        df = df.withColumn(
                "delay_days_P_balances",
                F.datediff(F.col("min_trx_date"), F.col("last_valid_date"))
            )    
        df = df.withColumn("delay_days_P_balances",F.last("delay_days_P_balances", ignorenulls=True).over(window_spec))
        return df.drop('last_valid_date', 'grp_counter')
    
    def rolling_delay_stats(self,df):
        window_spec = self.get_rolling_window(lookback=5)

        df = df.withColumn(
            "rolling_avg_delay_days_p",
            F.when(
                F.col("Count_Transactions_back") >= 2,
                F.round(F.avg("delay_days_p_balances").over(window_spec), 2)
            )
        ).withColumn(
            "stddev_delay_days_p",
            F.when(
                F.col("Count_Transactions_back") >= 2,
                F.round(F.stddev("delay_days_p_balances").over(window_spec), 2)
            )
        )

        return df
    
    def filling_gaps(self,df):
        window_spec = Window.partitionBy("SHADOW_ACCOUNT_NBR").orderBy("year", "month")
        df = df.withColumn(

            "stddev_delay_days_p",

            F.when(F.col("trx_count") == 0, F.lit(None))

             .otherwise(F.col("stddev_delay_days_p"))

        )
        df = df.withColumn("stddev_delay_days_p",F.last("stddev_delay_days_p", ignorenulls=True).over(window_spec))

        df = df.withColumn(

            "rolling_avg_delay_days_p",

            F.when(F.col("trx_count") == 0, F.lit(None))

             .otherwise(F.col("rolling_avg_delay_days_p"))

        )
        df = df.withColumn("rolling_avg_delay_days_p",F.last("rolling_avg_delay_days_p", ignorenulls=True).over(window_spec))
        return df


        
    def deviation_recency_balance(self,df):
        df = df.withColumn(
            "RD_from_last_gap_balance",
            F.round((F.col("recency_balance") - F.col("rolling_avg_delay_days_p")) / F.col("stddev_delay_days_p"), 4)
        )
        return df
    
    def limit_segment(self,df):
        window_spec = Window.partitionBy('SHADOW_ACCOUNT_NBR').orderBy('Year','Month').rowsBetween(Window.unboundedPreceding,0)

        df = df.withColumn(
            "credit_limit_segment",
            when((col("credit_limit") >= 1000) & (col("credit_limit") <= 5000), "1k-5k")
            .when((col('credit_limit').isNull()),None)
            .when((col("credit_limit") > 5000) & (col("credit_limit") <= 15000), "5k-15k")
            .when((col("credit_limit") > 15000) & (col("credit_limit") <= 25000), "15k-25k")
            .when((col("credit_limit") > 25000) & (col("credit_limit") <= 50000), "25k-50k")
            .when((col("credit_limit") > 50000) & (col("credit_limit") <= 100000), "50k-100k")
            .when((col("credit_limit") > 100000) & (col("credit_limit") <= 500000), "100k-500k")
            .when((col("credit_limit") > 500000) & (col("credit_limit") <= 2000000), " 500k+")
            .otherwise("others")
        )

        df = df.withColumn('credit_limit_segment',
                                            F.last('credit_limit_segment',ignorenulls=True).over(window_spec))

        return df
    
    def recency_balance(self,df):

        df = df.withColumn(

            "recency_balance",

            F.when(

                (F.col("available_credit") == 0) & (F.col("trx_count") < 1),

                F.lit(0)

            ).otherwise(

                F.when(

                    ((F.col("available_credit") == 0) & (F.col("trx_count") > 0)) |

                    ((F.col("available_credit") == 1) & (F.col("trx_count") > 0)),

                    F.datediff(

                        F.col("min_trx_date"),

                        F.add_months(

                            F.col("prev_max_trx_date_filled"),

                            F.col("sum_streak")

                        )

                    )

                ).otherwise(

                    F.when(

                        (F.col("available_credit") == 1) & (F.col("trx_count") == 0),

                        F.datediff(

                        F.col("period_date"),

                        F.add_months(

                            F.col("prev_max_trx_date_filled"),

                            F.col("sum_streak")

                        )

                    )

                    )

                )

            )

        )

        return df.drop('sum_streak')
    def run(self):
        
        df_new = self.spark.table(self.source_table)        # For frequency (generic)

           #Fill Missing months and Forward Fill Calculated Values 
        df_new = self.fill_missing_months(df_new)
        # df_new = previous_max_trx_date(df_new)
        df_new = self.first_day_month(df_new)
        
#         #for recency 
        df_new =self.recency(df_new)
    
        # df_new = rolling_rec_stats(df_new)
        df_new =  self.deviation_recency(df_new)
        
        #Calculate consecutive zero months for (purchase , deposite , general)
        #Transacting or not for the next 6 months
        # df_new = calculate_consecutive_zero_deposits(df_new)
        # df_new = calculate_consecutive_zero_transactions(df_new)
        df_new = self.next_six_months(df_new)
 
 #Rollings     
        # df_new = compute_rolling_freq_stats(df_new)
        # df_new = freq_deviation(df_new)
    
        # Compute rolling stats and deviations for purchase frequency
        df_new = self.compute_rolling_stats_P(df_new)
        df_new = self.freq_deviation_P(df_new)

        # For deposits
        # df_new = compute_rolling_stats_D(df_new)
        # df_new = freq_deviation_D(df_new)

        # For monetary amount (generic)
        # df_new = compute_rolling_amount_stats(df_new)
        # df_new = amount_deviation(df_new)

        # For purchase amount
        # df_new = self.compute_rolling_amount_stats_P(df_new)
        # df_new = self.amount_deviation_P(df_new)

        # For deposit amount
        # df_new = compute_rolling_amount_stats_D(df_new)
        # df_new = amount_deviation_D(df_new)
                
        #remaining balance functions 
        # df_new = counter_remaining_balance(df_new, 0.92)
        df_new = self.counter_remaining_balance_P(df_new,0.95)
        # df_new =counter_remaining_balance_D(df_new)
        
        df_new = self.delay_days_balance(df_new)
        df_new = self.rolling_delay_stats(df_new)
        df_new = self.filling_gaps(df_new)
        
        df_new = self.recency_balance(df_new)

        
        df_new = self.limit_segment(df_new)
        df_new = self.deviation_recency_balance(df_new)
        print('all is done')
       
        # Step 2: Filter on period_date >= '2022-06-30'
        df_new = df_new.filter((F.col("period_date") >= F.lit("2022-06-1"))&
                                    (F.col("period_date") <= F.lit("2025-12-31"))    
                              )
        
        #Filtering on shadow accounts that only closed after 30/06 so we have at least one transaction for each customer
        df_new = df_new.filter((col('trunced_closing')>="2022-06-1")|(col('trunced_closing').isNull()))
        df_new = df_new.filter(col('opening_date').isNotNull())
        print("done")
                               
        df_new.write.mode("overwrite").saveAsTable(self.target_table)
        print(" __Successfully Writing")
        
def main():
    spark = SparkSession\
                        .builder\
                        .appName("PythonSQL")\
                        .config("spark.sql.parquet.datetimeRebaseModeInWrite","LEGACY")\
                        .config("spark.sql.legacy.timeParserPolicy","LEGACY")\
                        .config("spark.sql.execution.arrow.enabled", "true")\
                        .getOrCreate()
    # aggregation = Aggregation(spark)
    # pipeline = DataPreparation(spark)  
    stopper = StopperPipeline(spark)
    
    # aggregation.run()
    # pipeline.run()
    stopper.run()
    spark.stop()
    
    
if __name__ == "__main__":
    main()