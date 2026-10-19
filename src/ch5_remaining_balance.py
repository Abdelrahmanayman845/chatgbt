from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import col, when, lag, row_number



# ------------------------------------------------- #
# Segments
# ------------------------------------------------- #  

def limit_segment(df):
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


# -------------------------------------------------------------------- #
#Zero purchase transactions counter considering the remaining balance  
# -------------------------------------------------------------------- #
from pyspark.sql import Window

from pyspark.sql.functions import col, when, lag, row_number
def counter_remaining_balance_P(stopper,threshold):
    # df = stopper.withColumn("utilized_rate", col("opening_balance") / (col("credit_limit") + 1))
    df = stopper.withColumn("balance_and_inactive_P", 
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
    ## changed in 20/09/2026 the utilized rate for Calculating the Recency from 0.95 to 0.80
    df = df.withColumn('balance_and_inactivity_p_counter',

                                     F.when((F.col('trx_count') == 0) & (F.col('utilized_rate') >= 0.95), 0)

                                     .otherwise(F.col('balance_and_inactivity_p_counter')))
  
 
    return df.drop('groupP','consecutive_flags_P','group','consecutive_flags','balance_and_inactive_P','counter_raw_P')


# -------------------------------------------------------------------- #
#Zero deposit transactions counter considering the remaining balance
# -------------------------------------------------------------------- #
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
 
def delay_days_balance(df):
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
    
    F.expr("""
        add_months(
            last(last_valid_date, true) over (
                partition by SHADOW_ACCOUNT_NBR 
                order by year, month 
                rows between unbounded preceding and current row
            ),
            sum_streak
        )  """))
 

    df = df.withColumn(
            "delay_days_P_balances",
            F.datediff(F.col("min_trx_date"), F.col("last_valid_date"))
        )    
    df = df.withColumn("delay_days_P_balances",F.last("delay_days_P_balances", ignorenulls=True).over(window_spec))
    return df.drop('last_valid_date', 'grp_counter')


def recency_balance(df):

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
                    F.expr("add_months(prev_max_trx_date_filled,sum_streak)")

                )

            ).otherwise(

                F.when(

                    (F.col("available_credit") == 1) & (F.col("trx_count") == 0),

                    F.datediff(

                        F.col("period_date"),
                        F.expr("add_months(prev_max_trx_date_filled,sum_streak)")   


                    )

                )

                )

            )

        )

    

    return df.drop('sum_streak')

def get_rolling_window(
    partition_col="SHADOW_ACCOUNT_NBR",
    order_cols=("period_date",),
    lookback=5):
    return (
        Window
        .partitionBy(partition_col)
        .orderBy(*order_cols)
        .rowsBetween(-lookback, 0)
    )
 
# window_spec = get_rolling_window(lookback=lookback)

def rolling_delay_stats(df):
    window_spec = get_rolling_window(lookback=5)
    
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
    

def filling_gaps(df):
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



def deviation_recency_balance(df):
    df = df.withColumn(
        "RD_from_last_gap_balance",
        F.round((F.col("recency_balance") - F.col("rolling_avg_delay_days_p")) / F.col("stddev_delay_days_p"), 4)
    )
    return df



