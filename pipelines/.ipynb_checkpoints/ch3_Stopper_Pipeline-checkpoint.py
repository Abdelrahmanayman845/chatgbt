from Development_Chadi.src.ch1_Data_loader import *
from Development_Chadi.src.ch2_Transactions_Aggregation import *
from Development_Chadi.src.ch3_Rolling_average import *
from Development_Chadi.src.ch4_Inactivity_counter import *
from Development_Chadi.src.ch5_remaining_balance import *
from Development_Chadi.src.ch6_Delay_days_gaps import *
from Development_Chadi.src.ch7_filling_missing import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .config("spark.sql.parquet.datetimeRebaseModeInWrite","LEGACY")\
    .config("spark.sql.legacy.timeParserPolicy","LEGACY")\
    .config("spark.sql.execution.arrow.enabled", "true")\
    .getOrCreate()

class StopperPipeline:
    
    def __init__(self, spark):
        
        self.spark = spark
        # self.config = config
        self.source_table = "analytics_zone_retail_cross_selling.ch1_accounts_prepared_view_2026_10" +"_cdc"
        self.target_table = "analytics_zone_retail_cross_selling.ch3_churn_final_view_2026_10" +"_cdc"

        
    def run(self):
        
        df_new = spark.table(self.source_table)        # For frequency (generic)
        df_new = df_new.dropDuplicates()
           #Fill Missing months and Forward Fill Calculated Values 
        df_new = fill_missing_months(df_new)
        # df_new = previous_max_trx_date(df_new)
        df_new = first_day_month(df_new)
        
#         #for recency 
        df_new =recency(df_new)
    
        # df_new = rolling_rec_stats(df_new)
        df_new =  deviation_recency(df_new)
        
        #Calculate consecutive zero months for (purchase , deposite , general)
        #Transacting or not for the next 6 months
        # df_new = calculate_consecutive_zero_deposits(df_new)
        # df_new = calculate_consecutive_zero_transactions(df_new)
        df_new = next_six_months(df_new)
 
 #Rollings     
        # df_new = compute_rolling_freq_stats(df_new)
        # df_new = freq_deviation(df_new)
    
        # Compute rolling stats and deviations for purchase frequency
        df_new = compute_rolling_stats_P(df_new)
        df_new = freq_deviation_P(df_new)

        # For deposits
        # df_new = compute_rolling_stats_D(df_new)
        # df_new = freq_deviation_D(df_new)

        # For monetary amount (generic)
        # df_new = compute_rolling_amount_stats(df_new)
        # df_new = amount_deviation(df_new)

        # For purchase amount
        df_new = compute_rolling_amount_stats_P(df_new)
        df_new = amount_deviation_P(df_new)

        # For deposit amount
        # df_new = compute_rolling_amount_stats_D(df_new)
        # df_new = amount_deviation_D(df_new)
                
        #remaining balance functions 
        # df_new = counter_remaining_balance(df_new, 0.92)
        df_new = counter_remaining_balance_P(df_new,0.95)
        # df_new =counter_remaining_balance_D(df_new)
        
        df_new = delay_days_balance(df_new)
        df_new = rolling_delay_stats(df_new)
        df_new = filling_gaps(df_new)
        
        df_new = recency_balance(df_new)

        
        df_new = limit_segment(df_new)
        df_new = deviation_recency_balance(df_new)
        print('all is done')
       
        # Step 2: Filter on period_date >= '2022-06-30'

        # Cr trm hist 6/2024 ## Change Every Month
        df_new = df_new.filter((F.col("period_date") >= F.lit("2025-06-01"))&
                                    (F.col("period_date") < F.lit("2026-10-01"))    
                              )
        
        #Filtering on shadow accounts that only closed after 30/06 so we have at least one transaction for each customer
        df_new = df_new.filter((col('trunced_closing')>="2025-06-01")|(col('trunced_closing').isNull()))
        # df_new = df_new.filter(col('opening_date').isNotNull())
        print("done" , f"Table Name : {self.target_table}")
                               
        df_new.write.mode("overwrite").saveAsTable(self.target_table)
        print(" __Successfully Writing")
        
        

        

