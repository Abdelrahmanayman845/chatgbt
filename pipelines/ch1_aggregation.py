from Development_Chadi.src.ch1_Data_loader import *
from Development_Chadi.src.ch2_Transactions_Aggregation import *
from Development_Chadi.src.ch3_Rolling_average import *
from Development_Chadi.src.ch4_Inactivity_counter import *
from Development_Chadi.src.ch5_remaining_balance import *
from Development_Chadi.src.ch6_Delay_days_gaps import *
from Development_Chadi.src.ch7_filling_missing import *


class Aggregation:
    
    def __init__(self, spark):
        
        self.spark = spark
        # self.config = config
        self.path = "staging_zone.hpsv3_mw_cr_transaction_view"
        self.target_table = "analytics_zone_retail_cross_selling.ch1_accounts_prepared_aggregated_2026_10" 

        
    def run(self):
        
        df = load_transaction_data(self.spark, self.path)
        
        # aggregation
        transactions_mapped = debit_deposit(df) ## Review 
        agg_df = aggregate_transactions(transactions_mapped)
        print('Done1')
        
        
        # compute delay days
        agg_df = compute_delay_days(agg_df)
        print('Done2')
        agg_df =  identify_count_transaction(agg_df)
        print('Done3')
        agg_df = compute_avg_last_5_gaps(agg_df)
        print('Done4')
        
        start_date = "2025-06-01" 
        end_date = "2026-10-01" # Changing Every Month        
        # Cross Join
        agg_df = create_reference_data(self.spark, agg_df, start_date, end_date) 
        print('Done5')
        agg_df = calculate_consecutive_zero_purchases(agg_df)
        agg_df = agg_df.withColumn(
                "period_date",
                F.to_date(
                    F.concat_ws(
                        "-",
                        F.col("Year").cast("string"),
                        F.lpad(F.col("Month").cast("string"), 2, "0"),
                        F.lit("01")
                    ),
                    "yyyy-MM-dd"
                )
                    )
        # Changing every month
        agg_df = agg_df.filter((F.col("period_date") < F.lit("2026-10-01"))&(F.col("period_date") >= F.lit("2025-06-01")))
        
        print('Saving the Full Series Aggregated Table...')
        print("done" , f"Table Name : {self.target_table}")

        agg_df.write.mode("overwrite").saveAsTable(self.target_table)
        print(" __Successfully Writing")