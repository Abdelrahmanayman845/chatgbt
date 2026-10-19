from Development_Chadi.src.ch1_Data_loader import *
from Development_Chadi.src.ch2_Transactions_Aggregation import *
from Development_Chadi.src.ch3_Rolling_average import *
from Development_Chadi.src.ch4_Inactivity_counter import *
from Development_Chadi.src.ch5_remaining_balance import *
from Development_Chadi.src.ch6_Delay_days_gaps import *
from Development_Chadi.src.ch7_filling_missing import *
from pyspark import SparkContext, SparkConf
import os
from pyspark.sql import SparkSession
spark = (
    SparkSession
    .builder
    .appName("PythonSQL")
 
    # -------------------------
    # Adaptive execution (MUST)
    # -------------------------
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
 
    # -------------------------
    # Shuffle tuning
    # -------------------------
    .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "2000"))
    .config("spark.sql.files.maxPartitionBytes", "268435456")  # 256MB
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "268435456")
 
    # -------------------------
    # Join behavior
    # -------------------------
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # avoid wrong broadcast
    .config("spark.sql.join.preferSortMergeJoin", "true")
 
    # -------------------------
    # Stability (very important on big tables)
    # -------------------------
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .config("spark.sql.execution.sortBeforeRepartition", "true")
 
    .getOrCreate()
)
class DataPreparation:
    
    def __init__(self, spark):
        
        self.spark = spark
        # self.config = config
        self.balances_table = "staging_zone.hpsv3_v_cr_term_view"
        self.accounts_table = "staging_zone.hpsv3_shadow_account_cdc"
        
                # self.config = config
        self.source_table = "analytics_zone_retail_cross_selling.ch1_accounts_prepared_aggregated_2026_10" 
        self.target_table = "analytics_zone_retail_cross_selling.ch1_accounts_prepared_view_2026_10" +"_cdc"

        
    def run(self):
        agg_df = self.spark.table(self.source_table)
        
        agg_df = agg_df
        # print(agg_df.count())
        # Read balances
        df_balances = load_balances(self.spark, self.balances_table)
        #join on balances
        
        NUM_PARTITIONS = 200
 
        df_balances = df_balances.repartition(NUM_PARTITIONS, "shadow_account_nbr")
        agg_df      = agg_df.repartition(NUM_PARTITIONS, "shadow_account_nbr")
        df_trn_balances = agg_df.join(df_balances, on = ['shadow_account_nbr', 'year', 'month'], how = 'left')

        # Read account information
        df_accounts = load_accounts(self.spark, self.accounts_table)
        #join on balances
        final_df = df_trn_balances.join(df_accounts, on = ['shadow_account_nbr'], how = 'left')
        
        # Generate Date        
        # final_df = final_df.filter((F.col("period_date") >= F.lit("2022-06-1")))
        
        # Create available credit and utilized rate columns
        final_df = utilized_rate(final_df)
        # Changed the parameter form 0.95 to 0.8 
        final_df = available_balance(final_df, 0.95)        
        # Step 2: Filter on period_date >= '2022-06-30'
        
        final_df = account_status(final_df)
        # print(final_df.count())
        
        print('Saving the table...')
        print("done" , f"Table Name : {self.target_table}")

        final_df.write.mode("overwrite").saveAsTable(self.target_table)
        print(" __Successfully Writing")
        


