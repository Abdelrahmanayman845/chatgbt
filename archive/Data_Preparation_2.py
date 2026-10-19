# from Development_Chadi.src.ch1_Data_loader import *
# from Development_Chadi.src.ch2_Transactions_Aggregation import *
# from Development_Chadi.src.ch3_Rolling_average import *
# from Development_Chadi.src.ch4_Inactivity_counter import *
# from Development_Chadi.src.ch5_remaining_balance import *
# from Development_Chadi.src.ch6_Delay_days_gaps import *
# from Development_Chadi.src.ch7_filling_missing import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, DataFrame, HiveContext, Row
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.functions import col

class DataPreparation:
    
    def __init__(self, spark):
        
        self.spark = spark
        # self.config = config
        self.path = "staging_zone.hpsv3_mw_cr_transaction_view"
        self.balances_table = "staging_zone.hpsv3_cr_term_hist" ## Updating this to check the all balances 
        #self.balances_table = "staging_zone.hpsv3_cr_term_hist_cdc" ## Updating this to check the all balances 
        self.accounts_table = "staging_zone.hpsv3_shadow_account_cdc"

    def load_balances(self,spark: SparkSession, table_name: str)-> DataFrame:
        """
        Load the Credit Staement Balance Table from Hive once.
        Returns
        -------
        DataFrame
            Raw link table with  SHADOW_ACCOUNT_NBR, CREDIT_LIMIT and BALANCES.
        """
        df_balances = spark.table(self.balances_table).filter(F.col("hive_insertion_date") == "2026-04-01")
        
        df_balances = spark.table("staging_zone.hpsv3_cr_term_hist").select(
            F.col("shadow_account_nbr"),
            F.col("client_code").alias("client_code"),
            F.col("statement_date").alias("statement_date"),
            F.col("credit_limit").alias("credit_limit"),
            F.col("OPENING_BALANCE").alias("opening_balance"),
            F.col("closing_balance").alias("closing_balance"),
            'CURRENT_BALANCE_PURCHASE',
            'CURRENT_BALANCE_CASH',
            'CURRENT_BALANCE_TRANSFER',
            'CURRENT_BALANCE_ECOM',
            'CURRENT_BALANCE_CHEQUE',
            'CURRENT_BALANCE_INTEREST',
            'CURRENT_BALANCE_INSTALLMENT',
            'CURRENT_BALANCE_FEES',
            'CURRENT_BALANCE_TAX',
            'CURRENT_BALANCE_REFUND',
            'CURRENT_BALANCE_DEPOSIT',
            "DEBIT_ACCOUNT_NBR").filter(F.col('statement_date')>=F.lit('2022-06-01').cast('date'))
        
        # 3) Deduplicate WITHOUT global shuffle
        # -------------------------------------------------------
        df_balances = (

        df_balances
        .groupBy("shadow_account_nbr","statement_date")
        .agg(F.first(F.struct(*df_balances.columns)).alias("row"))
        .select("row.*")
    )

         # Add additional calculated or formatted columns if needed
        df_balances = df_balances.withColumn("Remaining_closing_Balance", F.col("credit_limit") - F.col("closing_balance")) \
                                 .withColumn("year", F.year(F.col("statement_date"))) \
                                 .withColumn("month", F.month(F.col("statement_date")))

        return df_balances

    def load_accounts(self,spark: SparkSession, table_name: str)-> DataFrame:
        """
        Load the Credit Staement Balance Table from Hive once.
        Returns
        -------
        DataFrame
            Raw link table with  SHADOW_ACCOUNT_NBR, Opening_date, Closing date.
        """    

        df_sh = spark.table("staging_zone.hpsv3_shadow_account").filter(F.col("hive_insertion_date") == "2025-07-01")
        df_sh = df_sh.select(
                        "SHADOW_ACCOUNT_NBR", "CANCELLATION_DATE", 
                        F.col("AGREEMENT_DATE").alias("opening_date"),
                        "ADMINISTRATIVE_STATUS","ADMINISTRATIVE_STATUS_DATE"
                        )

        df_sh = df_sh.withColumn(
            "closing_date",
            F.when(
                F.col("ADMINISTRATIVE_STATUS").isin([6, 7]),
                F.col("ADMINISTRATIVE_STATUS_DATE")
            ).otherwise(None).cast(DateType())
        )
        return df_sh

    # Create the column account status
    def account_status(self, df):
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
    def available_balance(self, df, threshold):

        df = df.withColumn(

            "available_credit",

            F.when(F.col("utilized_rate").isNull(), F.lit(None))  # keep null

             .when(F.col("utilized_rate") < threshold, 1)

             .otherwise(0)

        )

        return df
    def utilized_rate(self, df):
        df = df.withColumn("utilized_rate", F.col("opening_balance") / (F.col("credit_limit") + 1))
        return df

    def run(self):
        agg_df = self.spark.table("analytics_zone_retail_cross_selling.ch1_accounts_prepared_aggregated_2026_02_16")
        
        agg_df = agg_df
        # print(agg_df.count())
        # Read balances
        df_balances = self.load_balances(self.spark, self.balances_table)
        #join on balances
        df_trn_balances = agg_df.join(df_balances, on = ['shadow_account_nbr', 'year', 'month'], how = 'left')

        # Read account information
        df_accounts = self.load_accounts(self.spark, self.accounts_table)
        #join on balances
        final_df = df_trn_balances.join(df_accounts, on = ['shadow_account_nbr'], how = 'left')
        

        # Create available credit and utilized rate columns
        final_df = self.utilized_rate(final_df)
        final_df = self.available_balance(final_df, 0.95)        
        # Step 2: Filter on period_date >= '2022-06-30'
        
        final_df = self.account_status(final_df)
        
        print('Saving the table...')
        final_df.write.mode("overwrite").saveAsTable("analytics_zone_retail_cross_selling.ch1_accounts_prepared_view_2026_02_16" +"_cdc")
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
    pipeline = DataPreparation(spark)  
    # stopper = StopperPipeline(spark)
    
    # aggregation.run()
    pipeline.run()
    # stopper.run()
    spark.stop()
    
    
if __name__ == "__main__":
    main()
  
    