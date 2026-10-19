from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame, HiveContext, Row
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.functions import col
from pyspark.sql import Window

def load_transaction_data(spark: SparkSession, table_name: str) -> DataFrame:
    """
    Loads the transaction history table and filters out fee transactions.
    Returns a DataFrame with unique transactions.
    """
    transactions = spark.table(table_name).dropDuplicates()
    start_year = 2022
    start_month = 6

    table_trnx = transactions.filter(((F.col('hive_insertion_year') == start_year) & (F.col("hive_insertion_month") >= start_month)) |
                                       ((F.col('hive_insertion_year')> start_year ))
                                  )
    
    # Create window partitioned by MICROFILM_REF_NUMBER and BILLING_AMOUNT
    window_spec = Window.partitionBy("MICROFILM_REF_NUMBER", "BILLING_AMOUNT")
    # Filter out transactions that have both 'R' and 'N' flags in their group (reversals)
    transactions = transactions.withColumn(
        "has_both_flags",
        (F.sum(F.when(F.col("REVERSAL_FLAG") == "R", 1).otherwise(0)).over(window_spec) > 0) &
        (F.sum(F.when(F.col("REVERSAL_FLAG") == "N", 1).otherwise(0)).over(window_spec) > 0) 
    ).filter(
        (F.col("has_both_flags") == False) & (F.col("REVERSAL_FLAG") == "N" ) #new 
    ).drop("has_both_flags")
    # Keep only specific transaction descriptions
    transactions = transactions.filter(
        (col("TRANSACTION_DESCRIPTION") == 'Purchase') |
        (col("TRANSACTION_DESCRIPTION") == 'Withdrawal') |
        (col("TRANSACTION_DESCRIPTION") == 'Cash Advance') |
        (col("TRANSACTION_DESCRIPTION") == 'Cash Adv') |
        (col("TRANSACTION_DESCRIPTION") == 'Unique Transaction') |
        (col("TRANSACTION_DESCRIPTION") == 'Unique Trans')
    )
    # Filter out fee-related acronyms
    transactions = transactions.filter(~F.col("Acronym").contains("STAMP DUTY FEES"))
    transactions = transactions.filter(~F.col("Acronym").contains("BANQUE_MISR_TAKSIT"))
    transactions = transactions.filter(~F.col("Acronym").contains("BM WALLET"))
    transactions = transactions.filter(~F.col("Acronym").contains("INSTALLMENT FEE"))
    # Keep only debit transaction signs
    transactions = transactions.filter(col('transaction_sign') == 'D')
    # transactions = transactions.filter(col('REVERSAL_FLAG')=='N')
    # Filter by date
    # Add Year and Month columns
    transactions = transactions.withColumn("Year", F.col("hive_insertion_year"))
    transactions = transactions.withColumn("Month", F.col("hive_insertion_month"))
    # Select final columns
    transactions = transactions.select(
        'SHADOW_ACCOUNT_NBR',
        # 'CARD_NUMBER',
        'MICROFILM_REF_NUMBER',
        'Year',
        'Month',
        'PROCESSING_DATE',
        'TRANSACTION_SIGN',
        'BILLING_AMOUNT'
        # 'TRANSACTION_DESCRIPTION',
        # "STATEMENT_DATE"
        
    )
    
    return transactions


def load_balances(spark: SparkSession, table_name: str)-> DataFrame:
    df_balances = spark.sql(f"""
    SELECT * 
    FROM (
        SELECT * 
        FROM {table_name}
        WHERE hive_insertion_date = '2026-10-04'
    ) t
    """) # changes every month
    
    df_balances = spark.table(table_name).select(
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
        "DEBIT_ACCOUNT_NBR"
        ).filter(F.col('statement_date')>=F.lit('2025-06-01').cast('date'))
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



def load_accounts(spark: SparkSession, table_name: str)-> DataFrame:
    """
    Load the Credit Staement Balance Table from Hive once.

    Returns
    -------
    DataFrame
        Raw link table with  SHADOW_ACCOUNT_NBR, Opening_date, Closing date.
    """    
    
    df_sh = spark.table(table_name).filter(F.col("is_current") == "1")
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

def load_accounts_link_table(spark: SparkSession) -> DataFrame:
    """
    Load the raw link table from Hive once.
    
    Returns
    -------
    DataFrame
        Raw link table with CARD_NUMBER, SHADOW_ACCOUNT_NBR, OPENING_DATE.
    """
    return (
        spark.table("staging_zone.hpsv3_accounts_link")
        .select(
            F.col("ENTITY_ID").alias("CARD_NUMBER"),
            F.col("ACCOUNT_NUMBER").alias("SHADOW_ACCOUNT_NBR"),
            F.col("OPENING_DATE")
        )
        .distinct()
    )
 

def get_account_link_view(df_link: DataFrame, view_type: str) -> DataFrame:
    """
    Returns the desired view of the link table: either 'mapping' or 'dimension'.
    
    Parameters
    ----------
    df_link : DataFrame
        Raw account link DataFrame.
    view_type : str
        'mapping' for CARD_NUMBER → SHADOW_ACCOUNT_NBR mapping,
        'dimension' for SHADOW_ACCOUNT_NBR → OPENING_DATE dimension.
 
    Returns
    -------
    DataFrame
        Transformed view of the link table.
    """
    view_type = view_type.lower()
    
    if view_type == "mapping":
        return (
            df_link
            .select("CARD_NUMBER", "SHADOW_ACCOUNT_NBR")
            .dropDuplicates(["CARD_NUMBER", "SHADOW_ACCOUNT_NBR"])
        )
    elif view_type == "dimension":
        return (
            df_link
            .select("SHADOW_ACCOUNT_NBR", "OPENING_DATE")
            .dropDuplicates(["SHADOW_ACCOUNT_NBR"])
        )
    else:
        raise ValueError("Invalid view_type. Must be 'mapping' or 'dimension'.")
 