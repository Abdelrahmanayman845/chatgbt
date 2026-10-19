from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, month, year, datediff

    
# Initialize Spark Session
spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .config("spark.sql.parquet.datetimeRebaseModeInWrite","LEGACY")\
    .config("spark.sql.legacy.timeParserPolicy","LEGACY")\
    .config("spark.sql.execution.arrow.enabled", "true")\
    .getOrCreate()
spark.sql("use staging_zone")

#load data 
df_balances = spark.table("staging_zone.hpsv3_cr_term_hist")

#select columns 
df_balances = df_balances.withColumn("Remaining_Balance", F.col("credit_limit") - F.col("closing_balance")) \
                         .withColumn("year", F.year(F.col("statement_date"))) \
                         .withColumn("month", F.month(F.col("statement_date")))
 
df_balances = df_balances.select(
    F.col("shadow_account_nbr"),
    F.col("client_code").alias("client_code"),
    F.col("statement_date").alias("statement_date"),
    F.col("year").alias("year"),
    F.col("month").alias("month"),
    F.col("credit_limit").alias("credit_limit"),
    F.col("OPENING_BALANCE").alias("opening_balance"),
    F.col("closing_balance").alias("closing_balance"),
    F.col("Remaining_Balance").alias("remaining_balance"),
    F.col("CURRENT_BALANCE_PURCHASE").alias("current_balance_purchase"),
    F.col("CURRENT_BALANCE_CASH").alias("current_balance_cash"),
    F.col("CURRENT_BALANCE_TRANSFER").alias("current_balance_transfer"),
    F.col("CURRENT_BALANCE_ECOM").alias("current_balance_ecom"),
    F.col("CURRENT_BALANCE_CHEQUE").alias("current_balance_cheque"),
    F.col("CURRENT_BALANCE_INTEREST").alias("current_balance_interest"),
    F.col("CURRENT_BALANCE_INSTALLMENT").alias("current_balance_installment"),
    F.col("CURRENT_BALANCE_FEES").alias("current_balance_fees"),
    F.col("CURRENT_BALANCE_DEPOSIT").alias("current_balance_deposit")
)

#utilized openning and closing limit 
df_balances= df_balances.withColumn("utilized_opening_Limit" , round (col("opening_balance") / (col("credit_limit") + 1 ),3 ))
df_balances= df_balances.withColumn("utilized_closing_Limit" ,round (col("closing_balance") / (col("credit_limit") + 1 ), 3 ))

#get the last 3 months (lags)
w_lag = Window.partitionBy("shadow_account_nbr").orderBy("year" , "month")

df_balances = df_balances.withColumn("m1_interest" , lag("current_balance_interest" ,1).over(w_lag))
df_balances = df_balances.withColumn("m2_interest" , lag ("current_balance_interest" , 2).over(w_lag))



#segmentation 

cl_filtered = df_balances.filter(col("credit_limit").isNotNull())

balances = cl_filtered.withColumn(
    "credit_limit_segment",
    when((col("credit_limit") >= 1000) & (col("credit_limit") <= 5000), "1k-5k")
    .when((col("credit_limit") > 5000) & (col("credit_limit") <= 50000), "5k-50k")
    .when((col("credit_limit") > 50000) & (col("credit_limit") <= 100000), "50k-100k")
    .when((col("credit_limit") > 100000) & (col("credit_limit") <= 500000), "100k-500k")
    .when((col("credit_limit") > 500000) & (col("credit_limit") <= 2000000), " 500k+")
    .otherwise("others")
)

############################################### Conditions ##############################################

balances = balances.withColumn(

    "client_status",
    when(
        (col("current_balance_interest") == 0) &
        (col("m1_interest") == 0) &
        (col("m2_interest") == 0) &
        (col("current_balance_purchase") == 0),
        "inactive"
    ).when(
        (col("current_balance_interest") == 0) &
        (col("m1_interest") == 0) &
        (col("m2_interest") == 0) &
        (col("current_balance_purchase") > 0),
        "consistent transactor"
    ).when(

        (col("credit_limit_segment") == "1k-5k") &
        (col("current_balance_interest") >= 10) &
        (col("m1_interest") >= 10) &
        (col("m2_interest") >= 10),
        "consistent revolver"

    ).when(

        (col("credit_limit_segment") == "1k-5k") &
        (col("current_balance_interest") < 10) &
        (col("m1_interest") >= 10) &
        (col("m2_interest") >= 10),
        "potential revolver"

    ).when(

        (col("credit_limit_segment") == "1k-5k") &
        (col("current_balance_interest") >= 10) &
        (col("m1_interest") >= 10) &
        (col("m2_interest") < 10),

        "potential revolver"

    ).when(

        (col("credit_limit_segment") == "1k-5k") &
        (col("current_balance_interest") < 10) &
        (col("m1_interest") < 10) &
        (col("m2_interest") < 10),

        "incidental revolver"

    ).when(
    (col("credit_limit_segment") == "1k-5k") &
    (col("current_balance_interest") < 10) &
    (col("m1_interest") < 10) &
    (col("m2_interest") < 10) &
    (col("current_balance_purchase") == 0) ,
        "incidental revolver"
        
    
    
    ).when(

        (col("credit_limit_segment") == "1k-5k") &
        (col("current_balance_interest") < 10) &
        (col("m1_interest") < 10) &
        (col("m2_interest") >= 10) ,
        "incidental revolver"

    ).when(

        (col("credit_limit_segment") == "1k-5k") &
        (col("current_balance_interest") >= 10) &
        (col("m1_interest") < 10) &
        (col("m2_interest") < 10) ,

        "incidental revolver"
        
    #5k-50k-------------------------------
    ).when(

        (col("credit_limit_segment") == "5k-50k") &
        (col("current_balance_interest") >= 30) &
        (col("m1_interest") >= 30) &
        (col("m2_interest") >= 30),
        "consistent revolver"

    ).when(

        (col("credit_limit_segment") == "5k-50k") &
        (col("current_balance_interest") < 30) &
        (col("m1_interest") >= 30) &
        (col("m2_interest") >= 30),
        "potential revolver"

    ).when(

        (col("credit_limit_segment") == "5k-50k") &
        (col("current_balance_interest") >= 30) &
        (col("m1_interest") >= 30) &
        (col("m2_interest") < 30),

        "potential revolver"

     ).when(
    (col("credit_limit_segment") == "5k-50k") &
    (col("current_balance_interest") < 30) &
    (col("m1_interest") < 30) &
    (col("m2_interest") < 30) &
    (col("current_balance_purchase") == 0) ,
        "incidental revolver"
        
    ).when(

        (col("credit_limit_segment") == "5k-50k") &
        (col("current_balance_interest") < 30) &
        (col("m1_interest") < 30) &
        (col("m2_interest") < 30) ,

        "incidental revolver"

    ).when(

        (col("credit_limit_segment") == "5k-50k") &
        (col("current_balance_interest") < 30) &
        (col("m1_interest") < 30) &
        (col("m2_interest") >= 30) ,
        "incidental revolver"

    ).when(

        (col("credit_limit_segment") == "5k-50k") &
        (col("current_balance_interest") >= 30) &
        (col("m1_interest") < 30) &
        (col("m2_interest") < 30) ,

        "incidental revolver"
        
    #50k-100k-------------------------------------------------------->>
    ).when(

        (col("credit_limit_segment") == "50k-100k") &
        (col("current_balance_interest") >= 200) &
        (col("m1_interest") >= 200) &
        (col("m2_interest") >= 200),
        "consistent revolver"

    ).when(

        (col("credit_limit_segment") == "50k-100k") &
        (col("current_balance_interest") < 200) &
        (col("m1_interest") >= 200) &
        (col("m2_interest") >= 200),
        "potential revolver"

    ).when(

        (col("credit_limit_segment") == "50k-100k") &
        (col("current_balance_interest") >= 200) &
        (col("m1_interest") >= 200) &
        (col("m2_interest") < 200),

        "potential revolver"

    ).when(
    (col("credit_limit_segment") == "50k-100k") &
    (col("current_balance_interest") < 200) &
    (col("m1_interest") < 200) &
    (col("m2_interest") < 200) &
    (col("current_balance_purchase") == 0) ,
        "incidental revolver"
        
    ).when(

        (col("credit_limit_segment") == "50k-100k") &
        (col("current_balance_interest") < 200) &
        (col("m1_interest") < 200) &
        (col("m2_interest") < 200) ,

        "incidental revolver"

    ).when(

        (col("credit_limit_segment") == "50k-100k") &
        (col("current_balance_interest") < 200) &
        (col("m1_interest") < 200) &
        (col("m2_interest") >= 200) ,
        "incidental revolver"

    ).when(

        (col("credit_limit_segment") == "50k-100k") &
        (col("current_balance_interest") >= 200) &
        (col("m1_interest") < 200) &
        (col("m2_interest") < 200) ,

        "incidental revolver"
        
    #100k-500k---------------------------------------------------------------->>
    ).when(

        (col("credit_limit_segment") == "100k-500k") &
        (col("current_balance_interest") > 500) &
        (col("m1_interest") > 500) &
        (col("m2_interest") > 500),
        "consistent revolver"

    ).when(

        (col("credit_limit_segment") == "100k-500k") &
        (col("current_balance_interest") < 500) &
        (col("m1_interest") > 500) &
        (col("m2_interest") > 500),
        "potential revolver"

    ).when(

        (col("credit_limit_segment") == "100k-500k") &
        (col("current_balance_interest") > 500) &
        (col("m1_interest") > 500) &
        (col("m2_interest") < 500),

        "potential revolver"

    ).when(
    (col("credit_limit_segment") == "100k-500k") &
    (col("current_balance_interest") < 500) &
    (col("m1_interest") < 500) &
    (col("m2_interest") < 500) &
    (col("current_balance_purchase") == 0) ,
        "incidental revolver"
    ).when(

        (col("credit_limit_segment") == "100k-500k") &
        (col("current_balance_interest") < 500) &
        (col("m1_interest") < 500) &
        (col("m2_interest") < 500) ,

        "incidental revolver"

    ).when(

        (col("credit_limit_segment") == "100k-500k") &
        (col("current_balance_interest") < 500) &
        (col("m1_interest") < 500) &
        (col("m2_interest") > 500) ,
        "incidental revolver"

    ).when(

        (col("credit_limit_segment") == "100k-500k") &
        (col("current_balance_interest") > 500) &
        (col("m1_interest") < 500) &
        (col("m2_interest") < 500) ,

        "incidental revolver"
        #500k+---------------------------------------------------------------->>
    ).when(

        (col("credit_limit_segment") == "500k+") &
        (col("current_balance_interest") > 500) &
        (col("m1_interest") > 500) &
        (col("m2_interest") > 500),
        "consistent revolver"

    ).when(

        (col("credit_limit_segment") == "500k+") &
        (col("current_balance_interest") < 500) &
        (col("m1_interest") > 500) &
        (col("m2_interest") > 500),
        "potential revolver"

    ).when(

        (col("credit_limit_segment") == "500k+") &
        (col("current_balance_interest") > 500) &
        (col("m1_interest") > 500) &
        (col("m2_interest") < 500),

        "potential revolver"

    ).when(
    (col("credit_limit_segment") == "500k+") &
    (col("current_balance_interest") < 500) &
    (col("m1_interest") < 500) &
    (col("m2_interest") < 500) &
    (col("current_balance_purchase") == 0) ,
        "incidental revolver"
    ).when(

        (col("credit_limit_segment") == "500k+") &
        (col("current_balance_interest") < 500) &
        (col("m1_interest") < 500) &
        (col("m2_interest") < 500) ,

        "incidental revolver"

    ).when(

        (col("credit_limit_segment") == "500k+") &
        (col("current_balance_interest") < 500) &
        (col("m1_interest") < 500) &
        (col("m2_interest") > 500) ,
        "incidental revolver"

    ).when(

        (col("credit_limit_segment") == "500k+") &
        (col("current_balance_interest") > 500) &
        (col("m1_interest") < 500) &
        (col("m2_interest") < 500) ,

        "incidental revolver"
        
     #others------------------------------------------------------------------>>
     ).when(

        (col("credit_limit_segment") == "others") &
        (col("current_balance_interest") >= 10) &
        (col("m1_interest") >= 10) &
        (col("m2_interest") >= 10),
        "consistent revolver"

    ).when(

        (col("credit_limit_segment") == "others") &
        (col("current_balance_interest") < 10) &
        (col("m1_interest") >= 10) &
        (col("m2_interest") >= 10),
        "potential revolver"

    ).when(

        (col("credit_limit_segment") == "others") &
        (col("current_balance_interest") >= 10) &
        (col("m1_interest") >= 10) &
        (col("m2_interest") < 10),

        "potential revolver"

    ).when(
    (col("credit_limit_segment") == "others") &
    (col("current_balance_interest") < 10) &
    (col("m1_interest") < 10) &
    (col("m2_interest") < 10) &
    (col("current_balance_purchase") == 0) ,
        "incidental revolver"
        
    ).when(

        (col("credit_limit_segment") == "others") &
        (col("current_balance_interest") < 10) &
        (col("m1_interest") < 10) &
        (col("m2_interest") < 10),

        "incidental revolver"

    ).when(

        (col("credit_limit_segment") == "others") &
        (col("current_balance_interest") < 10) &
        (col("m1_interest") < 10) &
        (col("m2_interest") >= 10) ,
        "incidental revolver"

    ).when(

        (col("credit_limit_segment") == "others") &
        (col("current_balance_interest") >= 10) &
        (col("m1_interest") < 10) &
        (col("m2_interest") < 10) ,

        "incidental revolver"    
    ).otherwise("inconsistent")
)

''' Active shadow numbers 

520092679126      
520039394623      
666190361954
666037771712      
204024914729      
204024917524      
204024917727      
520092679425      
666038067110      


'''