from pyspark.sql import SparkSession

def create_spark():
    spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .config("spark.sql.parquet.datetimeRebaseModeInWrite","LEGACY")\
    .config("spark.sql.legacy.timeParserPolicy","LEGACY")\
    .config("spark.sql.execution.arrow.enabled", "true")\
    .getOrCreate()
    
    return spark