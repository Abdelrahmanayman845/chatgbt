import sys
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

import warnings
warnings.filterwarnings("ignore")
sys.path.append("/home/cdsw")

from Development_Chadi.pipelines.Data_Preparation import DataPreparation
from Development_Chadi.pipelines.Stopper_Pipeline import StopperPipeline
from Development_Chadi.pipelines.aggregation import Aggregation
from Development_Chadi.utils.helpers import *

def main():
    spark = create_spark()
    # aggregation = Aggregation(spark)
#   pipeline = DataPreparation(spark)  
    stopper = StopperPipeline(spark)
    
    # aggregation.run()
#    pipeline.run()
    stopper.run()
    spark.stop()
    
    
if __name__ == "__main__":
    main()
  
    
