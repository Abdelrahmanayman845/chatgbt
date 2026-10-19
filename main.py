import sys
import warnings
import logging
import time
from pyspark.sql import SparkSession
import os
warnings.filterwarnings("ignore")
BASE_DIR = os.getcwd()
sys.path.insert(0, BASE_DIR + "/Development_Chadi")
print(os.getcwd())
from pipelines.ch2_Data_Preparation import DataPreparation
from pipelines.ch3_Stopper_Pipeline import StopperPipeline
from pipelines.ch1_aggregation import Aggregation
from helpers import create_spark

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(
   level=logging.INFO,
   format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------------------
# Run Pipelines Sequentially
# ---------------------------
def run_all_pipelines(spark: SparkSession):
   pipelines = [
      ("aggregation", Aggregation),
      ("data_preparation", DataPreparation),
      ("stopper", StopperPipeline)
   ]
   for name, pipeline_class in pipelines:
       start_time = time.time()
       try:
           logger.info(f"Starting pipeline: {name}")
           pipeline = pipeline_class(spark)
           pipeline.run()
           duration = time.time() - start_time
           logger.info(f"Finished {name} in {duration:.2f} seconds")
       except Exception as e:
           logger.error(f"Pipeline {name} failed: {str(e)}", exc_info=True)
           raise 
       finally:

           try:
               # Clear unused cached tables
               spark.catalog.clearCache()
               # Clear lineage 
               spark.sparkContext._jsc.sc().clearJobGroup()
               logger.info(f"Memory cleanup done after {name}")
           except Exception:
               logger.warning("Memory cleanup skipped")

# ---------------------------
# Main
# ---------------------------
def main():
   spark = None
   try:
       logger.info("Creating Spark session...")
       spark = create_spark()
       # Optional but VERY useful for long pipelines
       spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
       run_all_pipelines(spark)
   except Exception:
       logger.error("Application failed", exc_info=True)
   finally:
       if spark:
           logger.info("Stopping Spark session...")
           spark.stop()
            
if __name__ == "__main__":
   main()