from pyspark.sql import SparkSession
from Development_Chadi.src.ch8_modeling_data_prep import *


spark = SparkSession\
    .builder\
    .appName("preprocessing")\
    .config("spark.sql.parquet.datetimeRebaseModeInWrite","LEGACY")\
    .config("spark.sql.legacy.timeParserPolicy","LEGACY")\
    .config("spark.sql.execution.arrow.enabled", "true")\
    .getOrCreate()

SAVE_PATH = "/home/cdsw/_ayman/dormancy/data/"

df = load_data(spark)
df = prepare_features(df)
df = filter_clients(df)

df_pd = df.toPandas()

X, y, clients = build_sequences(df_pd)

X_train, y_train, X_val, y_val, X_test, y_test = split_data(X, y, clients)

X_train, X_val, X_test, scaler = scale_data(X_train, X_val, X_test)

# SAVE EVERYTHING
np.save(SAVE_PATH + "X_train.npy", X_train)
np.save(SAVE_PATH + "X_val.npy", X_val)
np.save(SAVE_PATH + "X_test.npy", X_test)

np.save(SAVE_PATH + "y_train.npy", y_train)
np.save(SAVE_PATH + "y_val.npy", y_val)
np.save(SAVE_PATH + "y_test.npy", y_test)

with open(SAVE_PATH + "scaler.pkl", "wb") as f:
    pickle.dump(scaler, f)

print(" Preprocessing done & saved")