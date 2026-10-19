import warnings
import  pickle
import re
import os
import sys
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from __future__ import print_function

import pyspark.pandas as ps
from pyspark.sql.functions import col
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, HiveContext ,Row, Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, FloatType, StringType, ArrayType, IntegerType
from pyspark.sql import Row

from dateutil.relativedelta import relativedelta
import datetime as dt
from datetime import datetime ,date
from dateutil.relativedelta import relativedelta

from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import load_model


warnings.filterwarnings('ignore')

spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .config("spark.sql.parquet.datetimeRebaseModeInWrite","LEGACY")\
    .config("spark.sql.legacy.timeParserPolicy","LEGACY")\
    .config("spark.sql.execution.arrow.enabled", "true")\
    .getOrCreate()


df1 = spark.table("analytics_zone_retail_cross_selling.ch3_churn_final_view_2026_04_05_cdc")
df1 = df1.withColumn('balance_and_inactivity_p_counter',

                                     F.when((F.col('trx_count') == 0) & (F.col('utilized_rate') >= 0.95), 0)

                                     .otherwise(F.col('balance_and_inactivity_p_counter')))
## lag sequence 7 months to predict present

## opening   date >> 3shan myb2ash 3ndna null  for custmers recennt opening 
## Closing to filter customers not closed their CC 
deviations = df1.filter(((F.col('period_date')>='2025-05-01')&(F.col('period_date')<='2026-03-01'))&(F.col('opening_date')<'2026-03-01')
                       &((F.col('closing_date')>'2026-03-01')|(F.col('closing_date').isNull())))\
.select('SHADOW_ACCOUNT_NBR', 'period_date', 'RD_from_last_gap_balance').orderBy('SHADOW_ACCOUNT_NBR', 'period_date') .orderBy('SHADOW_ACCOUNT_NBR', 'period_date') 
colname = "RD_from_last_gap_balance"

id_col = "SHADOW_ACCOUNT_NBR"
 
# Step 1: count nulls per id

null_counts = (

    deviations.groupBy(id_col)

      .agg(F.sum(F.when(F.col(colname).isNull(), 1).otherwise(0)).alias("null_count"))

)
 
# Step 2: keep only ids with null_count <= 20

valid_ids = null_counts.filter(F.col("null_count") <=8).select(id_col)
 
# Step 3: join back to original df to keep only valid ids

df_filtered = deviations.join(valid_ids, on=id_col, how="inner")

df_filtered = df_filtered.orderBy('SHADOW_ACCOUNT_NBR', 'period_date')


df_filtered = df_filtered.toPandas()


# Display the result
# df_filtered.head()

SEQ_LEN = 7

PRED = 1   # predict 1 step ahead



df_filtered['RD_from_last_gap_balance'] = df_filtered['RD_from_last_gap_balance'].clip(lower=-5, upper=3.5)

df_filtered = df_filtered.sort_values(by=['SHADOW_ACCOUNT_NBR', 'period_date'])
 

 grouped = df_filtered.groupby('SHADOW_ACCOUNT_NBR')[['RD_from_last_gap_balance']]\
.apply(lambda df: df.values.tolist())
 
X_samples, y_samples, clients_of = [], [], []
 
for client, series in grouped.items():

    series = np.asarray(series, dtype=float)  # shape (L, 3)

    L = len(series)

    n_windows = L - SEQ_LEN - (PRED - 1)

    if n_windows <= 0:

        continue

    for start in range(n_windows):

        X_samples.append(series[start:start+SEQ_LEN])  # (18, 3)


        y_samples.append(series[start+SEQ_LEN : start+SEQ_LEN + PRED, 0])  # Prediciton only feature 0, which is the deviation

        clients_of.append(client)
 
print(len(y_samples))
print(len(X_samples))
X = np.stack(X_samples)  # shape (N, 18, 3)

y = np.array(y_samples)  # shape (N,)

clients = np.array(clients_of)
print(X.shape , y.shape  , clients.shape)


SENTINEL = 5
 
# Step 1: replace NaNs with sentinel BEFORE scaling
X = np.nan_to_num(X, nan=SENTINEL)

mask = ~np.isnan(y).any(axis=1)

X[mask], y[mask], clients[mask]
scaler = pickle.load(open("/home/cdsw/_ayman/dormancy/Modeling/LSTM/model/scaler.pkl", "rb"))


X = X.copy()
mask = X[:, :, 0] != SENTINEL
X[:, :, 0][mask] = scaler.transform(
   X[:, :, 0][mask].reshape(-1, 1)
).reshape(-1)


def mse_mae_loss(y_true, y_pred):
   return 0.5 * K.mean(K.square(y_true - y_pred)) + \
          0.5 * K.mean(K.abs(y_true - y_pred))

model = load_model(
   "/home/cdsw/_ayman/dormancy/Modeling/LSTM/model/smallseq2_31_03_2026",
   custom_objects={'mse_mae_loss': mse_mae_loss}
)


y_pred = model.predict(X)

print("Len y_pred" ,  len(y_pred))

results = pd.DataFrame({
   "shadow_account": clients,
   "y_true": y.flatten(),
   "y_pred": y_pred.flatten()
})


results.to_csv("/home/cdsw/_ayman/dormancy/Modeling/LSTM/Results/Testing/april_model_op.csv")