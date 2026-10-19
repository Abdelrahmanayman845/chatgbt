import numpy as np
import pandas as pd
import pickle
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

SEQ_LEN = 7
PRED_LEN = 1
SENTINEL = 5
 
SAVE_PATH = "/home/cdsw/_ayman/dormancy/data/"
 

def load_data(spark):
    return spark.table("analytics_zone_retail_cross_selling.ch3_churn_final_view_2026_10_cdc")
 

def prepare_features(df):
    df = df.withColumn(
        'balance_and_inactivity_p_counter',
        F.when((F.col('trx_count') == 0) & (F.col('utilized_rate') >= 0.95), 0)
         .otherwise(F.col('balance_and_inactivity_p_counter'))
    )
 
    df = df.withColumn(
        'TENURE',
        F.when(
            F.months_between(col('period_date'), col('opening_date')) < 0,
            None
        ).otherwise(
            F.floor(F.months_between(col('period_date'), col('opening_date')))
        )
    )
 
    return df
 
 
def filter_clients(df):
 
    deviations = df.filter(
        ((F.col('period_date') >= '2022-06-01') &
         (F.col('period_date') < '2026-07-01')) &
        (F.col('opening_date') <= '2026-06-30')
    ).select(
        'SHADOW_ACCOUNT_NBR', 'period_date', 'RD_from_last_gap_balance'
    )
 
    null_counts = deviations.groupBy("SHADOW_ACCOUNT_NBR").agg(
        F.sum(F.when(F.col('RD_from_last_gap_balance').isNull(), 1).otherwise(0)).alias("null_count")
    )
 
    valid_ids = null_counts.filter(F.col("null_count") <= 5).select("SHADOW_ACCOUNT_NBR")
 
    return deviations.join(valid_ids, on="SHADOW_ACCOUNT_NBR")
 
 
def build_sequences(df_pd):
 
    df_pd['RD_from_last_gap_balance'] = df_pd['RD_from_last_gap_balance'].clip(-5, 3.5)
 
    grouped = df_pd.groupby('SHADOW_ACCOUNT_NBR')[['RD_from_last_gap_balance']] \
                   .apply(lambda x: x.values.tolist())
 
    X, y, clients = [], [], []
 
    for client, series in grouped.items():
        series = np.asarray(series, dtype=float)
        L = len(series)
 
        n = L - SEQ_LEN - (PRED_LEN - 1)
        if n <= 0:
            continue
 
        for i in range(n):
            X.append(series[i:i+SEQ_LEN])
            y.append(series[i+SEQ_LEN:i+SEQ_LEN+PRED_LEN, 0])
            clients.append(client)
 
    return np.array(X), np.array(y), np.array(clients)
 
 
def split_data(X, y, clients): ## Splitting based on time instead of randomized 
 
    unique_clients = np.unique(clients)
 
    train_c, temp_c = train_test_split(unique_clients, test_size=0.3, random_state=42)
    val_c, test_c = train_test_split(temp_c, test_size=0.5, random_state=42)
 
    def select(c):
        mask = np.isin(clients, c)
        return X[mask], y[mask]
 
    return (*select(train_c), *select(val_c), *select(test_c))
 
 
def scale_data(X_train, X_val, X_test):
 
    X_train = np.nan_to_num(X_train, nan=SENTINEL)
    X_val   = np.nan_to_num(X_val, nan=SENTINEL)
    X_test  = np.nan_to_num(X_test, nan=SENTINEL)
 
    mask = X_train[:,:,0] != SENTINEL
    scaler = StandardScaler()
    scaler.fit(X_train[:,:,0][mask].reshape(-1,1))
 
    def scale(X):
        Xc = X.copy()
        mask = Xc[:,:,0] != SENTINEL
        Xc[:,:,0][mask] = scaler.transform(
            Xc[:,:,0][mask].reshape(-1,1)
        ).reshape(-1)
        return Xc
 
    return scale(X_train), scale(X_val), scale(X_test), scaler
 
 

