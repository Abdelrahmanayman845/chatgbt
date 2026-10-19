import numpy as np
import pickle
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, LSTM, Dense, Masking
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras import backend as K
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
 

DATA_PATH = "/home/cdsw/_ayman/dormancy/data/"
MODEL_PATH = "/home/cdsw/_ayman/dormancy/model/"
SENTINEL = 5
 

X_train = np.load(DATA_PATH + "X_train.npy")
X_val   = np.load(DATA_PATH + "X_val.npy")
X_test  = np.load(DATA_PATH + "X_test.npy")
 
y_train = np.load(DATA_PATH + "y_train.npy")
y_val   = np.load(DATA_PATH + "y_val.npy")
y_test  = np.load(DATA_PATH + "y_test.npy")
 

SEQ_LEN = X_train.shape[1]
N_FEATURES = X_train.shape[2]
 
inputs = Input(shape=(SEQ_LEN, N_FEATURES))
x = Masking(mask_value=SENTINEL)(inputs)
 
x = LSTM(128, return_sequences=True)(x)
x = LSTM(64, return_sequences=True)(x)
x = LSTM(32)(x)
 
outputs = Dense(1)(x)
 
model = Model(inputs, outputs)
 
def mse_mae_loss(y_true, y_pred):
    return 0.5*K.mean(K.square(y_true - y_pred)) + 0.5*K.mean(K.abs(y_true - y_pred))
 
model.compile(
    optimizer=Adam(1e-3),
    loss=mse_mae_loss,
    metrics=['mae', 'mse']
)
 

early_stop = EarlyStopping(patience=2, restore_best_weights=True)
 
history = model.fit(
    X_train, y_train,
    validation_data=(X_val, y_val),
    epochs=50,
    batch_size=32,
    callbacks=[early_stop],
    verbose=1
)
 

y_pred = model.predict(X_test)
 
print("MAE:", mean_absolute_error(y_test, y_pred))
print("MSE:", mean_squared_error(y_test, y_pred))
print("R2 :", r2_score(y_test, y_pred))
 

model.save(MODEL_PATH + "lstm_model")
print("Model saved")