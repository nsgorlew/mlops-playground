import datetime
import joblib
import os
import pandas as pd
import requests

# need sys to use in windows
import sys
sys.path.insert(0, 'C:\GitHub\mlops-playground\src\monitoring-service\src')
from monitoring.monitor import Monitor
from utilities.duck_connector import DuckConnector


def train_model(X_train, y_train):
    trained_model = Monitor.train_isolation_forest(X_train, y_train)
    return trained_model


def save_model(model):
    joblib.dump(model, f"{os.getcwd()}/monitoring/isolation_forest_1_1_0.sav")
    return True


# request data from model-service
def post_request_data(service_url, data_filename):
    resp = requests.post(url=service_url,
                         data={"filename": data_filename}
                         )
    return resp


if __name__ == "__main__":
    print(os.getcwd())
    df_train = pd.read_json(path_or_buf=r"C:\GitHub\mlops-playground\src\monitoring-service\src\monitoring\data\training_data_1_0_0.jsonl",
                            lines=True,
                            encoding='utf-8-sig'
                            )
    df_test = pd.read_json(path_or_buf=r"C:\GitHub\mlops-playground\src\monitoring-service\src\monitoring\data\testing_data_1_0_0.jsonl",
                           lines=True,
                           encoding='utf-8-sig'
                           )

    X_df_train = df_train.iloc[:, 0:-1]
    y_df_train = df_train.iloc[:, -1]

    model = train_model(X_df_train, y_df_train)

    save_model(model=model)
