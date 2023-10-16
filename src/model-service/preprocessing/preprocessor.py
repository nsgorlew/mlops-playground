from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from ucimlrepo import fetch_ucirepo

import polars as pl
import structlog

class Preprocessor:
    
    def __init__(self):
        pass

    @staticmethod
    def run():
        cdc_diabetes_health_indicators = fetch_ucirepo(id=891) 
          
        X = cdc_diabetes_health_indicators.data.features 
        y = cdc_diabetes_health_indicators.data.targets

        # df = cleaned.slice(0,200000).sample(n=200000, shuffle=True)
        # split into train and test
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        return X_train, X_test, y_train, y_test

"""
    def run(self, data):
        data = Preprocessor.encode_labels(data)
        prepared_data = Preprocessor.hash_addresses(data)
        return prepared_data


    @staticmethod
    def encode_labels(data):
        # anything that isn't 'white' needs to be encoded as 1, it signifies ransomware
        # 'white' needs to be encoded as 0, it signifies non-ransomware
        data = data.with_columns(
            pl.when(pl.col("label") == "white")
            .then(pl.lit(0))
            .otherwise(pl.lit(1))
            .alias("label")
            )
        return data

    @staticmethod
    def hash_addresses(data):
        data = data.with_columns(
            pl.col("address").hash(10)
            )
        return data
"""