from sklearn.preprocessing import LabelEncoder

import polars as pl
import structlog

class Preprocessor:
    
    def __init__(self):
        pass

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
