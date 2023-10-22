# from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from ucimlrepo import fetch_ucirepo

# import polars as pl
import pandas as pd


class Preprocessor:
    
    def __init__(self):
        pass

    @staticmethod
    def run():
        cdc_diabetes_health_indicators = fetch_ucirepo(id=891) 
          
        X = Preprocessor.convert_feature_names(cdc_diabetes_health_indicators.data.features)
        y = cdc_diabetes_health_indicators.data.targets

        print(X.head)
        # split into train and test
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        return X_train.to_numpy(), X_test.to_numpy(), y_train.to_numpy(), y_test.to_numpy()

    @staticmethod
    def convert_feature_names(training_x):
        # improve readability and make valid JSON
        feature_map = {
            "HighBP": "highBP",
            "HighChol": "highChol",
            "CholCheck": "cholCheck",
            "BMI": "bmi",
            "Smoker": "smoker",
            "Stroke": "stroke",
            "HeartDiseaseorAttack": "heartDiseaseOrAttack",
            "PhysActivity": "physicalActivity",
            "Fruits": "fruits",
            "Veggies": "veggies",
            "HvyAlcoholConsump": "heavyAlcoholConsumption",
            "AnyHealthcare": "anyHealthCare",
            "NoDocbcCost": "noDocBecauseOfCost",
            "GenHlth": "generalHealthSelfAssessment",
            "MentHlth": "mentalHealthIssues",
            "PhysHlth": "physicalHealthIssues",
            "DiffWalk": "difficultyWalking",
            "Sex": "sex",
            "Age": "age",
            "Education": "education",
            "Income": "income"
        }
        training_x.rename(columns=feature_map, inplace=True)
        return training_x

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
