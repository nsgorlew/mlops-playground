from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler
from sklearn.decomposition import PCA
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import KBinsDiscretizer, MinMaxScaler
from utilities.persistence import Persist

import joblib
import numpy as np
import pandas as pd


class Preprocessor:
    
    def __init__(self):
        pass

    @staticmethod
    def run(model_version):
        X = pd.read_csv(Persist.pull(bucket=os.environ["S3_BUCKET"],
                                     key="data/diabetes_012_health_indicators_BRFSS2015.csv"
                                     )
                        )
        y = pd.read_csv(Persist.pull(bucket=os.environ["S3_BUCKET"],
                                     key="data/diabetes_012_health_indicators_BRFSS2015.csv"
                                     )
                        , usecols=["Diabetes_012"]
                        )

        X = Preprocessor.convert_feature_names(X)

        # sampling to improve class imbalance and increase instances
        X, y = Preprocessor.oversample(X, y)
        X, y = Preprocessor.undersample(X, y)

        X = Preprocessor.scale_columns(frame=X, columns=[
            "physicalHealthIssues",
            "age",
            "bmi",
            "mentalHealthIssues"
        ]
                                       )

        # drop correlated features
        X = Preprocessor.drop_extra_columns(frame=X,
                                            drop_columns=[
                                                "Diabetes_012"
                                            ]
                                            )

        # X = Preprocessor.reduce(frame=X, num_components=10)
        # split into train and test
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

        # persist processed data
        df_train = pd.concat([X_train, y_train], axis=1, join="inner")
        Persist.push_training_testing_data(bucket=os.environ["S3_BUCKET"],
                                           key=f"data/training_data_{model_version}.jsonl",
                                           frame=df_train
                                           )

        df_test = pd.concat([X_train, y_train], axis=1, join="inner")
        Persist.push_training_testing_data(bucket=os.environ["S3_BUCKET"],
                                           key=f"data/training_data_{model_version}.jsonl",
                                           frame=df_test
                                           )

        return X_train, X_test, y_train, y_test

    @staticmethod
    def clean_for_inference(data_dict, scalers_dir):
        frame = pd.DataFrame.from_dict([data_dict])

        frame = Preprocessor.scale_columns_inference(frame=frame, columns=[
            "physicalHealthIssues",
            "age",
            "bmi",
            "mentalHealthIssues"
        ],
                                                     scalers_path=scalers_dir
                                                     )

        # drop appID
        frame = Preprocessor.drop_extra_columns(frame=frame,
                                                drop_columns=[
                                                    "appID"
                                                ]
                                                )

        return frame

    @staticmethod
    def scale_columns_inference(frame, columns, scalers_path):
        for col in columns:
            scaler = joblib.load(f"{scalers_path}/{col}_scaler.sav")
            frame[col] = scaler.fit_transform([frame[col]])
        return frame

    @staticmethod
    def discretize(frame, num_bins):
        enc = KBinsDiscretizer(n_bins=num_bins, encode='ordinal', strategy='uniform')
        frame_binned = enc.fit_transform(frame)
        return frame_binned

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

    @staticmethod
    def drop_extra_columns(frame, drop_columns):
        frame.drop(columns=drop_columns, inplace=True)
        return frame

    @staticmethod
    def reduce(frame, num_components):
        pca = PCA(n_components=num_components)
        reduced = pca.fit_transform(frame)
        joblib.dump(reduced, f"utilities/scalers/pca.sav")
        return reduced

    @staticmethod
    def oversample(x_frame, y_frame):
        smote = SMOTE(sampling_strategy={0: 213703, 1: 150000, 2: 150000})
        x_frame, y_frame = smote.fit_resample(x_frame, y_frame)
        return x_frame, y_frame

    @staticmethod
    def undersample(x_frame, y_frame):
        under = RandomUnderSampler(sampling_strategy={0: 100000})
        x_frame, y_frame = under.fit_resample(x_frame, y_frame)
        return x_frame, y_frame

    @staticmethod
    def scale_columns(frame, columns):
        # frame = frame.reshape(-1, 1)
        for col in columns:
            scaler = MinMaxScaler()
            frame[col] = scaler.fit_transform(frame[col].values.reshape(-1, 1))
            joblib.dump(scaler, f"utilities/scalers/{col}_scaler.sav")
        return frame

    @staticmethod
    def bmi_to_discrete(frame):
        # 0 is not obese, 1 is obese
        conditions = {
            0: frame["bmi"] < 30,
            1: frame["bmi"] >= 30
        }
        frame["bmi"] = np.select(conditions.values(), conditions.keys(), default=frame["bmi"])
        frame["bmi"].astype("int64")
        return frame

    @staticmethod
    def mental_to_discrete(frame):
        # 0 signifies no mental health events
        conditions = {
            0: frame["mentalHealthIssues"] == 0,
            1: frame["mentalHealthIssues"] > 0
        }
        frame["mentalHealthIssues"] = np.select(conditions.values(), conditions.keys(), default=frame["mentalHealthIssues"])
        return frame

    @staticmethod
    def age_to_discrete(frame):
        # 0 signifies under 35
        conditions = {
            0: frame["age"] <= 3,
            1: frame["age"] > 3
        }
        frame["mentalHealthIssues"] = np.select(conditions.values(), conditions.keys(), default=frame["mentalHealthIssues"])
        return frame

    @staticmethod
    def health_to_discrete(frame):
        # 1 signifies good health self assessment
        conditions = {
            0: frame["generalHealthSelfAssessment"] < 3,
            1: frame["generalHealthSelfAssessment"] >= 3
        }
        frame["generalHealthSelfAssessment"] = np.select(conditions.values(), conditions.keys(), default=frame["generalHealthSelfAssessment"])
        return frame

    @staticmethod
    def recent_physical_health_problem(frame):
        # 1 signifies physical health problem in past 30 days
        conditions = {
            0: frame["physicalHealthIssues"] == 0,
            1: frame["physicalHealthIssues"] > 0
        }
        frame["physicalHealthIssues"] = np.select(conditions.values(), conditions.keys(), default=frame["physicalHealthIssues"])
        return frame

    @staticmethod
    def education_to_categorical(frame):
        # 1 signifies at least high school graduate or equivalent
        conditions = {
            0: frame["education"] < 4,
            1: frame["education"] >= 4
        }
        frame["education"] = np.select(conditions.values(), conditions.keys(), default=frame["education"])
        return frame

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
