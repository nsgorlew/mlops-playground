from catboost import CatBoostClassifier
from preprocessing.preprocessor import Preprocessor
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score, recall_score
from ucimlrepo import fetch_ucirepo
import polars as pl
import structlog
import os

# data prep  
cdc_diabetes_health_indicators = fetch_ucirepo(id=891) 
  
# data (as pandas dataframes) 
X = cdc_diabetes_health_indicators.data.features 
y = cdc_diabetes_health_indicators.data.targets

# df = cleaned.slice(0,200000).sample(n=200000, shuffle=True)


# split into train and test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = CatBoostClassifier(iterations=5000, learning_rate=0.01)
model.fit(X_train.to_numpy(), y_train.to_numpy(), verbose=True)

preds_class = model.predict(X_test.to_numpy())

results = recall_score(y_test.to_numpy(), preds_class)
print(results)
