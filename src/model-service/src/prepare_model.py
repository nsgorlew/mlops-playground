from preprocessing.preprocessor import Preprocessor
from sklearn.metrics import roc_auc_score
import xgboost as xgb
import structlog

logger = structlog.get_logger()

X_train, X_test, y_train, y_test = Preprocessor.run()
dtrain = xgb.DMatrix(X_train, label=y_train)
deval = xgb.DMatrix(X_test, label=y_test)

params = {
    'alpha': 0.5,
    'objective': 'binary:logistic',
    'max_depth': 10,
    'learning_rate': 0.05,
}

num_round = 10
model = xgb.train(params, dtrain)

y_pred = model.predict(deval)
auc_loaded_model = roc_auc_score(y_test, y_pred)
logger.info(f"AUC: {auc_loaded_model}")

# persist model
model_json = model.save_model('diabetes_model_1_0_0.json')

logger.info("Model Persisted")