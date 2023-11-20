from preprocessing.preprocessor import Preprocessor
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import recall_score, accuracy_score, precision_score, confusion_matrix, ConfusionMatrixDisplay
# from sklearn.model_selection import RandomizedSearchCV
import joblib
import matplotlib.pyplot as plt
import structlog


logger = structlog.get_logger()

X_train, X_test, y_train, y_test = Preprocessor.run(model_version="1_0_0")

# model = KNeighborsClassifier(algorithm='brute', n_neighbors=4, weights='distance')
model = RandomForestClassifier(n_estimators=200, max_depth=20)
model.fit(X_train, y_train.values.ravel())

# predict and score model
y_pred = model.predict(X_test)


# hyperparameter tuning
"""
param_grid = {
    'n_neighbors': [4, 5, 6, 7, 8, 9, 10],
    'weights': ['uniform', 'distance'],
    'algorithm': ['auto', 'ball_tree', 'kd_tree', 'brute']
}
grid_search = RandomizedSearchCV(KNeighborsClassifier(),
                                 param_distributions=param_grid)
grid_search.fit(X_train, y_train.values.ravel())
logger.info(f"best estimator: {grid_search.best_estimator_}")
logger.info(f"best hyperparameters: {grid_search.best_params_}")
"""

cm = confusion_matrix(y_test, y_pred, labels=[0, 1, 2])
recall_loaded_model = recall_score(y_test, y_pred, labels=[0, 1, 2], average='macro')
accuracy_loaded_model = accuracy_score(y_test, y_pred)
precision_loaded_model = precision_score(y_test, y_pred, labels=[0, 1, 2], average='macro')

logger.info(f"Recall: {recall_loaded_model}")
logger.info(f"Accuracy: {accuracy_loaded_model}")
logger.info(f"Precision: {precision_loaded_model}")

# persist model
model_json = joblib.dump(model, 'diabetes_classifier_1_0_0.sav')

logger.info("Model Persisted")
disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=[0, 1, 2])
disp.plot()
plt.show()
