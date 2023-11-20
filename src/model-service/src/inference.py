from preprocessing.preprocessor import Preprocessor
from utilities.persistence import Persist
import joblib
import os
import structlog

logger = structlog.get_logger()


class ModelInference:

	def __init__(self):
		self.features = [
						"highBP",
						"highChol",
						"cholCheck",
						"bmi",
						"smoker",
						"stroke",
						"heartDiseaseOrAttack",
						"physicalActivity",
						"fruits",
						"veggies",
						"heavyAlcoholConsumption",
						"anyHealthCare",
						"noDocBecauseOfCost",
						"generalHealthSelfAssessment",
						"mentalHealthIssues",
						"physicalHealthIssues",
						"difficultyWalking",
						"sex",
						"age",
						"education",
						"income"
						]

	@staticmethod
	def predict(model_file, data):
		try:
			model = ModelInference.load_model(f"{os.getcwd()}/{model_file}")
			cleaned_data = Preprocessor.clean_for_inference(data_dict=data, scalers_dir="C:/GitHub/mlops-playground/src/model-service/src/utilities/scalers")
			prediction = model.predict(cleaned_data)
			# log_prediction = model.predict_log_proba(cleaned_data)
			data["prediction"] = prediction.tolist()
			# data["log_prob"] = log_prediction.tolist()

			logger.info("Persisting data...")
			persistence_result = Persist.push(data, local=True)
			if not persistence_result:
				raise Exception

			return {"prediction": data["prediction"]}
		except Exception as ex:
			logger.exception(str(ex))
			raise ex

	@staticmethod
	def load_model(model_path):
		loaded_model = joblib.load(model_path)
		return loaded_model
