from utilities.persistence import Persist
import json
import xgboost as xgb
import os
import numpy as np
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

	def predict(self, model_file, data):
		try: 
			model = ModelInference.load_model(f"{os.getcwd()}/{model_file}")
			d_inf = xgb.DMatrix(np.array([[int(data[feature]) for feature in self.features]]))
			prediction = model.predict(d_inf)
			data["prediction"] = int(prediction[0])

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
		booster = xgb.Booster()
		booster.load_model(model_path)
		return booster
