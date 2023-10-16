from datetime import datetime, date
import json
import os

class Persist:

	def __init__(self):
		pass

	@staticmethod
	def push(data, local=True):
		persistence_path = f"{os.getcwd()}/{date.today()}"
		if local:
			if os.path.exists(persistence_path):
				with open(f"{persistence_path}/{data['appID']}_{datetime.now().strftime('%Y-%m-%d')}.json", "w") as f:
					json.dump(data, f)
			else:
				os.mkdir(persistence_path, 0o666)
				with open(f"{persistence_path}/{data['appID']}_{datetime.now().strftime('%Y-%m-%d')}.json", "w") as f:
					json.dump(data, f)
					
			return True
		else:
			# TODO: push to cloud data lake
			return True
