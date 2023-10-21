import json
import requests
import structlog


logger = structlog.get_logger()

class RequestHandler:

	def __init__(self):
		pass

	@staticmethod
	def post(address, data, trace_id):
		try:
			logger.info(f"Posting data to {address}...")
			response = requests.post(
				url=address,
				headers= {
				"Content-Type": "application/json",
				"trace": trace_id
				},
				data=data
			)
			logger.info("POST successful")
			return response
		except Exception as exc:
			logger.error(f"POST to {address} failed")
			raise exc
