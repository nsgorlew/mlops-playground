from fastapi import Request, FastAPI
from pydantic import BaseModel
from utilities.persistence import Persist
import json
import structlog
from structlog.contextvars import (
    bind_contextvars,
    bound_contextvars,
    clear_contextvars,
    merge_contextvars,
    unbind_contextvars,
)

from structlog import configure

app = FastAPI()

configure(processors=[merge_contextvars, structlog.processors.JSONRenderer()])
logger = structlog.get_logger()


@app.post("/invoke")
async def invoke(request: Request):
    try:
    	# clear the context variables for each request
    	clear_contextvars()
    	# get trace id and json data from request
    	trace = request.headers.get("trace")
    	data = await request.json()

        # persist data in lake
    	bind_contextvars(traceID=trace)
    	logger.info("Persisting data...")
    	Persist.push(data, local=True)
    	# logger.info(f"data for {data['appID']} persisted")
    except Exception as exc:
    	logger.error("Persistence failed", exc_info=True)

    	return {"error": "fix"}
    # TODO: send data to model service
    return await request.json()

@app.get("/ping")
async def check():
	return "Ingestion Service is up!"