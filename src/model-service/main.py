from fastapi import Request, FastAPI
from pydantic import BaseModel
from utilities.persistence import Persist
from utilities.request_handling import RequestHandler
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


@app.post("/model")
async def invoke(request: Request):
    try:
    	# clear the context variables for each request
    	clear_contextvars()
    	# get trace id and json data from request
    	trace = request.headers.get("trace")
    	data = await request.json()

    	bind_contextvars(traceID=trace)
    	
        
    except Exception as exc:
    	logger.error("Persistence failed", exc_info=True)

    	return {"error": "fix"}
    # TODO: send data to model service
    return await request.json()

@app.get("/ping")
async def check():
	return "Ingestion Service is up!"