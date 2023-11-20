# need sys to use in windows
import sys
sys.path.insert(0, 'C:\GitHub\mlops-playground\src\model-service\src')

from fastapi import Request, FastAPI, Response, HTTPException
from fastapi.responses import FileResponse
from inference import ModelInference
from utilities.encoding import NumPyEncoder
# from utilities.persistence import Persist
# from utilities.request_handling import RequestHandler
import json
import os
import pandas as pd
import structlog
from structlog.contextvars import (
    bind_contextvars,
    clear_contextvars,
    merge_contextvars,
)

from structlog import configure

app = FastAPI()

configure(processors=[merge_contextvars, structlog.processors.JSONRenderer()])
logger = structlog.get_logger()


@app.post("/model")
async def invoke(request: Request):
    # clear the context variables for each request
    clear_contextvars()

    # get trace id and json data from request
    trace = request.headers.get("trace")
    req_data = await request.json()

    bind_contextvars(traceID=trace)

    try:
        result = ModelInference().predict(model_file="diabetes_classifier_1_0_0.sav", data=req_data)
        return Response(status_code=200, content=json.dumps(result, cls=NumPyEncoder))

    except KeyError as ke:
        logger.exception(f"Key {ke} not in request body")
        raise HTTPException(status_code=400, detail=f"{str(ke)} not in request body")


@app.post("/model/get-cleaned-data")
async def invoke(request: Request):
    try:

        data = await request.json()

        filename = data["filename"]
        return FileResponse(f"C:/GitHub/mlops-playground/src/model-service/src/preprocessing/output/{filename}")

    except Exception as exc:
        logger.exception(f"Could not process request")
        raise HTTPException(status_code=500, detail=f"Request could not be processed: {exc}")


@app.get("/ping")
async def check():
    return "Ingestion Service is up!"
