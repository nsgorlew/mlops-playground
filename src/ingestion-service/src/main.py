from fastapi import Request, FastAPI, Response, HTTPException
from utilities.persistence import Persist

import json
import os
import requests
from jsonschema import validate
from jsonschema.exceptions import ValidationError
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
logger = structlog.get_logger(src="main.py")


@app.post("/ingestion/invoke")
async def invoke(request: Request):
    try:
        # clear the context variables for each request
        clear_contextvars()
        # get trace id and json data from request
        trace = request.headers.get("trace")
        data = await request.json()

        # validate the data against the schema validation
        try:
            schema = json.load(open("C:/GitHub/mlops-playground/src/ingestion-service/src/utilities/validation/validator-schema.json"))
            validate(instance=data, schema=schema)
        except ValidationError as ve:
            logger.error(f"{str(ve)}")
            raise ve

        # persist data in lake
        bind_contextvars(traceID=trace)

        try:
            logger.info("Persisting data...")
            persistence_result = Persist.push(data, local=True)
        except Exception as exc:
            raise exc
        if not persistence_result:
            raise Exception

        # downstream_response = RequestHandler.post()

    except Exception as exc:
        logger.error("Persistence failed", exc_info=True)

        return {"error": str(exc)}
    # TODO: send data to model service
    try:
        model_response = requests.post(
            url=os.environ["MODEL-SERVICE-URL"],
            data=data,
            headers=None
        )

        return Response(
            content=json.dumps(model_response),
            status_code=200,
            headers=None,
            media_type="application/json"
        )
    except Exception as exc:
        error = {"error details": str(exc)}
        return Response(
            content=json.dumps(error),
            status_code=500,
            media_type="application/json"
        )

    return await request.json()


@app.get("/ping")
async def check():
    return "Ingestion Service is up!"
