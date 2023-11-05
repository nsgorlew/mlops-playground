from fastapi import Request, FastAPI, Response, HTTPException
from inference import ModelInference
from utilities.encoding import NumPyEncoder
# from utilities.persistence import Persist
# from utilities.request_handling import RequestHandler
import json
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


@app.post("/monitor")
async def invoke(request: Request):
    # clear the context variables for each request
    clear_contextvars()

    # get trace id and json data from request
    trace = request.headers.get("trace")
    data = await request.json()

    bind_contextvars(traceID=trace)

    try:
        # TODO: aggregate data in the data lake (will need a baseline also)
        result = ModelInference().predict(model_file="../diabetes_model_1_0_0.md", data=data)
        return Response(status_code=200, content=json.dumps(result, cls=NumPyEncoder))
    except KeyError as ke:
        logger.exception(f"Key {ke} not in request body")
        raise HTTPException(status_code=400, detail=f"{str(ke)} not in request body")


@app.get("/ping")
async def check():
    return "Monitoring Service is up!"
