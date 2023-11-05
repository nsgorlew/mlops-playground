from fastapi import Request, FastAPI
# from pydantic import BaseModel, Field, ValidationError
from utilities.persistence import Persist
from utilities.request_handling import RequestHandler
import json
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
logger = structlog.get_logger()

"""
class RequestSchema(BaseModel):
    appID: int = Field(gt=0)
    highBP: bool
    highChol: bool
    bmi: int = Field(ge=10, lt=80)
    smoker: bool
    stroke: bool
    heartDiseaseOrAttack: bool
    physicalActivity: bool
    fruits: bool
    veggies: bool
    heavyAlcoholConsumption: bool
    anyHealthCare: bool
    noDocBecauseOfCost: bool
    generalHealthSelfAssessment: int = Field(ge=0, lt=6)
    mentalHealthIssues: int = Field(ge=0, le=30)
    physicalHealthIssues: int = Field(ge=0, le=30)
    difficultyWalking: bool
    sex: bool
    age: int = Field(ge=0, lt=130)
    education: int = Field(ge=1, le=6)
    income: int = Field(ge=1, le=8)
"""


@app.post("/ingestion/invoke")
async def invoke(request: Request):
    try:
        # clear the context variables for each request
        clear_contextvars()
        # get trace id and json data from request
        trace = request.headers.get("trace")
        data = await request.json()

        # validate the data against basemodel
        try:
            schema = json.load(open("C:/GitHub/mlops-playground/src/ingestion-service/src/utilities/validation/validator-schema.json"))
            validate(instance=data, schema=schema)
        except ValidationError as ve:
            logger.error(f"{str(ve)}")
            raise ve

        # persist data in lake
        bind_contextvars(traceID=trace)

        logger.info("Persisting data...")
        persistence_result = Persist.push(data, local=True)
        if not persistence_result:
            raise Exception

        # downstream_response = RequestHandler.post()

    except Exception as exc:
        logger.error("Persistence failed", exc_info=True)

        return {"error": str(exc)}
    # TODO: send data to model service
    return await request.json()


@app.get("/ping")
async def check():
    return "Ingestion Service is up!"
