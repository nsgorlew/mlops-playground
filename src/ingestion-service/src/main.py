from fastapi import FastAPI
from pydantic import BaseModel
import json
import structlog


app = FastAPI()


class InvokeRequestData(BaseModel):
	appID: str
	highBP: int
	highChol: int
	cholCheck: int
	bmi: int
	smoker: int
	stroke: int
	heartDiseaseOrAttack: int
	physicalActivity: int
	fruits: int
	veggies: int
	heavyAlcoholConsumption: int
	anyHealthCare: int
	noDocBecauseOfCost: int
	generalHealthSelfAssessment: int
	mentalHealthIssues: int
	physicalHealthIssues: int
	difficultyWalking: int
	sex: int
	age: int
	education: int
	income: int


@app.post("/invoke")
async def invoke(data: InvokeRequestData):
    # TODO: persist data in duckdb
    

    # TODO: send data to model service


@app.get("/ping")
async def check():
	return "Ingestion Service is healthy!"