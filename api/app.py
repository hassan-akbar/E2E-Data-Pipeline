# api/app.py
from fastapi import FastAPI
import logging
from routes.patient_summary import router as patient_router
from routes.reload_parquet import router as reload_parquet_router
from dotenv import load_dotenv

load_dotenv()


# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


app = FastAPI(title="Patient Monitoring Data Service")

app.include_router(patient_router, prefix="/api", tags=["Patient Metrics"])
app.include_router(
    reload_parquet_router,
)


@app.get("/")
def root():
    return {"message": "FastAPI is running"}
