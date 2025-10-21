from fastapi import APIRouter
from api.services.parquet_reader import get_patient_summary
from api.models.patient_metrics_schema import PatientSummary

router = APIRouter()


@router.get("/patient/{ward}/{bed_number}", response_model=PatientSummary)
def get_summary(ward: str, bed_number: str):
    """
    Returns patient summary metrics for a given ward and bed.
    """
    result = get_patient_summary(ward, bed_number)
    return result
