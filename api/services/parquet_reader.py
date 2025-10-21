import pandas as pd
import logging
from fastapi import HTTPException, status

logger = logging.getLogger(__name__)

GOLD_PATH = "././data/analytics/pose_data_metrics"


# Load parquet once at startup - enables fast memory load as opposed to IO
try:
    logger.info(f"Loading Parquet data into memory from {GOLD_PATH}")
    PARQUET_DF = pd.read_parquet(GOLD_PATH)
    logger.info(f"Loaded {len(PARQUET_DF)} rows into memory.")
except Exception as e:
    logger.error(f"Failed to load Parquet: {e}")
    PARQUET_DF = pd.DataFrame()


def get_patient_summary(ward: str, bed_number: str):
    df = PARQUET_DF
    result = df[(df["ward"] == ward) & (df["bed_number"] == bed_number)]

    if result.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No data found for ward: {ward}-{bed_number}",
        )

    return result.to_dict(orient="records")[0]


def reload_parquet():
    global PARQUET_DF
    try:
        logger.info("Reloading Parquet")
        PARQUET_DF = pd.read_parquet(GOLD_PATH)
        logger.info(f"Reloaded data successfully")
    except Exception as e:
        logger.error(f"Failed to reload data {e}")
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "Internal Error while reading data"
        )
