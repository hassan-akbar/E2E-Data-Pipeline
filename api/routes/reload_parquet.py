from fastapi import APIRouter, HTTPException, Header, status
from services.parquet_reader import reload_parquet

import os

router = APIRouter()


@router.post("/reload")
async def reload_data(x_api_key: str = Header(None)):
    print(os.getenv("RELOAD_API_KEY"))
    if x_api_key != os.getenv("RELOAD_API_KEY"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
    reload_parquet()
    return {"message": "Data reloaded successfully"}
