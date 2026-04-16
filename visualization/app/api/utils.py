from fastapi import APIRouter
from pathlib import Path

router = APIRouter()

data_path: Path = Path("visualization/data/taxi_zones.geojson")


@router.get("/date_range")
async def get_date_range():
    return {
        "min": "2021-01-01",
        "max": "2025-12-01",
        "total_months": 12 * 5 - 1,
    }
