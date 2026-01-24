from fastapi import APIRouter
from pathlib import Path
import json

router = APIRouter()

data_path: Path = Path("visualization/data/taxi_zones.geojson")

@router.get("/taxi_zones") 
async def read_taxi_zones():
    with data_path.resolve().open("r") as f:
        zone_data = json.load(f)
    return zone_data
