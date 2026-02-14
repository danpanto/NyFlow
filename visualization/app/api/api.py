from fastapi import APIRouter, Request
from fastapi import APIRouter, Request, Query
router = APIRouter()

@router.get("/taxi_zones")
async def read_taxi_zones(request: Request):
    # Access the cache from the app state
    return request.app.state.taxi_zones

@router.get("/date_range")
async def read_date_range(request: Request):
    return {
        "min": "2021-01-01T00:00:00",
        "max": "2025-12-31T23:00:00"
    }

@router.get("/vendor")
async def read_vendors(request: Request):
    return {
        "0": "Green taxi",
        "1": "Yellow taxi",
        "2": "Uber",
        "3": "Lyft",
        "4": "Others",
    }
