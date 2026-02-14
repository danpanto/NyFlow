from fastapi import APIRouter, Request
from fastapi import APIRouter, Request, Query
router = APIRouter()

@router.get("/taxi_zones")
async def read_taxi_zones(request: Request):
    # Access the cache from the app state
    return request.app.state.taxi_zones

