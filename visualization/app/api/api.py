from fastapi import APIRouter
from . import taxi_zones

api_router = APIRouter()
api_router.include_router(taxi_zones.router)
