from fastapi import APIRouter, Request
from datetime import datetime
import polars as pl
from typing import List, Optional
from pydantic import BaseModel, Field

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
    }

class DateRange(BaseModel):
    min: str
    max: str

class QueryRequest(BaseModel):
    vendors: List[str]
    date: DateRange
    variables: List[str]

@router.post("/query")
async def get_dashboard_data(req: QueryRequest, request: Request):
    
    try:
        startDate = datetime.fromisoformat(req.date.min).replace(tzinfo=None)
        endDate = datetime.fromisoformat(req.date.max).replace(tzinfo=None)
    except ValueError:
        return {"status": "invalid", "msg": "Invalid date format."}

    sumOp = lambda x: pl.col(x).sum()
    meanOp = lambda x: pl.col(x).sum() / pl.col("count").sum()

    variable_operations = {
        "total_trips": sumOp("count"),
        "total_price": sumOp("fare_amount"),
        "mean_price": meanOp("fare_amount"),
        "total_tip": sumOp("tip_amount"),
        "mean_tip": meanOp("tip_amount"),
        "mean_distance": meanOp("trip_distance"),
    }

    active_aggs = [variable_operations[v].alias(v) for v in req.variables if v in variable_operations]
    if len(active_aggs) == 0:
        return {"status": "invalid", "msg": "Invalid variable."}

    lf = request.app.state.lf

    lf = lf.filter(pl.col("pickup_datetime").is_between(startDate, endDate))

    if req.vendors:
        lf = lf.filter(pl.col("VendorID").is_in(req.vendors))

    lf_trips = (
        lf.group_by("PULocationID")
          .agg(active_aggs)
    )

    df_result = (
        request.app.state.ids
            .join(lf_trips, on="PULocationID", how="left")
            .fill_null(0)
            .collect()
    )


    response_data = {}
    for var in req.variables:
        if var in df_result.columns:
            response_data[var] = dict(zip(
                df_result["PULocationID"].to_list(), 
                df_result[var].to_list()
            ))

    return {"status": "ok", "data": response_data}
