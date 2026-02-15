from fastapi import APIRouter, Request
from datetime import datetime
import polars as pl
from typing import List, Optional, Literal
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

sumOp = lambda x: pl.col(x).sum()
meanOp = lambda x: pl.col(x).sum() / pl.col("count").sum()
meanDisOp = lambda x: pl.col(x).sum() / pl.col("trip_distance").sum()
meanTimeOp = lambda x: pl.col(x).sum() / pl.col("duration").sum()

variable_operations = {
    "total_trips": sumOp("count"),
    "total_price": sumOp("fare_amount"),
    "mean_price": meanOp("fare_amount"),
    "total_tip": sumOp("tip_amount"),
    "mean_tip": meanOp("tip_amount"),
    "mean_distance": meanOp("trip_distance"),
    "mean_duration": meanOp("duration"),
    "mean_tip_time": meanTimeOp("tip_amount"),
    "mean_tip_dis": meanDisOp("tip_amount"),
    "mean_price_time": meanTimeOp("fare_amount"),
    "mean_price_dis": meanTimeOp("fare_amount")
}

class DateRange(BaseModel):
    min: str
    max: str

class QueryRequest(BaseModel):
    vendors: List[str]
    date: DateRange
    variables: List[str]
    zones: Optional[List[int]] = None # Assuming PULocationID is an integer
    time_grouping: Optional[Literal["week", "day", "hour"]] = None

@router.post("/query")
async def get_dashboard_data(req: QueryRequest, request: Request):
    
    try:
        startDate = datetime.fromisoformat(req.date.min).replace(tzinfo=None)
        endDate = datetime.fromisoformat(req.date.max).replace(tzinfo=None)
    except ValueError:
        return {"status": "invalid", "msg": "Invalid date format."}

    active_aggs = [variable_operations[v].alias(v) for v in req.variables if v in variable_operations]
    if len(active_aggs) == 0:
        return {"status": "invalid", "msg": "Invalid variable."}

    lf = request.app.state.lf

    # 1. Date and optional filters
    lf = lf.filter(pl.col("pickup_datetime").is_between(startDate, endDate))

    if req.vendors:
        lf = lf.filter(pl.col("VendorID").is_in(req.vendors))
        
    if req.zones:
        lf = lf.filter(pl.col("PULocationID").is_in(req.zones))

    # 2. Prepare the base ids layer (and filter it by zones if provided)
    # Ensure it's evaluated lazily to align with `lf`
    base_ids = request.app.state.ids.lazy() if hasattr(request.app.state.ids, 'lazy') else request.app.state.ids
    if req.zones:
        base_ids = base_ids.filter(pl.col("PULocationID").is_in(req.zones))

    # 3. Handle grouping (Time Series vs Flat)
    if req.time_grouping:
        interval = "7d"

        if req.time_grouping == "day":
            interval = "1d"
        elif req.time_grouping == "hour":
            interval = "1h"
        
        # Truncate datetimes to the chosen interval bucket
        lf = lf.with_columns(pl.col("pickup_datetime").dt.truncate(interval).alias("time_bucket"))
        
        lf_trips = lf.group_by(["PULocationID", "time_bucket"]).agg(active_aggs)
        
        # Cross join unique time buckets with base_ids to ensure 0s for inactive periods
        unique_times = lf_trips.select("time_bucket").unique()
        base_grid = base_ids.join(unique_times, how="cross")
        
        df_result = (
            base_grid
            .join(lf_trips, on=["PULocationID", "time_bucket"], how="left")
            .fill_null(0)
            .collect()
        )
        
        response_data = {var: {} for var in req.variables if var in df_result.columns}
        
        # Partition data by time to build the nested JSON
        partitions = df_result.partition_by("time_bucket", as_dict=True)
        for time_key, group_df in partitions.items():
            # Extract datetime from tuple (Polars as_dict keys are tuples)
            time_val = time_key[0] if isinstance(time_key, tuple) else time_key
            time_str = time_val.isoformat()
            
            for var in req.variables:
                if var in df_result.columns:
                    response_data[var][time_str] = dict(zip(
                        group_df["PULocationID"].to_list(), 
                        group_df[var].to_list()
                    ))
                    
    else:
        # Standard execution (No time grouping)
        lf_trips = lf.group_by("PULocationID").agg(active_aggs)
        
        df_result = (
            base_ids
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
