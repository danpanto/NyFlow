from fastapi import APIRouter, Request
from datetime import datetime
import polars as pl
from typing import List, Optional, Literal
from pydantic import BaseModel, Field
import numpy as np
import math

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

class ClickPos(BaseModel):
    lat: float
    lng: float

class QueryRequest(BaseModel):
    vendors: List[str]
    date: DateRange
    variables: List[str]
    zones: Optional[List[int]] = None # Assuming PULocationID is an integer
    time_grouping: Optional[Literal["week", "day", "hour"]] = None
    click_pos: Optional[ClickPos] = None

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

@router.post("/route")
async def get_optimal_route(req: QueryRequest, request: Request):    
    try:
        startDate = datetime.fromisoformat(req.date.min).replace(tzinfo=None)
        endDate = datetime.fromisoformat(req.date.max).replace(tzinfo=None)
        
        lf = request.app.state.lf
        lf = lf.filter(pl.col("pickup_datetime").is_between(startDate, endDate))
        
        if req.vendors:
            lf = lf.filter(pl.col("VendorID").is_in(req.vendors))

        # Df con la reputación de las zonas
        top_zone_df = (
            lf.group_by("PULocationID")
            .agg(pl.col("count").sum().alias("total_trips"))
            .sort("total_trips", descending=True)
            .collect()
        )

        # Diccionario de reputación para búsquedas rápidas O(1)
        reputation_map = dict(zip(
            top_zone_df["PULocationID"].to_list(), 
            top_zone_df["total_trips"].to_list()
        ))

        # Aplicar variante de Greedy Lookahead
        MAX_STOPS: int = 5
        MAX_DISTANCE: float = 15.0
        EPS: float = 1e-4
        start: tuple[float, float] = (req.click_pos.lat, req.click_pos.lng) if req.click_pos else (40.7580, -73.9855)
        route_points: list[tuple[float, float]] = [start]
        visited_zones: set[int] = set()
        total_distance: float = 0.0
        current_coords: tuple[float, float] = start

        while len(route_points) <= MAX_STOPS and total_distance < MAX_DISTANCE:
            # Extraer puntos cercanos al actual
            distances, indexes = request.app.state.tree.query(np.deg2rad([current_coords]), k=20)
            candidate_zones_info = request.app.state.gdf_zones.iloc[indexes[0]]

            best_score: float = -1
            best_zone = None
            best_zone_coords: tuple[float, float] | None = None
            distance_to_best_zone = 0
            i_cand = 0
            for _, row in candidate_zones_info.iterrows():
                z: int = row["locationid"]
                c: tuple[float, float] = (row.geometry.centroid.y, row.geometry.centroid.x)
                d: float = distances[0][i_cand]
                i_cand += 1

                if z in visited_zones:
                    continue

                # Puntuación basada en reputación propia + potencial de vecinos (Lookahead simplificado)
                distances2, indexes2 = request.app.state.tree.query(np.deg2rad([c]), k=3)
                neighbor_zones_ids = request.app.state.gdf_zones.iloc[indexes2[0]]['locationid'].astype(int).tolist()
                
                neighbor_potential = 0.0
                for idx, n_id in enumerate(neighbor_zones_ids):
                    if n_id != z:
                        dist_n = distances2[0][idx]
                        neighbor_potential += reputation_map.get(n_id, 0.0) / (dist_n + EPS)

                own_reputation = reputation_map.get(z, 0.0)
                # Penalización lineal sobre la distancia en radianes para no aplastar zonas populares lejanas
                score = (.8 * own_reputation + 0.2 * neighbor_potential) / (d + EPS)

                if score > best_score:
                    best_score = score
                    best_zone = z
                    best_zone_coords = c
                    distance_to_best_zone = d

            if best_zone is None or best_zone_coords is None:
                break  # No hay más candidatos disponibles

            route_points.append(best_zone_coords)
            visited_zones.add(best_zone)
            total_distance += distance_to_best_zone
            current_coords = best_zone_coords

        if len(route_points)<2:
            route_points.append((40.7128, -74.0060))
            
    except Exception as e:
        print(f"Error calculating dynamic destination: {e}")
        route_points = [
                (40.7580, -73.9855), 
                (40.7128, -74.0060)
        ]           
        
    return {"status": "ok", "data": route_points}


@router.post("/restaurant-ratings")
async def get_restaurant_ratings(req: QueryRequest, request: Request):               
    return {"status": "ok", "data": request.app.state.gdf_restaurants.groupby("locationid")["SCORE"].mean().to_dict()}

@router.post("/restaurant-points")
async def get_restaurant_points(request: Request):
    try:
        gdf = request.app.state.gdf_restaurants
        import math
        
        # Eliminar coordenadas incorrectas
        gdf_valid = gdf[(gdf.geometry.centroid.x != 0.0) | (gdf.geometry.centroid.y != 0.0)]
        
        # Eliminar duplicados históricos
        gdf_valid = gdf_valid.assign(x=gdf_valid.geometry.centroid.x, y=gdf_valid.geometry.centroid.y)
        deduped = gdf_valid.groupby(['x', 'y', 'DBA'], dropna=False)['SCORE'].mean().reset_index()

        data = []
        for x, y, name, score in zip(deduped['x'], deduped['y'], deduped['DBA'], deduped['SCORE']):
            data.append({
                "name": name,
                "score": float(score) if not math.isnan(score) else None,
                "lat": float(y),
                "lng": float(x)
            })
        return {"status": "ok", "data": data}
    except Exception as e:
        print(f"Error serving restaurant points: {e}")
        return {"status": "error"}