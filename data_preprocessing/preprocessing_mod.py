from __future__ import annotations

import os
import time
import requests
from pathlib import Path
import polars as pl
import pyarrow.parquet as pq

TARGET_SCHEMA: dict[str, pl.DataType] = {
    "VendorID": pl.String, 
    "pickup_datetime": pl.Datetime("us"), 
    "dropoff_datetime": pl.Datetime("us"), 
    "passenger_count": pl.Int32, 
    "trip_distance": pl.Float64, 
    "RatecodeID": pl.Int32, 
    "store_and_fwd_flag": pl.Boolean,
    "PULocationID": pl.Int32, 
    "DOLocationID": pl.Int32, 
    "payment_type": pl.String, 
    "fare_amount": pl.Float64, 
    "extra": pl.Float64, 
    "mta_tax": pl.Float64, 
    "tip_amount": pl.Float64, 
    "tolls_amount": pl.Float64, 
    "improvement_surcharge": pl.Float64, 
    "total_amount": pl.Float64, 
    "congestion_surcharge": pl.Float64, 
    "airport_fee": pl.Float64, 
    "cbd_congestion_fee": pl.Float64, 
    "dispatching_base_num": pl.String, 
    "originating_base_num": pl.String, 
    "request_datetime": pl.Datetime("us"), 
    "on_scene_datetime": pl.Datetime("us"), 
    "bcf": pl.Float64, 
    "sales_tax": pl.Float64, 
    "driver_pay": pl.Float64, 
    "shared_request_flag": pl.Boolean,
    "shared_match_flag": pl.Boolean,
    "access_a_ride_flag": pl.Boolean,
    "wav_request_flag": pl.Boolean,
    "wav_match_flag": pl.Boolean,
    "trip_type": pl.String, 
    "ehail_fee": pl.Float64, 
    "hvfhs_license_num": pl.String, 
}

def _yn10_to_bool(expr: pl.Expr) -> pl.Expr:
    """
    Convierte Y/N, 1/0, 1.0/0.0 a Booleano real.
    Maneja Utf8View casteando primero a String.
    """
    s = expr.cast(pl.String, strict=False).str.strip_chars().str.to_uppercase()
    
    # 2. Mapa de valores aceptados
    mapping = {
        "Y": True, "YES": True, "T": True, "TRUE": True,
        "1": True, "1.0": True,
        "N": False, "NO": False, "F": False, "FALSE": False,
        "0": False, "0.0": False,
    }
    
    # 3. Reemplazo estricto (lo que no esté en el mapa será Null)
    return s.replace_strict(mapping, default=None)

def _download_file(url: str, local_path: Path) -> bool:
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        with requests.get(url, stream=True, headers=headers) as r:
            if r.status_code == 200:
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                return True
            else:
                return False
    except Exception as e:
        print(f"      Error red: {e}")
        return False

def _normalize_to_target_schema(lf: pl.LazyFrame, rename: dict) -> pl.LazyFrame:
    # 1. Renombrado seguro
    current_cols = lf.collect_schema().names()
    safe_rename = {k: v for k, v in rename.items() if k in current_cols}
    if safe_rename:
        lf = lf.rename(safe_rename)
    
    # 2. Selección y Casteo
    cols_after = lf.collect_schema().names()
    expressions = []
    
    for col_name, dtype in TARGET_SCHEMA.items():
        if col_name in cols_after:
            if dtype == pl.Boolean:
                expressions.append(_yn10_to_bool(pl.col(col_name)).alias(col_name))
            else:
                expressions.append(pl.col(col_name).cast(dtype, strict=False))
        else:
            # Columna faltante -> Null
            expressions.append(pl.lit(None).cast(dtype).alias(col_name))
            
    return lf.select(expressions)

def _append_to_parquet(lf: pl.LazyFrame, writer: pq.ParquetWriter | None, out_file: Path):
    try:
        df = lf.collect()
    except Exception as e:
        print(f"      Error leyendo Parquet: {e}")
        return writer

    if df.height == 0:
        return writer

    table = df.to_arrow()
    
    if writer is None:
        writer = pq.ParquetWriter(out_file, table.schema, compression="zstd")
        
    try:
        writer.write_table(table)
    except Exception:
        try:
            table = table.cast(writer.schema)
            writer.write_table(table)
        except Exception as e:
            print(f"      Error escribiendo disco: {e}")
            
    return writer

def process_web_data(vehicle_type: str, url_prefix: str, start_year: int, end_year: int, 
                     out_file: Path|str, rename: dict):
    
    out_file = Path(out_file)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    temp_file = Path(f"temp_{vehicle_type}.parquet")
    writer = None
    
    print(f"--- Iniciando {vehicle_type} ({start_year}-{end_year}) ---")

    try:
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                file_name = f"{url_prefix}_tripdata_{year}-{month:02d}.parquet"
                url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
                
                print(f"{year}-{month:02d}: Descargando...", end="\r")
                
                if _download_file(url, temp_file):
                    try:
                        lf = pl.scan_parquet(temp_file)
                        lf = _normalize_to_target_schema(lf, rename)
                        writer = _append_to_parquet(lf, writer, out_file)
                        print(f"{year}-{month:02d}: OK              ")
                    except Exception as e:
                        print(f"{year}-{month:02d}: Error {e}")
                    finally:
                        if temp_file.exists():
                            os.remove(temp_file)
                else:
                    print(f"{year}-{month:02d}: No encontrado.")
                
                time.sleep(0.5)

    finally:
        if writer:
            writer.close()
            print(f"Finalizado {vehicle_type}. Guardado en: {out_file}")
        if temp_file.exists():
            os.remove(temp_file)

# --- EJECUCIÓN ---
if __name__ == "__main__":
    out_dir = Path.cwd() / "data"
    YEAR_START = 2021
    YEAR_END = 2025 

    # 1. YELLOW
    process_web_data(
        vehicle_type="yellow_taxi",
        url_prefix="yellow",
        start_year=YEAR_START, end_year=YEAR_END,
        out_file=out_dir / "yellow_taxi_unified.parquet",
        rename={
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
            "Airport_fee": "airport_fee"
        }
    )

    # # 2. GREEN
    # process_web_data(
    #     vehicle_type="green_taxi",
    #     url_prefix="green",
    #     start_year=YEAR_START, end_year=YEAR_END,
    #     out_file=out_dir / "green_taxi_unified.parquet",
    #     rename={
    #         "lpep_pickup_datetime": "pickup_datetime",
    #         "lpep_dropoff_datetime": "dropoff_datetime"
    #     }
    # )

    # # 3. UBER (FHV)
    # process_web_data(
    #     vehicle_type="uber",
    #     url_prefix="fhvhv", 
    #     start_year=YEAR_START, end_year=YEAR_END,
    #     out_file=out_dir / "uber_unified.parquet",
    #     rename={
    #         "hvfhs_license_num": "hvfhs_license_num",
    #         "dispatching_base_num": "dispatching_base_num",
    #         "pickup_datetime": "pickup_datetime",
    #         "dropoff_datetime": "dropoff_datetime",
    #         "dropOff_datetime": "dropoff_datetime",
    #         "PULocationID": "PULocationID",
    #         "PUlocationID": "PULocationID",
    #         "DOLocationID": "DOLocationID",
    #         "DOlocationID": "DOLocationID",
    #         "trip_miles": "trip_distance",            
    #         "base_passenger_fare": "fare_amount",     
    #         "tips": "tip_amount",                     
    #         "tolls": "tolls_amount",                  
    #         "black_car_fund": "bcf",                  
    #         "sales_tax": "sales_tax",                 
    #         "congestion_surcharge": "congestion_surcharge", 
    #         "airport_fee": "airport_fee",
    #         "driver_pay": "driver_pay",
    #         "shared_request_flag": "shared_request_flag",
    #         "shared_match_flag": "shared_match_flag",
    #         "access_a_ride_flag": "access_a_ride_flag",
    #         "wav_request_flag": "wav_request_flag",
    #         "wav_match_flag": "wav_match_flag"
    #     }
    # )