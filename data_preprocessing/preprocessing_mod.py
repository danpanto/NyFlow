from __future__ import annotations

import os
import time
import requests
from pathlib import Path
import polars as pl
import pyarrow.parquet as pq
from field_tranformations import _normalize_to_target_schema, yellow_params, green_params, uber_params


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
                     out_file: Path|str, coalesce: list = [], rename:dict = {}, required_schema:list = []):
    
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
                        lf = _normalize_to_target_schema(lf, rename=rename, required_schema=required_schema, coalesce=coalesce)
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
        start_year=YEAR_START, end_year=YEAR_END,
        out_file=out_dir / "yellow_taxi_unified.parquet",
        **yellow_params
    )

    # # 2. GREEN
    # process_web_data(
    #     start_year=YEAR_START, end_year=YEAR_END,
    #     out_file=out_dir / "green_taxi_unified.parquet",
    #     **green_params
    # )

    # # 3. UBER (FHV)
    # process_web_data(
    #     start_year=YEAR_START, end_year=YEAR_END,
    #     out_file=out_dir / "uber_unified.parquet",
    #     **uber_params
    # )