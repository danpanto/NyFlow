"""
Concatena muchos Parquet de un directorio de forma LAZY con la librería polars, validando esquema.

- Escanea ficheros locales (glob).
- Para cada fichero crea un LazyFrame con scan_parquet.
- Asegura el esquema
- Los concatena

Falta:
- Hacer un filtrado para descartar observaciones inválidas (abajo está comentado parte del código)
- Seleccionar las variables con las que nos quedemos
- Mapear algunos valores
- Subirlo a una base de datos con la api de polars
"""


from __future__ import annotations

from pathlib import Path
import polars as pl


TARGET_SCHEMA: dict[str, pl.DataType] = {
    "VendorID": pl.String, # yellow, green
    "pickup_datetime": pl.Datetime("us"), # yellow: tpep_pickup_datetime, uber, green: lpep_pickup_datetime
    "dropoff_datetime": pl.Datetime("us"), # yellow: tpep_dropoff_datetime, uber, green: lpep_dropoff_datetime
    "passenger_count": pl.Int32, # yellow, green
    "trip_distance": pl.Float64, # yellow, uber: trip_miles, green
    "RatecodeID": pl.Int32, # yellow, green
    "store_and_fwd_flag": pl.String, # yellow, green
    "PULocationID": pl.Int32, # yellow, uber, green
    "DOLocationID": pl.Int32, # yellow, uber, green
    "payment_type": pl.Int32, # yellow, green
    "fare_amount": pl.Float64, # yellow, uber: base_passenger_fare, green
    "extra": pl.Float64, # yellow, green
    "mta_tax": pl.Float64, # yellow, green
    "tip_amount": pl.Float64, # yellow, uber: tips, green
    "tolls_amount": pl.Float64, # yellow, uber: tolls, green
    "improvement_surcharge": pl.Float64, # yellow, green
    "total_amount": pl.Float64, # yellow, green, uber: base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + airport_fee + cbd_congestion_fee
    "congestion_surcharge": pl.Float64, # yellow, uber, green
    "airport_fee": pl.Float64, # yellow: Airport_fee, uber
    "cbd_congestion_fee": pl.Float64, # yellow, uber
    "hvfhs_license_num": pl.String, # uber
    "dispatching_base_num": pl.String, # uber
    "originating_base_num": pl.String, # uber
    "request_datetime": pl.Datetime("us"), # uber
    "on_scene_datetime": pl.Datetime("us"), # uber
    "bcf": pl.Float64, # uber
    "sales_tax": pl.Float64, # uber
    "driver_pay": pl.Float64, # uber
    "shared_request_flag": pl.String, # uber
    "shared_match_flag": pl.String,# uber
    "access_a_ride_flag": pl.String,# uber
    "wav_request_flag": pl.String,# uber
    "wav_match_flag": pl.String,# uber
    "trip_type":pl.Int32 # green

    # trip_time: uber
}

def _find_parquets(root_dir: str, filename: str) -> list[str]:
    root = Path(root_dir)
    return sorted(p.as_posix() for p in root.rglob(filename))

def _normalize_to_target_schema(lf: pl.LazyFrame) -> pl.LazyFrame:
    # Asegura que existan todas las columnas del esquema (las que falten -> null)
    lf = lf.with_columns([
        pl.lit(None).alias(col)
        for col in TARGET_SCHEMA.keys()
        if col not in lf.collect_schema()
    ])  # with_columns reemplaza/añade columnas [web:518]

    # Castea y ordena columnas; strict=False => si no castea, null [web:520]
    return lf.select([
        pl.col(col).cast(dtype, strict=False).alias(col)
        for col, dtype in TARGET_SCHEMA.items()
    ])


def _scan_many(paths: list[str], hive_partitioning: bool = False) -> pl.LazyFrame:
    if not paths:
        raise FileNotFoundError("No se encontraron parquets.")
    lfs = [pl.scan_parquet(p, hive_partitioning=hive_partitioning) for p in paths]
    return pl.concat(lfs, how="diagonal_relaxed")



# ---------- 1) Yellow ----------
def load_yellow(root_dir: str, hive_partitioning: bool = False) -> pl.LazyFrame:
    paths = _find_parquets(root_dir, "yellow_taxi.parquet")
    lf = _scan_many(paths, hive_partitioning=hive_partitioning)

    # Mapeo de columnas originales -> unificadas
    lf = lf.rename({
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "store_and_fwd_flag": "store_and_fwd_flag",
        "PULocationID": "PULocationID",
        "DOLocationID": "DOLocationID",
        "RatecodeID": "RatecodeID",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
        "extra": "extra",
        "mta_tax": "mta_tax",
        "tip_amount": "tip_amount",
        "tolls_amount": "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "total_amount": "total_amount",
        "congestion_surcharge": "congestion_surcharge",
        "airport_fee": "airport_fee",
        "cbd_congestion_fee": "cbd_congestion_fee",
        "VendorID": "VendorID",
    })

    lf = lf.with_columns(
        pl.lit("Yellow Taxi").alias("hvfhs_license_num")   # indica tipo de transporte
    )

    return _normalize_to_target_schema(lf)


# ---------- 2) Green ----------
def load_green(root_dir: str, hive_partitioning: bool = False) -> pl.LazyFrame:
    paths = _find_parquets(root_dir, "green_taxi.parquet")
    lf = _scan_many(paths, hive_partitioning=hive_partitioning)

    lf = lf.rename({
        "lpep_pickup_datetime": "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "store_and_fwd_flag": "store_and_fwd_flag",
        "PULocationID": "PULocationID",
        "DOLocationID": "DOLocationID",
        "RatecodeID": "RatecodeID",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
        "extra": "extra",
        "mta_tax": "mta_tax",
        "tip_amount": "tip_amount",
        "tolls_amount": "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "total_amount": "total_amount",
        "congestion_surcharge": "congestion_surcharge",
        "cbd_congestion_fee": "cbd_congestion_fee",
        "trip_type": "trip_type",
        "VendorID": "VendorID",
    })

    
    lf = lf.with_columns(
        pl.lit("Green Taxi").alias("hvfhs_license_num")   # indica tipo de transporte
    )

    return _normalize_to_target_schema(lf)


# ---------- 3) Uber / HVFHV ----------
def load_uber(root_dir: str, hive_partitioning: bool = False) -> pl.LazyFrame:
    paths = _find_parquets(root_dir, "uber.parquet")
    lf = _scan_many(paths, hive_partitioning=hive_partitioning)

    # Renombres a tu esquema unificado
    lf = lf.rename({
        "hvfhs_license_num": "hvfhs_license_num",
        "dispatching_base_num": "dispatching_base_num",
        "originating_base_num": "originating_base_num",
        "request_datetime": "request_datetime",
        "on_scene_datetime": "on_scene_datetime",
        "pickup_datetime": "pickup_datetime",
        "dropoff_datetime": "dropoff_datetime",
        "PULocationID": "PULocationID",
        "DOLocationID": "DOLocationID",
        "trip_miles": "trip_distance",
        "base_passenger_fare": "fare_amount",
        "tolls": "tolls_amount",
        "tips": "tip_amount",
        "driver_pay": "driver_pay",
        "bcf": "bcf",
        "sales_tax": "sales_tax",
        "congestion_surcharge": "congestion_surcharge",
        "airport_fee": "airport_fee",
        "cbd_congestion_fee": "cbd_congestion_fee",
        "shared_request_flag": "shared_request_flag",
        "shared_match_flag": "shared_match_flag",
        "access_a_ride_flag": "access_a_ride_flag",
        "wav_request_flag": "wav_request_flag",
        "wav_match_flag": "wav_match_flag",
    })

    # Asegura que los sumandos existan (si faltan, null -> luego coalesce a 0.0)
    lf = lf.with_columns([
        pl.lit(None).alias(c)
        for c in [
            "fare_amount", "tolls_amount", "bcf", "sales_tax",
            "congestion_surcharge", "airport_fee", "tip_amount", "cbd_congestion_fee"
        ]
        if c not in lf.collect_schema()
    ])

    # Calcula total_amount = base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + airport_fee + tips + cbd_congestion_fee
    # coalesce(null, 0) para que no se vuelva null si falta algún componente
    lf = lf.with_columns([
        (
            pl.coalesce([pl.col("fare_amount"), pl.lit(0.0)]) +
            pl.coalesce([pl.col("tolls_amount"), pl.lit(0.0)]) +
            pl.coalesce([pl.col("bcf"), pl.lit(0.0)]) +
            pl.coalesce([pl.col("sales_tax"), pl.lit(0.0)]) +
            pl.coalesce([pl.col("congestion_surcharge"), pl.lit(0.0)]) +
            pl.coalesce([pl.col("airport_fee"), pl.lit(0.0)]) +
            pl.coalesce([pl.col("cbd_congestion_fee"), pl.lit(0.0)])
        ).alias("total_amount")
    ])

    return _normalize_to_target_schema(lf)

    # start_py = datetime(year, month, 1)
    # end_py = start_py + relativedelta(months=1)
    # start = pl.lit(start_py)
    # end = pl.lit(end_py)

    # url = url_raw.format(year=year, month=month)

    # lf = (
    #     pl.scan_parquet(url).filter(
    #         (pl.col("tpep_pickup_datetime") >= start) &
    #         (pl.col("tpep_pickup_datetime") < end) &
    #         (pl.col("tpep_dropoff_datetime") >= pl.col("tpep_pickup_datetime")) &
    #         (pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime") <= pl.duration(hours=24)) &
    #         (pl.col("trip_distance") > 0)
    #     )

if __name__ == "__main__":
    lf_y = load_yellow("data", hive_partitioning=True)
    lf_g = load_green("data", hive_partitioning=True)
    lf_u = load_uber("data", hive_partitioning=True)

    # Si quieres unificar todo en uno:
    lf_all = pl.concat([lf_y, lf_g, lf_u], how="vertical_relaxed")

    print(lf_all.select(["hvfhs_license_num"]).head().collect())

    
