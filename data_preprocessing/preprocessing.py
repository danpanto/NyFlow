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

from optimize_raw_df import optimize_dataframe, get_sort_column_by_schema

from pathlib import Path
import polars as pl
import pyarrow.parquet as pq

TARGET_SCHEMA: dict[str, pl.DataType] = {
    "VendorID": pl.String, # yellow, green
    "pickup_datetime": pl.Datetime("us"), # yellow: tpep_pickup_datetime, uber, green: lpep_pickup_datetime
    "dropoff_datetime": pl.Datetime("us"), # yellow: tpep_dropoff_datetime, uber, green: lpep_dropoff_datetime
    "passenger_count": pl.Int32, # yellow, green
    "trip_distance": pl.Float64, # yellow, uber: trip_miles, green
    "RatecodeID": pl.Int32, # yellow, green
    "store_and_fwd_flag": pl.Boolean, # yellow, green
    "PULocationID": pl.Int32, # yellow, uber, green
    "DOLocationID": pl.Int32, # yellow, uber, green
    "payment_type": pl.String, # yellow, green
    "fare_amount": pl.Float64, # yellow, uber: base_passenger_fare, green
    "extra": pl.Float64, # yellow, green
    "mta_tax": pl.Float64, # yellow, green
    "tip_amount": pl.Float64, # yellow, uber: tips, green
    "tolls_amount": pl.Float64, # yellow, uber: tolls, green
    "improvement_surcharge": pl.Float64, # yellow, green
    "total_amount": pl.Float64, # yellow, green, uber: base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + airport_fee + cbd_congestion_fee
    "congestion_surcharge": pl.Float64, # yellow, uber, green
    "airport_fee": pl.Float64, # yellow: Airport_fee, uber
    "cbd_congestion_fee": pl.Float64, # yellow, uber, green
    "dispatching_base_num": pl.String, # uber
    "originating_base_num": pl.String, # uber
    "request_datetime": pl.Datetime("us"), # uber
    "on_scene_datetime": pl.Datetime("us"), # uber
    "bcf": pl.Float64, # uber
    "sales_tax": pl.Float64, # uber
    "driver_pay": pl.Float64, # uber
    "shared_request_flag": pl.Boolean, # uber
    "shared_match_flag": pl.Boolean,# uber
    "access_a_ride_flag": pl.Boolean,# uber
    "wav_request_flag": pl.Boolean,# uber
    "wav_match_flag": pl.Boolean,# uber
    "trip_type": pl.String, # green
    "ehail_fee": pl.Float64, # green
    "hvfhs_license_num": pl.String, # uber USAR

    # trip_time: uber
    # black_car_fund #uber
}


# ---------- 1) Yellow ----------
yellow_params = {
    'in_parquet_names': "yellow_taxi.parquet",

    'coalesce': [("Airport_fee", "airport_fee")],

    'rename' : {
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime"
    },

    'required_schema' : ["VendorID", "pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance",
        "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra",      
        "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge",
        "airport_fee", "cbd_congestion_fee"]
}

# ---------- 2) Green ----------
green_params = {
    'in_parquet_names': "green_taxi.parquet",
    'rename': {
        "lpep_pickup_datetime": "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
    },

    'required_schema': ["pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance", "store_and_fwd_flag",
        "PULocationID", "DOLocationID", "RatecodeID", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "cbd_congestion_fee",
        "trip_type", "VendorID", "ehail_fee"]
}
    
# ---------- 3) Uber / HVFHV ----------
uber_params = {
    'in_parquet_names': "uber.parquet",

    'rename': {
        "trip_miles": "trip_distance",
        "base_passenger_fare": "fare_amount",
        "tolls": "tolls_amount",
        "tips": "tip_amount",
    },

    'required_schema': ["hvfhs_license_num","dispatching_base_num","originating_base_num","request_datetime","on_scene_datetime",
        "pickup_datetime","dropoff_datetime","PULocationID","DOLocationID","trip_distance","fare_amount","tolls_amount","tip_amount",
        "driver_pay","bcf","sales_tax","congestion_surcharge","airport_fee","cbd_congestion_fee","shared_request_flag",
        "shared_match_flag","access_a_ride_flag","wav_request_flag","wav_match_flag", "total_amount"]
}


def _yn10_to_bool(expr: pl.Expr) -> pl.Expr:
    s = expr.cast(pl.Utf8, strict=False).str.strip_chars().str.to_uppercase()
    mapping = {
        "Y": True, "1": True, "1.0": True,
        "N": False, "0": False, "0.0": False,
    }
    return s.replace_strict(mapping, default=None)

def _normalize_payment_type_label(expr: pl.Expr) -> pl.Expr:
    s2 = (
        expr.cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.replace_all(",", "")
            .str.to_lowercase()
    )

    mapping = {
        # numérico
        "0": "flex_fare_tip",
        "1": "credit_card",
        "2": "cash",
        "3": "no_charge",
        "4": "dispute",
        "5": "unknown",
        "6": "voided",
        # sinónimos texto
        "credit": "credit_card", "cre": "credit_card", "crd": "credit_card",
        "cash": "cash", "cas": "cash", "csh": "cash",
        "no charge": "no_charge", "no": "no_charge",
        "dispute": "dispute", "dis": "dispute",
    }
    return s2.replace_strict(mapping, default=None)

def _normalize_trip_type_label(expr: pl.Expr) -> pl.Expr:
    s = expr.cast(pl.Utf8, strict=False).str.strip_chars()
    mapping = {"1": "street_hail", "2": "dispatch"}
    return s.replace_strict(mapping, default=None)

FIELD_TRANFORMATIONS: dict[str, TransformationFunc] = {
    "payment_type": _normalize_payment_type_label(pl.col("payment_type")).alias("payment_type"),
    "trip_type": _normalize_trip_type_label(pl.col("trip_type")).alias("trip_type"),
    "hvfhs_license_num": pl.col("hvfhs_license_num").replace({'HV0002':'Juno', 'HV0003':'Uber', 'HV0004':'Via', 'HV0005':'Lyft'}).alias("hvfhs_license_num")
}

# estos campos se crean al final de las transformaciones en caso de que la columna no exista
FIELD_DERIVATIONS: dict[str, TransformationFunc] = {
    "total_amount": (
                        pl.coalesce([pl.col("fare_amount"), pl.lit(0.0)]) +
                        pl.coalesce([pl.col("tolls_amount"), pl.lit(0.0)]) +
                        pl.coalesce([pl.col("bcf"), pl.lit(0.0)]) +
                        pl.coalesce([pl.col("sales_tax"), pl.lit(0.0)]) +
                        pl.coalesce([pl.col("congestion_surcharge"), pl.lit(0.0)]) +
                        pl.coalesce([pl.col("airport_fee"), pl.lit(0.0)]) +
                        pl.coalesce([pl.col("cbd_congestion_fee"), pl.lit(0.0)])
                    ).alias("total_amount")
}


def _normalize_to_target_schema(lf: pl.LazyFrame, coalesce: list = [], rename:dict = {}, required_schema:list = []) -> pl.LazyFrame:
    existing = set(lf.collect_schema().names())

    # asegurar que tiene las columnas de coalesce
    needed = {c for pair in coalesce for c in pair}  # set de l y r
    missing = [c for c in needed if c not in existing]

    lf = lf.with_columns([
        pl.lit(None).alias(c)
        for c in missing
    ])

    # las columnas primeras del coalesce se eliminan
    drop = {l for l, _ in coalesce}

    lf = (
        lf.rename(rename, strict=False)
        .with_columns([pl.coalesce(l, r).alias(r) for l, r in coalesce])
        .drop(drop, strict=False)
    )

    # actualizar las columnas existentes
    existing = (
        (((existing | {r for _, r in coalesce}) - drop) - set(rename.keys()))
        | set(rename.values())
    )

    # rellenar las columnas que faltan en el esquema con None
    lf = lf.with_columns([pl.lit(None).alias(col) for col in required_schema if col not in existing])

    # castea tipos o hacer tranformaciones
    casteos = []
    for col in required_schema:
        tipo = TARGET_SCHEMA[col]
        if tipo == pl.Boolean:
            casteos.append(_yn10_to_bool(pl.col(col)).alias(col))
        elif col in FIELD_TRANFORMATIONS:
            casteos.append(FIELD_TRANFORMATIONS[col])
        else:
            casteos.append(pl.col(col).cast(tipo, strict=False).alias(col))
    
    lf = lf.select(casteos)


    # finalmente, derivar las columnas necesarias
    return lf.with_columns([deriv for col, deriv in FIELD_DERIVATIONS.items() if col in required_schema and col not in existing])


def _find_parquets(root: Path, filename: str) -> list[str]:
    return sorted(p for p in root.rglob(filename))

def _scan_many(paths: list[Path], hive_partitioning: bool = False) -> pl.LazyFrame:
    if not paths:
        raise FileNotFoundError("No se encontraron parquets.")
    lfs = [pl.scan_parquet(p, hive_partitioning=hive_partitioning) for p in paths]
    return pl.concat(lfs, how="diagonal_relaxed")

def _append_lf_to_single_parquet(lf: pl.LazyFrame, writer: pq.ParquetWriter | None, out_file: Path):
    df = lf.collect()
    table = df.to_arrow()

    if writer is None:
        writer = pq.ParquetWriter(out_file, table.schema, compression="zstd")

    writer.write_table(table)
    return writer


def process_parquets(root_path: Path|str, out_file: Path|str, in_parquet_names: str, sim_read: int = 4, 
                    coalesce: list = [], rename:dict = {}, required_schema:list = []):

    root_path = Path(root_path)
    out_file = Path(out_file)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    writer: pq.ParquetWriter | None = None
    parquets_dirs = _find_parquets(root_path, in_parquet_names)
    try:
        for i in range(0, len(parquets_dirs), sim_read):
            proc_path = parquets_dirs[i:i+sim_read]
            lf = _scan_many(proc_path)
            lf = _normalize_to_target_schema(lf, coalesce=coalesce, rename=rename, required_schema=required_schema)
            writer = _append_lf_to_single_parquet(lf, writer, out_file)

    finally:
        if writer is not None:
            writer.close()

if __name__ == "__main__":
    out_dir = Path.cwd() / "data"

    process_parquets(out_dir, out_dir / "all_yellow_taxi.parquet", **yellow_params)
    process_parquets(out_dir, out_dir / "all_green_taxi.parquet", **green_params)
    process_parquets(out_dir, out_dir / "all_uber.parquet", sim_read=1, **uber_params)
