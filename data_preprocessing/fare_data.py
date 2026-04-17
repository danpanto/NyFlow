import polars as pl
from pathlib import Path
import math
import duckdb
from datetime import datetime

data_dir = Path("data")

con = duckdb.connect("trips.duckdb")
con.execute("SET memory_limit = '10GB'")

# df con datos meteorológicos
df_lluvia = pl.scan_parquet(data_dir / "23-2601_climate_hourly.parquet")


def add_base_features(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Añade variables base sin hacer cruces históricos."""
    # Datos meteo
    lf = lf.with_columns(pl.col("pickup_datetime").dt.truncate("1h").alias("hour_key"))
    lf = lf.join(df_lluvia, on="hour_key", how="left")

    # Datos espaciales

    return (
        lf.with_columns(
            month=pl.col("pickup_datetime").dt.month(),
            dayofyear=pl.col("pickup_datetime").dt.ordinal_day(),
            weekday=pl.col("pickup_datetime").dt.weekday(),
            hour=(
                pl.col("pickup_datetime").dt.hour()
                + (pl.col("pickup_datetime").dt.minute() / 60.0)
            ),
        )
        .with_columns(payment_type=pl.col("payment_type").fill_null(5))
        .drop("hour_key")
    )


# --- CARGA DE DATOS ---
print("Muestreando y cargando datos unificados...")
df_all = (
    con.execute(f"""
    SELECT pickup_datetime, PULocationID, DOLocationID, fare_amount,
    payment_type, VendorID, trip_distance 
    FROM '{data_dir / "21-25_clipped.parquet"}'
    WHERE YEAR(pickup_datetime) >= 2023
    USING SAMPLE 2% (System, 42)
""")
    .pl()
    .lazy()
)

print("Aplicando ingeniería de características...")
df_all_base = add_base_features(df_all)


print("Dividiendo los datos por fechas...")
val_date = datetime(2025, 11, 1)
test_date = datetime(2025, 12, 1)
cols_to_drop = ["pickup_datetime"]

# Filtramos usando Polars y encadenamos el drop de las fechas que ya no necesitamos
df_train = df_all_base.filter(pl.col("pickup_datetime") < val_date).drop(cols_to_drop)

df_val = df_all_base.filter(
    (pl.col("pickup_datetime") >= val_date) & (pl.col("pickup_datetime") < test_date)
).drop(cols_to_drop)

df_test = df_all_base.filter(pl.col("pickup_datetime") >= test_date).drop(cols_to_drop)


# --- GUARDAR LOS PARQUETS ---
print("Guardando Parquets...")
df_train.sink_parquet(data_dir / "train_fare.parquet")
df_val.sink_parquet(data_dir / "val_fare.parquet")
df_test.sink_parquet(data_dir / "test_fare.parquet")

print("¡Procesamiento finalizado y datos guardados!")
