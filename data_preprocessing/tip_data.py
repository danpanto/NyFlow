import polars as pl
from pathlib import Path
import math
import duckdb
from datetime import datetime

ADD_COORDS_DATA = True
ADD_CLIMATE_DATA = True

data_dir = Path("data")

con = duckdb.connect("trips.duckdb")
con.execute("SET memory_limit = '10GB'")

# Cargamos el df con los centroides
df_coords = (
    pl.scan_parquet(data_dir / "map_centroids.parquet")
    .with_columns(pl.col("locationid").cast(pl.Int16))
    .filter(pl.col("locationid") < 264)
    .collect()
)
df_coords_pu = df_coords.rename(
    {"Latitude": "pickup_latitude", "Longitude": "pickup_longitude"}
)
df_coords_do = df_coords.rename(
    {"Latitude": "dropoff_latitude", "Longitude": "dropoff_longitude"}
)

landmarks = {
    "nyc": (-74.001541, 40.724944),
    "chp": (-73.137393, 41.366138),
    "exp": (-74.0375, 40.736),
}

# Datos meteorológicos
df_lluvia = pl.scan_parquet(data_dir / "23-2601_climate_hourly.parquet")
df_sol = pl.scan_parquet(data_dir / "23-2601_climate_daily.parquet")


def get_manhattan_dist_and_dir_exprs(lon1, lat1, lon2, lat2):
    c_lon1, c_lat1 = pl.col(lon1), pl.col(lat1)
    c_lon2 = pl.col(lon2) if isinstance(lon2, str) else pl.lit(lon2)
    c_lat2 = pl.col(lat2) if isinstance(lat2, str) else pl.lit(lat2)

    dlon_abs = (c_lon2 - c_lon1).abs()
    dlat_abs = (c_lat2 - c_lat1).abs()
    lat_media_rad = ((c_lat1 + c_lat2) / 2) * (math.pi / 180)

    distancia_x_metros = dlon_abs * 111320 * lat_media_rad.cos()
    distancia_y_metros = dlat_abs * 111320
    manhattan_dist_expr = distancia_x_metros + distancia_y_metros

    r_lon1, r_lat1 = c_lon1 * math.pi / 180, c_lat1 * math.pi / 180
    r_lon2_rad, r_lat2_rad = c_lon2 * math.pi / 180, c_lat2 * math.pi / 180
    dlon_rad = r_lon2_rad - r_lon1

    y = dlon_rad.sin() * r_lat2_rad.cos()
    x = (
        r_lat1.cos() * r_lat2_rad.sin()
        - r_lat1.sin() * r_lat2_rad.cos() * dlon_rad.cos()
    )
    dir_expr = pl.arctan2(y, x) * 180 / math.pi

    return manhattan_dist_expr, dir_expr


def add_base_features(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Añade variables base sin hacer cruces históricos."""
    lf = lf.join(
        df_coords_pu.lazy(), left_on="PULocationID", right_on="locationid", how="inner"
    ).join(
        df_coords_do.lazy(), left_on="DOLocationID", right_on="locationid", how="inner"
    )

    trip_dist_expr, trip_dir_expr = get_manhattan_dist_and_dir_exprs(
        "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude"
    )
    lf = lf.with_columns(distance=trip_dist_expr, direction=trip_dir_expr)

    if ADD_COORDS_DATA:
        landmark_exprs = []
        for name, (lon, lat) in landmarks.items():
            dist_pu, _ = get_manhattan_dist_and_dir_exprs(
                "pickup_longitude", "pickup_latitude", lon, lat
            )
            dist_do, _ = get_manhattan_dist_and_dir_exprs(
                "dropoff_longitude", "dropoff_latitude", lon, lat
            )
            landmark_exprs.extend(
                [
                    dist_pu.alias(f"pickup_dist_{name}"),
                    dist_do.alias(f"dropoff_dist_{name}"),
                ]
            )

        lf = lf.with_columns(*landmark_exprs)

    if ADD_CLIMATE_DATA:
        lf = lf.with_columns(
            pl.col("pickup_datetime").dt.truncate("1h").alias("hour_key")
        )
        lf = lf.join(df_lluvia, on="hour_key", how="inner")
        lf = lf.with_columns(
            temp_discomfort=(pl.col("temperature_2m") - 20).abs(),
        )

        lf = lf.with_columns(pl.col("pickup_datetime").cast(pl.Date).alias("date_key"))
        lf = lf.join(df_sol, on="date_key", how="inner")
        lf = lf.with_columns(
            is_daylight=pl.when(
                (pl.col("pickup_datetime") >= pl.col("sunrise"))
                & (pl.col("pickup_datetime") <= pl.col("sunset"))
            )
            .then(1)
            .otherwise(0)
        )

        lf = lf.drop("date_key", "hour_key", "sunrise", "sunset")

    return lf.with_columns(
        month=pl.col("pickup_datetime").dt.month(),
        dayofyear=pl.col("pickup_datetime").dt.ordinal_day(),
        weekday=pl.col("pickup_datetime").dt.weekday(),
        hour=(
            pl.col("pickup_datetime").dt.hour()
            + (pl.col("pickup_datetime").dt.minute() / 60.0)
        ),
    )  # .with_columns(pl.col("payment_type").fill_null(5).alias("payment_type"))


print("Iniciando carga unificada y recuperación de test faltante...")

# Configuración de Filtros: más o menos que las clases estén balanceadas
config = {
    "yellow": {"vendor": 0, "payment_type": 1, "sample": "5%"},  # 4.116.969
    "green": {"vendor": 1, "payment_type": 1, "sample": "100%"},  # 660.204
    "uber": {"vendor": 2, "payment_type": 7, "sample": "1%"},  # 3.582.515
    "lyft": {"vendor": 3, "payment_type": 7, "sample": "3%"},  # 3.620.679
}


def consulta_base(ruta, vendor_id, payment, sample_pct):
    return f"""
    WITH base AS (
        SELECT pickup_datetime, PULocationID, DOLocationID, tip_amount,
               VendorID, fare_amount
        FROM '{ruta}'
        WHERE (PULocationID BETWEEN 1 AND 263) AND (DOLocationID BETWEEN 1 AND 263)
        AND YEAR(pickup_datetime) >= 2023 
        AND VendorID = {vendor_id}
        AND payment_type = {payment}
    ),
    sampled AS (SELECT * FROM base USING SAMPLE {sample_pct} (System, 42)),
    guaranteed AS (SELECT * FROM base QUALIFY ROW_NUMBER() OVER (PARTITION BY PULocationID ORDER BY pickup_datetime) = 1)
    SELECT * FROM sampled UNION SELECT * FROM guaranteed
    """


# --- 1. CARGA DEL ARCHIVO PRINCIPAL (21-25) ---
list_dfs = []
for name, c in config.items():
    print(f"Cargando {name} desde clipped...")
    df_temp = con.execute(
        consulta_base(
            data_dir / "21-25_clipped.parquet",
            c["vendor"],
            c["payment_type"],
            c["sample"],
        )
    ).pl()
    list_dfs.append(df_temp)

# --- 2. CARGA DEL ARCHIVO DE TEST (Merged) ---
# Aquí solo buscamos lo que falta (Yellow y Green para las fechas de test)
print("Recuperando test de Yellow/Green desde el archivo merged...")
df_test_extra = con.execute(f"""
    SELECT pickup_datetime, PULocationID, DOLocationID, tip_amount, VendorID, fare_amount
    FROM '{data_dir / "20260423_090829_merged.parquet"}'
    WHERE (PULocationID BETWEEN 1 AND 263) AND (DOLocationID BETWEEN 1 AND 263)
        AND payment_type = 1
""").pl()

# --- 3. UNIFICACIÓN TOTAL ---
df_final_lazy = pl.concat([df.lazy() for df in list_dfs] + [df_test_extra.lazy()])

# --- 4. INGENIERÍA DE CARACTERÍSTICAS ---
print("Aplicando ingeniería de características a todo el bloque...")
df_all_base = add_base_features(df_final_lazy)

# --- 5. DIVISIÓN TEMPORAL ---
val_date = datetime(2025, 11, 1)
test_date = datetime(2025, 12, 1)
cols_to_drop = ["pickup_datetime"]

print("Separando sets finales...")
df_train = df_all_base.filter(pl.col("pickup_datetime") < val_date).drop(cols_to_drop)
df_val = df_all_base.filter(
    (pl.col("pickup_datetime") >= val_date) & (pl.col("pickup_datetime") < test_date)
).drop(cols_to_drop)
df_test = df_all_base.filter(pl.col("pickup_datetime") >= test_date).drop(cols_to_drop)

# --- 6. GUARDADO ---
print("Guardando Parquets unificados...")
df_train.sink_parquet(data_dir / "train_tip_final_unificado_ligero.parquet")
df_val.sink_parquet(data_dir / "val_tip_final_unificado_ligero.parquet")
df_test.sink_parquet(data_dir / "test_tip_final_unificado_ligero.parquet")

print("¡Procesamiento terminado! Tienes todo junto, sin huecos en el test.")
