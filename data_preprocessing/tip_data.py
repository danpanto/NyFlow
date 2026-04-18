import polars as pl
from pathlib import Path
import math
import duckdb
from datetime import datetime

ADD_COORDS_DATA = False

data_dir = Path("data")

con = duckdb.connect("trips.duckdb")
con.execute("SET memory_limit = '10GB'")

# 1. Cargamos el df con los centroides (Hacemos collect porque es diminuto y acelera los joins)
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
    if ADD_COORDS_DATA:
        lf = lf.join(
            df_coords_pu.lazy(), left_on="PULocationID", right_on="locationid", how="inner"
        ).join(
            df_coords_do.lazy(), left_on="DOLocationID", right_on="locationid", how="inner"
        )

        trip_dist_expr, trip_dir_expr = get_manhattan_dist_and_dir_exprs(
            "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude"
        )

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

        lf = lf.with_columns(
            distance=trip_dist_expr, direction=trip_dir_expr, *landmark_exprs
        )

    

    return lf.with_columns(
        month=pl.col("pickup_datetime").dt.month(),
        dayofyear=pl.col("pickup_datetime").dt.ordinal_day(),
        weekday=pl.col("pickup_datetime").dt.weekday(),
        hour=(
            pl.col("pickup_datetime").dt.hour()
            + (pl.col("pickup_datetime").dt.minute() / 60.0)
        )
    )


# --- CARGA DE DATOS ---
print("Muestreando y cargando datos unificados...")
df_all = (
    con.execute(f"""
    WITH sample_data AS (
        SELECT pickup_datetime, PULocationID, DOLocationID, tip_amount,
               payment_type, VendorID
        FROM '{data_dir / "21-25_clipped.parquet"}'
        WHERE YEAR(pickup_datetime) >= 2023
        USING SAMPLE 2% (System, 42)
    ),
    guaranteed_locations AS (
        -- Garantizamos 1 fila para cada PULocationID del 1 al 263
        SELECT pickup_datetime, PULocationID, DOLocationID, tip_amount,
               payment_type, VendorID
        FROM '{data_dir / "21-25_clipped.parquet"}'
        WHERE YEAR(pickup_datetime) >= 2023 
          AND PULocationID BETWEEN 1 AND 263
        -- QUALIFY actúa como un filtro sobre funciones de ventana
        QUALIFY ROW_NUMBER() OVER (PARTITION BY PULocationID ORDER BY pickup_datetime) = 1
    )
    
    SELECT * FROM sample_data
    UNION 
    SELECT * FROM guaranteed_locations
    """)
    .pl()
    .lazy()
)

print("Aplicando ingeniería de características...")
df_all_base = add_base_features(df_all)


print("Dividiendo los datos por fechas...")
val_date = datetime(2025, 11, 1)
test_date = datetime(2025, 12, 1)
cols_to_drop = []

# Filtramos usando Polars y encadenamos el drop de las fechas que ya no necesitamos
df_train = df_all_base.filter(pl.col("pickup_datetime") < val_date).drop(cols_to_drop)

df_val = df_all_base.filter(
    (pl.col("pickup_datetime") >= val_date) & (pl.col("pickup_datetime") < test_date)
).drop(cols_to_drop)

df_test = df_all_base.filter(pl.col("pickup_datetime") >= test_date).drop(cols_to_drop)


# --- GUARDAR LOS PARQUETS ---
print("Guardando Parquets...")
df_train.sink_parquet(data_dir / "train_tip_new_4.parquet")
df_val.sink_parquet(data_dir / "val_tip_new_4.parquet")
df_test.sink_parquet(data_dir / "test_tip_new_4.parquet")

print("¡Procesamiento finalizado y datos guardados!")