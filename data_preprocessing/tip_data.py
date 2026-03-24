import polars as pl
from pathlib import Path
import math
import duckdb

data_dir = Path("data")

con = duckdb.connect("trips.duckdb")
con.execute("SET memory_limit = '10GB'")

# 1. Cargamos el df con los centroides (Hacemos collect porque es diminuto y acelera los joins)
df_coords = pl.scan_parquet(data_dir / "map_centroids.parquet").with_columns(pl.col("locationid").cast(pl.Int16)).filter(pl.col("locationid") < 264).collect()
df_coords_pu = df_coords.rename({"Latitude": "pickup_latitude", "Longitude": "pickup_longitude"})
df_coords_do = df_coords.rename({"Latitude": "dropoff_latitude", "Longitude": "dropoff_longitude"})

landmarks = {
    "nyc": (-74.001541, 40.724944), 
    "chp": (-73.137393, 41.366138), 
    "exp": (-74.0375, 40.736) 
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
    x = r_lat1.cos() * r_lat2_rad.sin() - r_lat1.sin() * r_lat2_rad.cos() * dlon_rad.cos()
    dir_expr = pl.arctan2(y, x) * 180 / math.pi
    
    return manhattan_dist_expr, dir_expr

def add_base_features(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Añade variables base sin hacer cruces históricos."""
    lf = (
        lf
        .join(df_coords_pu.lazy(), left_on="PULocationID", right_on="locationid", how="inner")
        .join(df_coords_do.lazy(), left_on="DOLocationID", right_on="locationid", how="inner")
    )

    trip_dist_expr, trip_dir_expr = get_manhattan_dist_and_dir_exprs(
        "pickup_longitude", "pickup_latitude", 
        "dropoff_longitude", "dropoff_latitude"
    )

    landmark_exprs = []
    for name, (lon, lat) in landmarks.items():
        dist_pu, _ = get_manhattan_dist_and_dir_exprs("pickup_longitude", "pickup_latitude", lon, lat)
        dist_do, _ = get_manhattan_dist_and_dir_exprs("dropoff_longitude", "dropoff_latitude", lon, lat)
        landmark_exprs.extend([dist_pu.alias(f"pickup_dist_{name}"), dist_do.alias(f"dropoff_dist_{name}")])

    return lf.with_columns(
        distance=trip_dist_expr,
        direction=trip_dir_expr,
        *landmark_exprs
    ).with_columns(
        direction_bucket=((pl.col("direction") + 180) / (360.0 / 37)).cast(pl.Int32),
        month=pl.col("pickup_datetime").dt.month(),
        dayofyear=pl.col("pickup_datetime").dt.ordinal_day(),
        weekday=pl.col("pickup_datetime").dt.weekday(),
        hour=(pl.col("pickup_datetime").dt.hour() + (pl.col("pickup_datetime").dt.minute() / 60.0)),
        trip_duration_min=(pl.col("dropoff_datetime") - pl.col("pickup_datetime")).dt.total_minutes()
    )


# --- CARGA DE DATOS ---
df_train = con.execute(f"""
    SELECT pickup_datetime, dropoff_datetime, PULocationID, DOLocationID, tip_amount,
    payment_type, VendorID FROM '{data_dir / "21-25_clipped.parquet"}'
    WHERE YEAR(pickup_datetime) >= 2023 AND pickup_datetime < '2025-11-01'
    USING SAMPLE 2% (System, 42)
""").pl().lazy()

df_test = con.execute(f"""
    SELECT pickup_datetime, dropoff_datetime, PULocationID, DOLocationID, tip_amount,
    payment_type, VendorID FROM '{data_dir / "21-25_clipped.parquet"}'
    WHERE pickup_datetime >= '2025-11-01'
    USING SAMPLE 10% (System, 42)
""").pl().lazy()

# --- PREPARAR VARIABLES BASE ---
print("Extrayendo variables base...")
df_train_base = add_base_features(df_train)
df_test_base = add_base_features(df_test)

# --- CREAR DICCIONARIOS SOLO CON TRAIN Y MATERIALIZARLOS (.collect()) ---
print("Creando diccionarios de Train (Target Encoding y ETAs)...")

# Diccionario de Direcciones
diccionario_direcciones = (
    df_train_base
    .filter(pl.col("distance") > 5)
    .with_columns(tip_per_km=(pl.col("tip_amount") * 1000) / (pl.col("distance") + 5))
    .group_by("direction_bucket")
    .agg(pl.col("tip_per_km").mean().alias("mean_tip_per_bucket_TRAIN"))
).collect()

max_tip_mean = diccionario_direcciones["mean_tip_per_bucket_TRAIN"].max()

# Diccionario de ETAs (Tiempos esperados)
tiempos_esperados = (
    df_train_base
    .group_by(["PULocationID", "DOLocationID", "weekday", "hour"])
    .agg(pl.col("trip_duration_min").median().alias("expected_trip_duration"))
).collect()


def apply_historical_knowledge(lf: pl.LazyFrame) -> pl.LazyFrame:
    lf = lf.join(diccionario_direcciones.lazy(), on="direction_bucket", how="left")
    lf = lf.with_columns(
        pl.col("mean_tip_per_bucket_TRAIN").fill_null(0)
    ).with_columns(
        adj_dist=(pl.col("mean_tip_per_bucket_TRAIN") * pl.col("distance")) / max_tip_mean
    ).drop(["direction_bucket", "mean_tip_per_bucket_TRAIN"])

    lf = lf.join(tiempos_esperados.lazy(), on=["PULocationID", "DOLocationID", "weekday", "hour"], how="left")
    
    lf = lf.with_columns(
        pl.col("expected_trip_duration").fill_null(pl.col("trip_duration_min"))
    )
    
    return lf.with_columns(
        diff_eta = pl.col("trip_duration_min") - pl.col("expected_trip_duration"),
        delay_ratio = pl.col("trip_duration_min") / (pl.col("expected_trip_duration") + 0.01)
    ).drop(["expected_trip_duration", "pickup_datetime", "dropoff_datetime"])

# --- APLICAR A TRAIN Y TEST Y GUARDAR ---
print("Aplicando histórico y guardando Parquets...")
df_train_final = apply_historical_knowledge(df_train_base)
df_train_final.sink_parquet(data_dir / "train_tip.parquet")

df_test_final = apply_historical_knowledge(df_test_base)
df_test_final.sink_parquet(data_dir / "test_tip.parquet")

print("¡Procesamiento finalizado y datos guardados de forma segura!")