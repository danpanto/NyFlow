import polars as pl
from pathlib import Path
import math
from pyproj import Geod

data_dir = Path("data") # carpeta con los parquets

import duckdb
con = duckdb.connect("trips.duckdb")
con.execute("SET memory_limit = '10GB'")

def shared_features_lazy(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Añade variables de tiempo y distancia geodésica usando Polars LazyFrames."""
    
    landmarks = {
        "nyc": (-74.001541, 40.724944), # Zona centro
        "chp": (-73.137393, 41.366138), # Connecticut
        "exp": (-74.0375, 40.736) # Hoboken
    }

    # Helper para generar las expresiones matemáticas de Distancia y Dirección
    def get_dist_and_dir_exprs(lon1, lat1, lon2, lat2):
        r_lon1 = pl.col(lon1) * math.pi / 180
        r_lat1 = pl.col(lat1) * math.pi / 180
        r_lon2 = (pl.col(lon2) if isinstance(lon2, str) else pl.lit(lon2)) * math.pi / 180
        r_lat2 = (pl.col(lat2) if isinstance(lat2, str) else pl.lit(lat2)) * math.pi / 180
        
        dlon = r_lon2 - r_lon1
        dlat = r_lat2 - r_lat1
        
        a = (dlat / 2).sin()**2 + r_lat1.cos() * r_lat2.cos() * (dlon / 2).sin()**2
        dist_expr = 2 * a.sqrt().arcsin() * 6371000
        
        y = dlon.sin() * r_lat2.cos()
        x = r_lat1.cos() * r_lat2.sin() - r_lat1.sin() * r_lat2.cos() * dlon.cos()
        
        dir_expr = pl.arctan2(y, x) * 180 / math.pi
        
        return dist_expr, dir_expr

    trip_dist_expr, trip_dir_expr = get_dist_and_dir_exprs(
        "pickup_longitude", "pickup_latitude", 
        "dropoff_longitude", "dropoff_latitude"
    )

    landmark_exprs = []
    for name, (lon, lat) in landmarks.items():
        dist_pickup, _ = get_dist_and_dir_exprs("pickup_longitude", "pickup_latitude", lon, lat)
        dist_dropoff, _ = get_dist_and_dir_exprs("dropoff_longitude", "dropoff_latitude", lon, lat)
        
        landmark_exprs.extend([
            dist_pickup.alias(f"pickup_dist_{name}"),
            dist_dropoff.alias(f"dropoff_dist_{name}")
        ])

    return (
        lf.with_columns(
            # --- Variables de Tiempo ---
            month=pl.col("pickup_datetime").dt.month(),
            dayofyear=pl.col("pickup_datetime").dt.ordinal_day(),
            weekday=pl.col("pickup_datetime").dt.weekday(), # ATENCIÓN: Polars dt.weekday() es 1=Lunes, 7=Domingo
            hour=(pl.col("pickup_datetime").dt.hour() + 
                (pl.col("pickup_datetime").dt.minute() / 60.0)),

            # --- Distancia y Dirección del Viaje ---
            distance=trip_dist_expr,
            direction=trip_dir_expr,
            
            # --- Distancias a Landmarks ---
            *landmark_exprs
        )        
        # --- Transformaciones Cíclicas (Seno y Coseno) ---
        .with_columns(
            # sin_time=(2 * math.pi * pl.col("hour") / 24).sin(),
            # cos_time=(2 * math.pi * pl.col("hour") / 24).cos(),

            # sin_direction=(2 * math.pi * pl.col("direction") / 360).sin(),
            # cos_direction=(2 * math.pi * pl.col("direction") / 360).cos(),

            # sin_dayofyear=(2 * math.pi * pl.col("dayofyear") / 365).sin(),
            # cos_dayofyear=(2 * math.pi * pl.col("dayofyear") / 365).cos(),

            # sin_weekday=(2 * math.pi * pl.col("weekday") / 7).sin(),
            # cos_weekday=(2 * math.pi * pl.col("weekday") / 7).cos(),

            # Buckets de dirección: la función devuelve azimut entre -180 y 180.
            # Lo desplazamos a 0-360 y lo dividimos en 37 tramos para imitar pd.cut
            direction_bucket=((pl.col("direction") + 180) / (360.0 / 37)).cast(pl.Int32)
        )
        
        # --- Cálculo de adj_dist basado en tip_amount ---
        # 1. Propina por km (con suavizado de +5)
        .with_columns(
            tip_per_km=(pl.col("tip_amount") * 1000) / (pl.col("distance") + 5)
        )
        # 2. Filtramos internamente (solo usamos distance > 5 para la media)
        .with_columns(
            valid_tip=pl.when(pl.col("distance") > 5).then(pl.col("tip_per_km")).otherwise(None)
        )
        # 3. Calculamos la media agrupada por bucket (Window function)
        .with_columns(
            mean_tip_per_bucket=pl.col("valid_tip").mean().over("direction_bucket")
        )
        # 4. Calculamos la distancia ajustada final 
        # (El .max() sin .over() busca automáticamente el máximo de toda la columna)
        .with_columns(
            adj_dist=(pl.col("mean_tip_per_bucket") * pl.col("distance")) / pl.col("mean_tip_per_bucket").max()
        )
        .drop([
            #"hour", "direction", "weekday", "dayofyear", 
            "direction_bucket", "tip_per_km", "valid_tip", "mean_tip_per_bucket"
        ])
    )


df_agg = con.execute(f"""
    SELECT pickup_datetime, PULocationID, DOLocationID, tip_amount,
    payment_type, VendorID FROM '{data_dir / "21-25_clipped.parquet"}'
    WHERE YEAR(pickup_datetime) >= 2023
    USING SAMPLE 1% (System, 42)
""").pl().lazy()

# Marcar los viajes desde y hacia aeropuertos
airport_ids = [1, 4, 132]

df_agg = df_agg.with_columns(
    PUis_airport=pl.col("PULocationID").is_in(airport_ids).cast(pl.Int8),
    DOis_airport=pl.col("DOLocationID").is_in(airport_ids).cast(pl.Int8),
)

# Añadir información geográfica
df_coords = pl.scan_parquet(data_dir / "map_centroids.parquet")
df_coords = df_coords.with_columns(pl.col("locationid").cast(pl.Int16)).filter(pl.col("locationid") < 264)
df_coords_pu = df_coords.rename({
    "Latitude": "pickup_latitude",
    "Longitude": "pickup_longitude"
})

df_coords_do = df_coords.rename({
    "Latitude": "dropoff_latitude",
    "Longitude": "dropoff_longitude"
})

df_agg = (
    df_agg
    .join(df_coords_pu, left_on="PULocationID", right_on="locationid", how="inner")
    .join(df_coords_do, left_on="DOLocationID", right_on="locationid", how="inner")
)

# Añadir datos de la bolsa
df_cnn = pl.scan_parquet(data_dir / "cnn_fear_index.parquet")
df_cnn = df_cnn.with_columns(day_key=pl.col("Date").dt.truncate("1d").dt.date())
df_cnn = df_cnn.unique(subset=["day_key"], keep="last").select(["day_key", "cnn_fear_index"])
df_agg = df_agg.with_columns(day_key=pl.col("pickup_datetime").dt.truncate("1d").dt.date())

df_agg = df_agg.join(
    df_cnn,
    on="day_key",
    how="inner"
).drop("day_key")

# Añadir datos climatológicos
df_clima = pl.scan_parquet(data_dir / "23-2601_climate.parquet")
df_agg = df_agg.with_columns(day_key=pl.col("pickup_datetime").dt.truncate("1d").dt.date())
df_agg = df_agg.join(
    df_clima,
    on="day_key",
    how="inner"
).drop("day_key")

# Añadir información de la renta
df_rent = pl.scan_parquet(data_dir / "asking_rent_data.parquet")
df_rent = df_rent.with_columns(pl.col("LocationID").cast(pl.Int16))
df_rent = df_rent.with_columns(
    month_key=pl.col("Date").dt.truncate("1mo")
)
df_rent = df_rent.drop("Date")
df_rent_pu = df_rent.rename({"AskingRent": "PUAskingRent"})
df_rent_do = df_rent.rename({"AskingRent": "DOAskingRent"})

df_agg = df_agg.with_columns(
    month_key=pl.col("pickup_datetime").dt.truncate("1mo")
)
df_agg = (
    df_agg
    .join(df_rent_pu, left_on=["month_key","PULocationID"], right_on=["month_key","LocationID"], how="left")
    .join(df_rent_do, left_on=["month_key","DOLocationID"], right_on=["month_key","LocationID"], how="left")
)
df_agg = df_agg.drop("month_key")

# Añadir datos de landmarks
df_landmarks = pl.scan_parquet(data_dir / "landmarks_per_zone.parquet")
df_landmarks = df_landmarks.with_columns(pl.col("locationid").cast(pl.Int16))
df_landmarks_pu = df_landmarks.rename({"landmark_count": "pu_landmark_count"})
df_landmarks_do = df_landmarks.rename({"landmark_count": "do_landmark_count"})
df_agg = (
    df_agg
    .join(df_landmarks_pu, left_on="PULocationID", right_on="locationid", how="left")
    .join(df_landmarks_do, left_on="DOLocationID", right_on="locationid", how="left")
    .with_columns(
        pu_landmark_count=pl.col("pu_landmark_count").fill_null(0).cast(pl.Int16),
        do_landmark_count=pl.col("do_landmark_count").fill_null(0).cast(pl.Int16)
    )
)

# Añadir distancias
df_agg = shared_features_lazy(df_agg)

df_final = df_agg.collect()
df_final.write_parquet(data_dir / "datos_tip_all.parquet")

print("¡Procesamiento finalizado y datos guardados!")