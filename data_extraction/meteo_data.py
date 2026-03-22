import polars as pl
import requests

# 1. Configuramos la petición a la API de Open-Meteo (Historical)
# Usamos las coordenadas de Central Park (Nueva York)
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": 40.7831,
    "longitude": -73.9712,
    "start_date": "2023-01-01",
    "end_date": "2026-01-31",
    "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "sunshine_duration"],
    "timezone": "America/New_York"
}

# 2. Hacemos la llamada y extraemos el bloque 'daily'
respuesta = requests.get(url, params=params).json()
datos_diarios = respuesta["daily"]

# 3. Lo convertimos a Polars directamente
df_clima = pl.DataFrame(datos_diarios)

# 4. Limpiamos y creamos las columnas útiles para el modelo
df_clima = df_clima.with_columns(
    # Convertimos la fecha a tipo Date para el Join
    day_key=pl.col("time").cast(pl.Date),
    
    # La API da el sol en segundos, lo pasamos a horas para que el modelo lo entienda mejor
    horas_sol=(pl.col("sunshine_duration") / 3600).round(1),
    
    # Creamos un flag: ¿Llovió este día? (1 sí, 0 no)
    lluvia_flag=pl.when(pl.col("precipitation_sum") > 0).then(1).otherwise(0).cast(pl.Int8)
).drop(["time", "sunshine_duration"])

print(df_clima.head())

df_clima.write_parquet(path / "data" / "23-2601_climate.parquet")