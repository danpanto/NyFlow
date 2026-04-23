import polars as pl
import requests
from pathlib import Path

# Configuramos la ruta de guardado
path = Path.cwd()
if not Path(path, "data").exists():
    path = path.parent

# Configuramos la petición a la API de Open-Meteo (Historical)
# Usamos las coordenadas de Central Park (Nueva York)
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": 40.7831,
    "longitude": -73.9712,
    "start_date": "2023-01-01",
    "end_date": "2026-01-31",
    "hourly": ["temperature_2m", "precipitation", "rain", "snowfall"],
    "daily": ["sunrise", "sunset", "daylight_duration"],
    "timezone": "America/New_York",
}

respuesta = requests.get(url, params=params).json()
datos_horarios = respuesta["hourly"]

df_clima = pl.DataFrame(datos_horarios)

df_clima = df_clima.with_columns(
    hour_key=pl.col("time").cast(pl.Datetime),
).drop(["time"])

ruta_guardado = path / "data" / "23-2601_climate_hourly.parquet"
df_clima.write_parquet(ruta_guardado)


df_clima = pl.DataFrame(respuesta["daily"])
df_clima = df_clima.with_columns(
    date_key=pl.col("time").cast(pl.Date),
    sunrise=pl.col("sunrise").cast(pl.Datetime),
    sunset=pl.col("sunset").cast(pl.Datetime)
).drop(["time"])
ruta_guardado2 = path / "data" / "23-2601_climate_daily.parquet"
df_clima.write_parquet(ruta_guardado2)
print(df_clima.head())

print(f"✅ Datos meteorológicos horarios guardados en: {ruta_guardado, ruta_guardado2}")
