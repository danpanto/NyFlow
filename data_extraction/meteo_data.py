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
    # ¡Cambiamos a 'hourly' para tener precisión por hora!
    "hourly": ["temperature_2m", "precipitation", "rain", "snowfall"],
    "timezone": "America/New_York",
}

respuesta = requests.get(url, params=params).json()
datos_horarios = respuesta["hourly"]

df_clima = pl.DataFrame(datos_horarios)

df_clima = df_clima.with_columns(
    hour_key=pl.col("time").cast(pl.Datetime),
).drop(["time"])
print(df_clima.head())

ruta_guardado = path / "data" / "23-2601_climate_hourly.parquet"
df_clima.write_parquet(ruta_guardado)

print(f"✅ Datos meteorológicos horarios guardados en: {ruta_guardado}")
