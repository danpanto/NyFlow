import pandas as pd
from datetime import datetime, timezone
import requests
import json

def extract_fagi(start, end):
    start = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Referer": "https://cnn.com/",
        "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    }
    current = start
    df_ret = pd.DataFrame()
    while(current < end):
        url = f"https://production.dataviz.cnn.io/index/fearandgreed/graphdata/{current.strftime("%Y-%m-%d")}"
        r = requests.get(url, headers=headers, timeout=10)
        data = pd.DataFrame(json.loads(r.text)['fear_and_greed_historical']['data'])
        data["Date"] = pd.to_datetime(data["x"] / 1000, unit="s", utc=True).dt.ceil('D')
        current = data["Date"].max()
        df_ret = pd.concat([df_ret, data], ignore_index=True) 
    
    return df_ret


from pathlib import Path
path = Path.cwd()
if not Path(path, "data").exists(): path = path.parent

df_fagi = extract_fagi("2023-01-01", "2026-02-01")
df_final = df_fagi.drop(["x", "rating"], axis=1).rename(columns = {"y": "cnn_fear_index"})
df_filled = (
    df_final.set_index('Date')  # Ponemos la fecha como índice para poder usar resample
    .resample('D')        # 'D' significa frecuencia diaria (crea las filas faltantes)
    .ffill()              # Forward fill: rellena los huecos con el último valor válido
    .reset_index()        # Devolvemos la fecha a ser una columna normal
)
df_filled.to_parquet(path / "data" / "cnn_fear_index.parquet", index=False)