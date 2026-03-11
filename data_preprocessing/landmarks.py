import pandas as pd
import requests
import re
from io import StringIO


url = "https://en.wikipedia.org/wiki/List_of_National_Historic_Landmarks_in_New_York_City"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

response = requests.get(url, headers=headers)

tablas = pd.read_html(StringIO(response.text))

df_landmarks = tablas[1]


# Parsear cordenadas de los monumentos
extraccion = df_landmarks['Location'].str.extract(r"/\s*\ufeff?(\d+\.\d+)°N\s+(\d+\.\d+)°W")

df_landmarks['Latitude'] = extraccion[0].astype(float)

df_landmarks['Longitude'] = extraccion[1].astype(float) * -1 

guardar = df_landmarks[['Landmark name', 'Latitude', 'Longitude']]

#Guardarlo
guardar.to_parquet("../data/landmarks.parquet", index=False)