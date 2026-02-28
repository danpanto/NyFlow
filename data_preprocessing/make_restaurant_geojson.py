from pathlib import Path
import sys
import pandas as pd
import geopandas as gpd

path = Path.cwd()
if not Path(path, "data").exists(): path = path.parent
df = pd.read_csv(path / "data" / "restaurant_data.csv")
new_df = df[["DBA", "SCORE", "Latitude", "Longitude"]]
new_df["Latitude"] = new_df.Latitude.str.replace(",", ".").astype(float)
new_df["Longitude"] = new_df.Longitude.str.replace(",", ".").astype(float)
new_df.dropna(inplace=True)
new_df.drop_duplicates(inplace=True)

gdf = gpd.GeoDataFrame(
    new_df[["DBA", "SCORE"]],
    geometry=gpd.points_from_xy(new_df.Longitude, new_df.Latitude),
    crs="EPSG:4326"
)

gdf.to_file(path / "data" / "restaurant_info.geojson", driver="GeoJSON")