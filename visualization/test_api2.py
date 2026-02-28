import geopandas as gpd
import math

try:
    gdf = gpd.read_file('data/restaurant_info.geojson')
    print("Loaded gdf.")
    print("Scores sample:", gdf['SCORE'].head(5).tolist())
    print("Types of SCORE:", type(gdf['SCORE'].iloc[0]))
    
    # Simulate API loop
    for x, y, name, score in zip(gdf.geometry.centroid.x, gdf.geometry.centroid.y, gdf['DBA'], gdf['SCORE']):
        # Trying to convert to float
        s = float(score) if score is not None and not math.isnan(float(score)) else None
except Exception as e:
    print("Error:", e)
