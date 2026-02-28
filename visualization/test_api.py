import asyncio
from fastapi import FastAPI
from app.setup import lifespan
import math

async def test():
    app = FastAPI(lifespan=lifespan)
    async with lifespan(app):
        gdf = app.state.gdf_restaurants
        try:
            print(gdf.geometry.centroid.x[:5])
            print("Loop test:")
            count = 0
            for x, y, name, score in zip(gdf.geometry.centroid.x, gdf.geometry.centroid.y, gdf['DBA'], gdf['SCORE']):
                if count < 5:
                    print(x, y, name, score)
                count += 1
            print("Success")
        except Exception as e:
            print("Error:", e)

if __name__ == "__main__":
    asyncio.run(test())
