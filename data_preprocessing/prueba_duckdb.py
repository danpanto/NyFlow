import duckdb

con = duckdb.connect("trips.duckdb")
con.execute("SET memory_limit = '10GB'")

path = "data/trayectos.parquet"

res = con.execute(f"""
    SELECT
      MIN(trip_distance) AS min_trip_distance,
      MAX(trip_distance) AS max_trip_distance
    FROM '{path}'
""").fetchone()
print(res)
