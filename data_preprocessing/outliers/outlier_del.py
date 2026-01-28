import duckdb
from pathlib import Path
import pandas as pd

path = Path.cwd() / "data"

yellow_columns = [
    "trip_distance", 
    "fare_amount", 
    "total_amount", 
    "tip_amount",
]

green_columns = [

]

hv_columns = [

]

def get_stats(con, file_path, columns):
    select_parts = []
    for col in columns:
        select_parts.append(f"approx_quantile({col}, 0.25) as {col}_q1")
        select_parts.append(f"approx_quantile({col}, 0.75) as {col}_q3")

    query_stats = f"SELECT {', '.join(select_parts)} FROM '{file_path}'"

    return con.execute(query_stats).fetchone()


def get_map_stats(stats_result, columns):
    stats_map = {}
    idx = 0
    for col in columns:
        q1 = stats_result[idx]
        q3 = stats_result[idx+1]
        stats_map[col] = {'q1': q1, 'q3': q3}
        idx += 2
    return stats_map


def print_results(con, input_path, output_path):
    filas_final = con.execute(f"SELECT COUNT(*) FROM '{output_path}'").fetchone()[0]
    filas_iniciales = con.execute(f"SELECT COUNT(*) FROM '{input_path}'").fetchone()[0]
    perdida = filas_iniciales - filas_final
    porcentaje = (filas_final / filas_iniciales) * 100

    print(f"\t✅ Filas en el dataset inicial: {filas_iniciales:,.0f}\n",
          f"\t✅ Filas en el dataset limpio: {filas_final:,.0f}\n",
          f"\t📊 Se han mantenido el {porcentaje:.2f}% de los datos.\n",
          f"\t🗑️ Se han eliminado {perdida:,.0f} outliers.\n"
    )


if __name__ == "__main__":
    input_paths = [
        path / "yellow_taxi_unified.parquet", 
        #path / "green_taxi_unified.parquet", 
        #path / "hv_taxi_unified.parquet"
    ]

    output_paths = [
        path / "yellow_taxi_no_outliers.parquet", 
        path / "green_taxi_no_outliers.parquet", 
        path / "hv_taxi_no_outliers.parquet"
    ]

    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")

    for p in input_paths:
        print(f"PROCESANDO {p.name}...\n")
        print("\tOBTENIENDO ESTADÍSTICAS...")
        stats_result = get_stats(con, p, yellow_columns)
        print("\tOBTENIENDO MAPA DE ESTADÍSTICAS...")
        stats_map = get_map_stats(stats_result, yellow_columns)

        print("\tCONSTRUYENDO CONDICIONES DE FILTRADO...")
        where_conditions = []
        for col in yellow_columns:
            stats = stats_map[col]
            iqr = stats['q3'] - stats['q1']

            if iqr <= 0.0001:
                continue

            lower = max(0, stats['q1'] - 1.5 * iqr)
            upper = stats['q3'] + 1.5 * iqr
            
            where_conditions.append(f"({col} >= {lower} AND {col} <= {upper})")
        
        print(where_conditions)
        
        final_filter = " AND ".join(where_conditions)

        copy_query = f"""
        COPY (
            SELECT * FROM '{p}' 
            WHERE {final_filter}
        ) TO '{output_paths[input_paths.index(p)]}' (FORMAT 'PARQUET', CODEC 'SNAPPY');
        """

        print("\tELIMINANDO OUTLIERS Y GUARDANDO DATASET LIMPIO...")
        con.execute(copy_query)

        print_results(con, p, output_paths[input_paths.index(p)])