import duckdb
from pathlib import Path
import pandas as pd
from sklearn.ensemble import IsolationForest
import shutil
import time

PATH_DATA = Path.cwd() / "data"

# 1. Percentiles: Cortamos el 1% más extremo
PERC_LOW = 0.1 
PERC_HIGH = 0.99

# 2. IQR: Usamos 3.5 para ser tolerantes, ya que luego pasaremos el Isolation Forest
IQR_FACTOR = 3.5 

# 3. Isolation Forest: Eliminará el 1% restante de outliers
ISO_CONTAMINATION = 0.01
ISO_SAMPLE_SIZE = 200_000

# Columnas Numéricas a analizar
yellow_taxi_cols = [
    "trip_distance", "fare_amount", "extra",
]

green_taxi_cols = [

]

hv_taxi_cols = [

]


def get_combined_limits(con, file_path, columns):
    """Calcula los límites de IQR y Percentiles simultáneamente"""
    print(f"\t[CALCULANDO LÍMITES] Percentiles ({PERC_LOW}-{PERC_HIGH}) + IQR ({IQR_FACTOR})...")
    
    selects = []
    for col in columns:
        # IQR stats
        selects.append(f"approx_quantile({col}, 0.25) as {col}_q1")
        selects.append(f"approx_quantile({col}, 0.75) as {col}_q3")
        # Percentile stats
        selects.append(f"approx_quantile({col}, {PERC_LOW}) as {col}_p_min")
        selects.append(f"approx_quantile({col}, {PERC_HIGH}) as {col}_p_max")
        
    row = con.execute(f"SELECT {', '.join(selects)} FROM '{file_path}'").fetchone()
    
    # Organizar resultados
    limits = {}
    idx = 0
    for col in columns:
        # Extraer valores
        q1, q3 = row[idx], row[idx+1]
        p_min, p_max = row[idx+2], row[idx+3]
        idx += 4
        
        # Calcular límites IQR
        iqr = q3 - q1
        iqr_lower = q1 - (IQR_FACTOR * iqr)
        iqr_upper = q3 + (IQR_FACTOR * iqr)
        
        final_lower = max(0, iqr_lower, p_min)
        final_upper = min(iqr_upper, p_max)
        
        limits[col] = (final_lower, final_upper)
        print(f"\t  -> {col}: [{final_lower:.2f} - {final_upper:.2f}]")
        
    return limits

def step_1_sql_filtering(con, input_path, intermediate_path, limits):
    """Aplica IQR y Percentiles usando DuckDB"""
    print("\n\t[PASO 1] Aplicando filtros SQL (IQR + Percentiles)...")
    
    conditions = []
    for col, (low, high) in limits.items():
        conditions.append(f"({col} >= {low} AND {col} <= {high})")
        
    where_clause = " AND ".join(conditions)
    
    con.execute(f"""
        COPY (
            SELECT * FROM '{input_path}' WHERE {where_clause}
        ) TO '{intermediate_path}' (FORMAT 'PARQUET', CODEC 'SNAPPY')
    """)
    
    rows = con.execute(f"SELECT COUNT(*) FROM '{intermediate_path}'").fetchone()[0]
    print(f"\tPaso 1 completado. Filas restantes: {rows:,.0f}")

def step_2_isolation_forest(con, intermediate_path, final_path, columns):
    """Aplica ML sobre el archivo intermedio"""
    print("\n\t[PASO 2] Aplicando Isolation Forest (Machine Learning)...")
    
    # 1. Entrenar
    print(f"\t   Entrenando modelo con muestra de {ISO_SAMPLE_SIZE}...")
    df_sample = con.execute(f"""
        SELECT {', '.join(columns)} FROM '{intermediate_path}' 
        USING SAMPLE {ISO_SAMPLE_SIZE}
    """).df().fillna(0)
    
    clf = IsolationForest(
        n_estimators=100, 
        contamination=ISO_CONTAMINATION, 
        n_jobs=-1,
        random_state=42
    )
    clf.fit(df_sample)
    
    # 2. Predecir y guardar por chunks
    print("\t   Filtrando dataset completo...")
    
    # Borrar archivo final si existe para evitar errores de append
    if final_path.exists():
        final_path.unlink()
        
    # Usaremos una carpeta temporal para guardar las partes
    temp_dir = final_path.parent / "temp_parts"
    if temp_dir.exists(): shutil.rmtree(temp_dir)
    temp_dir.mkdir()
    
    chunk_size = 500_000
    offset = 0
    part_idx = 0
    
    total_rows_step1 = con.execute(f"SELECT COUNT(*) FROM '{intermediate_path}'").fetchone()[0]
    
    t_i = time.time()

    while True:
        # Leemos datos
        df_chunk = con.execute(f"""
            SELECT * FROM '{intermediate_path}' LIMIT {chunk_size} OFFSET {offset}
        """).df()
        
        if df_chunk.empty: break
        
        # Predecimos
        X = df_chunk[columns].fillna(0)
        preds = clf.predict(X) # 1 = Normal, -1 = Outlier
        
        # Filtramos (Solo nos quedamos con 1)
        df_clean = df_chunk[preds == 1]
        
        # Guardamos parte
        if not df_clean.empty:
            part_path = temp_dir / f"part_{part_idx}.parquet"
            df_clean.to_parquet(part_path, engine='pyarrow', index=False)
        
        offset += chunk_size
        part_idx += 1
        prog = min(100, (offset / total_rows_step1) * 100)
        print(f"\t   Progreso: {prog:.1f}% - Tiempo: {(time.time() - t_i) / 60:.1f}m", end="\r")
        
    print("\n\t   Consolidando archivo final...")
    # Unimos todas las partes con DuckDB en un solo archivo final
    con.execute(f"""
        COPY (SELECT * FROM '{temp_dir}/part_*.parquet') 
        TO '{final_path}' (FORMAT 'PARQUET', CODEC 'SNAPPY')
    """)
    
    # Limpieza
    shutil.rmtree(temp_dir)
    intermediate_path.unlink() 

def print_final_stats(con, original_path, final_path):
    orig = con.execute(f"SELECT COUNT(*) FROM '{original_path}'").fetchone()[0]
    final = con.execute(f"SELECT COUNT(*) FROM '{final_path}'").fetchone()[0]
    diff = orig - final
    perc = (final / orig) * 100
    
    print("\n" + "="*40)
    print(f"RESUMEN FINAL")
    print("="*40)
    print(f"Original:   {orig:,.0f}")
    print(f"Limpio:     {final:,.0f}")
    print(f"Eliminados: {diff:,.0f} outliers")
    print(f"Retención:  {perc:.2f}% de los datos")
    print("="*40)


def remove_outliers(outliers_cols):
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")

    PATH_DATA = Path.cwd() / "data"

    f_in = PATH_DATA / pl.DataFrame(lf.collect()).write_parquet("temp_input.parquet") or Path("temp_input.parquet")
    f_inter = PATH_DATA / f"{f_in.stem}_semi_clean.parquet"
    f_out = PATH_DATA / f"{f_in.stem}_final_clean.parquet"
    
    # 1. Calcular límites combinados (Percentil + IQR)
    limits = get_combined_limits(con, f_in, outliers_cols)
    
    # 2. Aplicar filtro estático (DuckDB) -> Genera archivo intermedio
    step_1_sql_filtering(con, f_in, f_inter, limits)
    
    # 3. Aplicar filtro ML (Isolation Forest) -> Genera archivo final
    step_2_isolation_forest(con, f_inter, f_out, outliers_cols)


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'")
    
    files = ["yellow_taxi_unified.parquet"]
    
    for f in files:
        f_in = PATH_DATA / f
        f_inter = PATH_DATA / f"{f.split('.')[0]}_semi_clean.parquet"
        f_out = PATH_DATA / f"{f.split('.')[0]}_final_clean.parquet"
        
        print(f"PROCESANDO: {f}")
        
        # 1. Calcular límites combinados (Percentil + IQR)
        limits = get_combined_limits(con, f_in, yellow_taxi_cols)
        
        # 2. Aplicar filtro estático (DuckDB) -> Genera archivo intermedio
        step_1_sql_filtering(con, f_in, f_inter, limits)
        
        # 3. Aplicar filtro ML (Isolation Forest) -> Genera archivo final
        step_2_isolation_forest(con, f_inter, f_out, yellow_taxi_cols)
        
        # 4. Resultados
        print_final_stats(con, f_in, f_out)