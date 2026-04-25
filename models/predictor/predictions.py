import pandas as pd
import numpy as np
from pathlib import Path
from .predictor import TaxiPredictor

# ==========================================
# 1. CONFIGURACIÓN E INICIALIZACIÓN
# ==========================================
print("🚀 Inicializando Motor de Inferencia...")
script_dir = Path(__file__).resolve().parent
predictor = TaxiPredictor(base_path="final_models") 

print("\n📖 Leyendo datos históricos...")
ruta_datos = script_dir.parent / "data" / "final_data.parquet"

try:
    df_completo = pd.read_parquet(ruta_datos)
except Exception as e:
    print(f"❌ Error leyendo el parquet. Revisa que exista en: {ruta_datos}")
    exit(1)
    
cols_numericas = df_completo.select_dtypes(include=['float64', 'int64']).columns
df_completo[cols_numericas] = df_completo[cols_numericas].astype("float32")

df_completo = df_completo.sort_values(by=["Series_ID", "timestamp"])

# ==========================================
# 2. CALCULAR VENTANAS DE TIEMPO
# ==========================================
fecha_actual = df_completo['timestamp'].max()
fechas_historia_perfecta = pd.date_range(end=fecha_actual, periods=72, freq='h')
fecha_inicio_historia = fechas_historia_perfecta.min()

print(f"   ⏱️ Hora actual del sistema: {fecha_actual}")
print(f"   ⏪ Extrayendo historia desde: {fecha_inicio_historia}")

df_historia = df_completo[df_completo['timestamp'] >= fecha_inicio_historia].copy()

# ==========================================
# 3. RELLENAR HORAS VACÍAS (PREPARACIÓN BATCH)
# ==========================================
print("\n🧹 Rellenando huecos en las series temporales...")
series_unicas = df_historia['Series_ID'].unique()
lista_df_limpios = []

# Mantenemos tu bucle SOLO para limpiar y rellenar ceros en zonas incompletas
for serie_id in series_unicas:
    df_zona = df_historia[df_historia['Series_ID'] == serie_id].copy()
    
    if len(df_zona) < 72:
        df_zona = df_zona.set_index('timestamp').reindex(fechas_historia_perfecta)
        df_zona = df_zona.rename_axis('timestamp').reset_index()
        cols_estaticas = ['Series_ID', 'PULocationID', 'VendorID', 'Latitude', 'Longitude']
        df_zona[cols_estaticas] = df_zona[cols_estaticas].ffill().bfill()
        cols_num = ['demand', 'avg_distance', 'avg_amount']
        df_zona[cols_num] = df_zona[cols_num].fillna(0)
        
    lista_df_limpios.append(df_zona)

# Unimos todo en un mega DataFrame perfecto para Darts
df_historia_batch = pd.concat(lista_df_limpios, ignore_index=True)

# ==========================================
# 4. INFERENCIA MASIVA EN LOTE (¡Sin bucle!)
# ==========================================
print("\n🔮 Iniciando predicciones en lote (Batch Inferencing)...")

try:
    # Le pasamos el DataFrame completo. ¡El predictor se encarga de separar y enrutar!
    print("   -> Prediciendo Demanda...")
    df_pred_demanda = predictor.predecir_demanda(df_historia_batch, horizonte=24)
    
    print("   -> Prediciendo Precio...")
    df_pred_precio = predictor.predecir_precio(df_historia_batch, horizonte=24)
    
    print("   -> Prediciendo Distancia...")
    df_pred_distancia = predictor.predecir_distancia(df_historia_batch, horizonte=24)
    
    # Unificamos los 3 DataFrames resultantes usando las claves lógicas
    claves_cruce = ['PULocationID', 'VendorID', 'timestamp']
    df_predicciones_finales = df_pred_demanda.merge(df_pred_precio, on=claves_cruce, how='outer')
    df_predicciones_finales = df_predicciones_finales.merge(df_pred_distancia, on=claves_cruce, how='outer')
    
    # Añadir el Series_ID de vuelta (opcional, concatenando strings)
    df_predicciones_finales['Series_ID'] = df_predicciones_finales['PULocationID'].astype(int).astype(str) + "_" + df_predicciones_finales['VendorID'].astype(int).astype(str)

except Exception as e:
    print(f"❌ Error crítico durante la inferencia en lote: {e}")
    exit(1)

# ==========================================
# 5. EXPORTAR RESULTADOS
# ==========================================
if not df_predicciones_finales.empty:
    
    # 1. La demanda no puede ser negativa y debe ser entera
    cols_demanda = ['pred_demand_p10', 'pred_demand_p50', 'pred_demand_p90']
    for col in cols_demanda:
        df_predicciones_finales[col] = np.clip(np.round(df_predicciones_finales[col]), 0, None)
        
    # 2. Precio y distancia no pueden ser negativos, con 2 decimales
    cols_dinero_distancia = [
        'pred_amount_p10', 'pred_amount_p50', 'pred_amount_p90',
        'pred_distance_p10', 'pred_distance_p50', 'pred_distance_p90'
    ]
    for col in cols_dinero_distancia:
        df_predicciones_finales[col] = np.clip(np.round(df_predicciones_finales[col], 2), 0, None)
    
    # Ordenar columnas para que quede bonito
    cols_ordenadas = ['Series_ID', 'PULocationID', 'VendorID', 'timestamp'] + cols_demanda + [c for c in cols_dinero_distancia if "amount" in c] + [c for c in cols_dinero_distancia if "distance" in c]
    df_predicciones_finales = df_predicciones_finales[cols_ordenadas]
    
    ruta_salida = script_dir.parent.parent / "data" / "predicciones_24h.parquet"
    ruta_salida.parent.mkdir(parents=True, exist_ok=True)
    
    df_predicciones_finales.to_parquet(ruta_salida, index=False)
    
    print("\n" + "="*50)
    print(f"🎉 ¡ÉXITO! Se han predicho {len(df_predicciones_finales)} horas futuras en lote.")
    print(f"💾 Resultados guardados en: {ruta_salida}")
    print("="*50)
else:
    print("\n⚠️ No se generó ninguna predicción.")