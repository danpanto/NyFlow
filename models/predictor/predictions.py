import pandas as pd
import numpy as np
from pathlib import Path
from .predictor import TaxiPredictor

# ==========================================
# 1. CONFIGURACIÓN E INICIALIZACIÓN
# ==========================================
print("🚀 Inicializando Motor de Inferencia...")
# Aseguramos que la ruta a los modelos sea correcta relativa a este script
script_dir = Path(__file__).resolve().parent
predictor = TaxiPredictor(base_path="final_models") 

print("\n📖 Leyendo datos históricos...")
# Asumiendo que tu carpeta data está en la raíz del proyecto
ruta_datos = script_dir.parent.parent / "data" / "final_data.parquet"

try:
    df_completo = pd.read_parquet(ruta_datos)
except Exception as e:
    print(f"❌ Error leyendo el parquet. Revisa que exista en: {ruta_datos}")
    exit(1)

df_completo = df_completo.sort_values(by=["Series_ID", "timestamp"])

# ==========================================
# 2. CALCULAR VENTANAS DE TIEMPO
# ==========================================
fecha_actual = df_completo['timestamp'].max()

# Generamos el grid de las 72 horas exactas que necesita TiDE
fechas_historia_perfecta = pd.date_range(end=fecha_actual, periods=72, freq='h')
fecha_inicio_historia = fechas_historia_perfecta.min()

print(f"   ⏱️ Hora actual del sistema: {fecha_actual}")
print(f"   ⏪ Extrayendo historia desde: {fecha_inicio_historia}")

df_historia = df_completo[df_completo['timestamp'] >= fecha_inicio_historia].copy()

# ==========================================
# 3. FABRICAR EL FUTURO (Covariables)
# ==========================================
print("   ⏩ Generando calendario para las próximas 24 horas...")
fechas_futuras = pd.date_range(start=fecha_actual + pd.Timedelta(hours=1), periods=24, freq='h')

df_futuro = pd.DataFrame({
    'timestamp': fechas_futuras,
    'hour': fechas_futuras.hour.astype("float32"),
    'dayofweek': fechas_futuras.dayofweek.astype("float32")
})

# ==========================================
# 4. BUCLE DE INFERENCIA MASIVA
# ==========================================
print("\n🔮 Iniciando predicciones por zona...")
resultados_prediccion = []
series_unicas = df_historia['Series_ID'].unique()

for idx, serie_id in enumerate(series_unicas):
    df_zona = df_historia[df_historia['Series_ID'] == serie_id].copy()
    
    # 🛠️ MAGIA MLOPS: Rellenar horas vacías con CEROS
    if len(df_zona) < 72:
        # Convertimos timestamp en índice para alinear con nuestro calendario perfecto
        df_zona = df_zona.set_index('timestamp').reindex(fechas_historia_perfecta)
        df_zona = df_zona.rename_axis('timestamp').reset_index()
        
        # Rellenamos los datos estáticos (ID, Vendor, Latitud...) copiando lo que sí tenemos
        cols_estaticas = ['Series_ID', 'PULocationID', 'VendorID', 'Latitude', 'Longitude']
        df_zona[cols_estaticas] = df_zona[cols_estaticas].ffill().bfill()
        
        # Rellenamos la demanda y covariables numéricas con 0 (¡No hubo taxis!)
        cols_num = ['demand', 'avg_distance', 'avg_amount']
        df_zona[cols_num] = df_zona[cols_num].fillna(0)
        
    try:
        # Predecimos la demanda
        pred_demanda = predictor.predecir_demanda(df_zona, df_futuro, horizonte=24)
        
        # Empaquetar
        df_resultado_zona = pd.DataFrame({
            'Series_ID': serie_id,
            'PULocationID': df_zona['PULocationID'].iloc[0],
            'VendorID': df_zona['VendorID'].iloc[0],
            'timestamp': fechas_futuras,
            'pred_demand': pred_demanda.values 
        })
        
        resultados_prediccion.append(df_resultado_zona)
        
        if (idx + 1) % 50 == 0:
            print(f"   ✅ Procesadas {idx + 1}/{len(series_unicas)} series...")
            
    except Exception as e:
        print(f"   ❌ Error al predecir la serie {serie_id}: {e}")

# ==========================================
# 5. EXPORTAR RESULTADOS (BLINDADO)
# ==========================================
if resultados_prediccion:
    df_predicciones_finales = pd.concat(resultados_prediccion, ignore_index=True)
    df_predicciones_finales['pred_demand'] = np.clip(np.round(df_predicciones_finales['pred_demand']), 0, None)
    
    # 🔥 SOLUCIÓN AL CRASH: Crear la carpeta si no existe y usar ruta absoluta
    ruta_salida = script_dir.parent.parent / "data" / "predicciones_24h.parquet"
    ruta_salida.parent.mkdir(parents=True, exist_ok=True) # <-- ESTO CREA LA CARPETA
    
    df_predicciones_finales.to_parquet(ruta_salida, index=False)
    
    print("\n" + "="*50)
    print(f"🎉 ¡ÉXITO! Se han predicho {len(df_predicciones_finales)} horas futuras.")
    print(f"💾 Resultados guardados en: {ruta_salida}")
    print("="*50)
else:
    print("\n⚠️ No se generó ninguna predicción.")