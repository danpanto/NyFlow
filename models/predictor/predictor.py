import os
import json
import joblib
import torch
import pandas as pd
import numpy as np
from pathlib import Path

from darts import TimeSeries
from darts.models import TiDEModel
from darts.dataprocessing.transformers import InvertibleMapper, Mapper
from darts.dataprocessing.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler
from darts.utils.timeseries_generation import datetime_attribute_timeseries

# Configuración de precisión requerida por el entrenamiento en 32-true
torch.set_float32_matmul_precision("high")

class TaxiPredictor:
    def __init__(self, base_path="final_models"):
        """
        Inicializa el predictor cargando todos los modelos y artefactos en memoria.
        """
        script_dir = Path(__file__).resolve().parent
        self.base_path = script_dir / base_path
        print(f"📂 Directorio base de modelos: {self.base_path}")
        
        self.darts_path = self.base_path / "darts_models"
        
        print("Cargando artefactos de preprocesamiento...")
        self.router = self._load_json(self.darts_path / "router_zonas.json")
        self.encoder_vendor = self._load_joblib(self.darts_path / "encoder_vendor.pkl")
        self.scaler_espacial = self._load_joblib(self.darts_path / "scaler_espacial.pkl")
        
        # Pipelines de transformación
        self.pipeline_target = Pipeline([InvertibleMapper(fn=np.log1p, inverse_fn=np.expm1)])
        self.pipeline_past = Pipeline([Mapper(fn=np.log1p)])

        print("Cargando modelos TiDE desde Checkpoints...")
        self.modelos_demanda = {
            "LOW": self._load_tide("demand/baja_demanda"),
            "MID": self._load_tide("demand/media_demanda"),
            "HIGH": self._load_tide("demand/alta_demanda")
        }
        
        self.modelo_precio = self._load_tide("amount/precio")
        self.modelo_distancia = self._load_tide("distance/distancia")
        
        print("✅ Todos los modelos cargados y listos.")

    def _load_json(self, path):
        with open(path, 'r') as f:
            return json.load(f)
            
    def _load_joblib(self, path):
        return joblib.load(path)
            
    def _load_tide(self, subpath):
        partes = subpath.split("/")
        carpeta_padre = partes[0]  
        nombre_modelo = partes[1]  
        
        work_dir_absoluto = str(self.base_path / carpeta_padre)
        
        try:
            return TiDEModel.load_from_checkpoint(
                model_name=nombre_modelo, 
                work_dir=work_dir_absoluto, 
                best=True,
                weights_only=False,
            )
        except Exception as e:
            print(f"⚠️ Error cargando modelo {nombre_modelo}: {e}")
            return None

    # ==========================================
    # PREPROCESAMIENTO Y TENSORES
    # ==========================================
    
    def _preprocesar_inputs(self, df_historia, target_col, past_cols):
        """
        Prepara Target y Past Covariates agrupando por zona/vendor.
        """
        df_clean = df_historia.copy()
        cols_numericas = df_clean.select_dtypes(include=['number']).columns
        df_clean[cols_numericas] = df_clean[cols_numericas].astype("float32")
        df_clean = df_clean.sort_values(["PULocationID", "VendorID", "timestamp"])
        
        cols_geo = ["Latitude", "Longitude"]
        if all(col in df_clean.columns for col in cols_geo):
            df_clean[cols_geo] = self.scaler_espacial.transform(df_clean[cols_geo])
            df_clean[cols_geo] = df_clean[cols_geo].astype("float32")
            
        nombres_dummies = []
        if "VendorID" in df_clean.columns:
            dummies_array = self.encoder_vendor.transform(df_clean[['VendorID']])
            nombres_dummies = list(self.encoder_vendor.get_feature_names_out(['VendorID']))
            df_clean[nombres_dummies] = dummies_array
            
        cols_estaticas = nombres_dummies + cols_geo
        
        ts_target = TimeSeries.from_group_dataframe(
            df_clean, group_cols=["PULocationID", "VendorID"],
            time_col="timestamp", value_cols=target_col, 
            static_cols=cols_estaticas, fill_missing_dates=True, freq="h"
        )
        
        ts_past = TimeSeries.from_group_dataframe(
            df_clean, group_cols=["PULocationID", "VendorID"],
            time_col="timestamp", value_cols=past_cols,
            fill_missing_dates=True, freq="h"
        )
        
        if not isinstance(ts_target, list): ts_target = [ts_target]
        if not isinstance(ts_past, list): ts_past = [ts_past]
            
        return ts_target, ts_past

    def _generar_covariables_futuras(self, df_historia, horizonte, num_series):
        """Genera el calendario y lo replica para cada serie temporal del lote."""
        inicio = df_historia['timestamp'].min()
        fin = df_historia['timestamp'].max() + pd.Timedelta(hours=horizonte)
        
        indice_tiempo = pd.date_range(start=inicio, end=fin, freq="h")
        cov_hora = datetime_attribute_timeseries(indice_tiempo, attribute="hour", cyclic=True)
        cov_dia = datetime_attribute_timeseries(indice_tiempo, attribute="dayofweek", cyclic=True)
        
        calendario_maestro = cov_hora.stack(cov_dia).astype("float32")
        return [calendario_maestro] * num_series

    def _formatear_predicciones(self, lista_ts_muestreadas, prefijo):
        """
        Extrae los cuantiles de las muestras de Monte Carlo y ensambla el DataFrame final.
        """
        if not isinstance(lista_ts_muestreadas, list):
            lista_ts_muestreadas = [lista_ts_muestreadas]
            
        dfs_salida = []
        for ts in lista_ts_muestreadas:
            # 1. Extraer los cuantiles sobre las 500 muestras generadas por Darts
            # (Lo pasamos directo a pandas DataFrame usando .iloc[:, 0] para aislar el array seguro)
            df_p10 = ts.quantile(0.10).to_dataframe()
            df_p50 = ts.quantile(0.50).to_dataframe()
            df_p90 = ts.quantile(0.90).to_dataframe()
            
            # 2. Recuperar identificadores
            puloc = int(ts.static_covariates["PULocationID"].iloc[0])
            vendor = int(ts.static_covariates["VendorID"].iloc[0])
            # 3. Ensamblar
            df_out = pd.DataFrame(index=df_p50.index)
            df_out["PULocationID"] = puloc
            df_out["VendorID"] = vendor
            df_out[f"pred_{prefijo}_p10"] = df_p10.iloc[:, 0].values
            df_out[f"pred_{prefijo}_p50"] = df_p50.iloc[:, 0].values
            df_out[f"pred_{prefijo}_p90"] = df_p90.iloc[:, 0].values
            
            dfs_salida.append(df_out)
            
        return pd.concat(dfs_salida).reset_index(names="timestamp")

    # ==========================================
    # FUNCIONES PÚBLICAS DE PREDICCIÓN
    # ==========================================

    def predecir_demanda(self, df_historia, horizonte=24):
        df_historia["segmento"] = df_historia["Series_ID"].astype(str).map(lambda x: self.router.get(x, "BASELINE"))
        resultados_totales = []
        
        for segmento, df_grupo in df_historia.groupby("segmento"):
            if segmento == "BASELINE":
                for (puloc, vendor), df_zona in df_grupo.groupby(["PULocationID", "VendorID"]):
                    df_zona = df_zona.sort_values("timestamp")
                    valores_ayer = df_zona['demand'].iloc[-24:].values
                    std_historia = df_zona['demand'].std()
                    margen_error = 1.28 * (std_historia if pd.notna(std_historia) else 0)
                    
                    tiling = int(np.ceil(horizonte / 24))
                    pred_central = np.tile(valores_ayer, tiling)[:horizonte]
                    indice_futuro = pd.date_range(start=df_zona["timestamp"].max() + pd.Timedelta(hours=1), periods=horizonte, freq="h")
                    
                    df_base = pd.DataFrame({
                        "timestamp": indice_futuro,
                        "PULocationID": puloc, "VendorID": vendor,
                        "pred_demand_p10": np.clip(pred_central - margen_error, 0, None),
                        "pred_demand_p50": pred_central,
                        "pred_demand_p90": pred_central + margen_error
                    })
                    resultados_totales.append(df_base)
                continue

            modelo = self.modelos_demanda.get(segmento)
            if not modelo: continue
            ts_target, ts_past = self._preprocesar_inputs(df_grupo, "demand", ["avg_distance", "avg_amount"])
            ts_future = self._generar_covariables_futuras(df_grupo, horizonte, len(ts_target))

            ts_target_tf = self.pipeline_target.transform(ts_target)
            ts_past_tf = self.pipeline_past.transform(ts_past)

            # ¡Ojo aquí! num_samples=500 para la inferencia de Monte Carlo
            pred_tf = modelo.predict(
                n=horizonte,
                series=ts_target_tf,
                past_covariates=ts_past_tf,
                future_covariates=ts_future,
                num_samples=500 
            )
            
            pred_real = self.pipeline_target.inverse_transform(pred_tf)
            df_formateado = self._formatear_predicciones(pred_real, "demand")
            resultados_totales.append(df_formateado)
            
        df_historia.drop(columns=["segmento"], inplace=True)
        return pd.concat(resultados_totales, ignore_index=True)

    def predecir_precio(self, df_historia, horizonte=24):
        ts_target, ts_past = self._preprocesar_inputs(df_historia, "avg_amount", ["demand", "avg_distance"])
        ts_future = self._generar_covariables_futuras(df_historia, horizonte, len(ts_target))

        ts_target_tf = self.pipeline_target.transform(ts_target)
        ts_past_tf = self.pipeline_past.transform(ts_past)

        pred_tf = self.modelo_precio.predict(
            n=horizonte, series=ts_target_tf, 
            past_covariates=ts_past_tf, future_covariates=ts_future, 
            num_samples=500
        )
        
        pred_real = self.pipeline_target.inverse_transform(pred_tf)
        return self._formatear_predicciones(pred_real, "amount")

    def predecir_distancia(self, df_historia, horizonte=24):
        ts_target, ts_past = self._preprocesar_inputs(df_historia, "avg_distance", ["demand", "avg_amount"])
        ts_future = self._generar_covariables_futuras(df_historia, horizonte, len(ts_target))

        ts_target_tf = self.pipeline_target.transform(ts_target)
        ts_past_tf = self.pipeline_past.transform(ts_past)

        pred_tf = self.modelo_distancia.predict(
            n=horizonte, series=ts_target_tf, 
            past_covariates=ts_past_tf, future_covariates=ts_future, 
            num_samples=500
        )
        
        pred_real = self.pipeline_target.inverse_transform(pred_tf)
        return self._formatear_predicciones(pred_real, "distance")