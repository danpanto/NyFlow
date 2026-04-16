import json
import joblib
import pandas as pd
import numpy as np
from pathlib import Path
from darts.models import TiDEModel
from darts import TimeSeries
from darts.dataprocessing.transformers import InvertibleMapper, Scaler
from darts.dataprocessing.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler

class TaxiPredictor:
    def __init__(self, base_path="final_models"):
        """
        Inicializa el predictor cargando todos los modelos y artefactos en memoria.
        Esto se hace una sola vez para que las predicciones sean rápidas.
        """
        # Convertimos la ruta base a un objeto Path relativo a este script
        script_dir = Path(__file__).resolve().parent
        self.base_path = script_dir / base_path
        print(f"📂 Directorio base de modelos: {self.base_path}")
        
        self.darts_path = self.base_path / "darts_models"
        
        print("Cargando artefactos de preprocesamiento...")
        self.router = self._load_json(self.darts_path / "router_zonas.json")
        self.encoder_vendor = self._load_joblib(self.darts_path / "encoder_vendor.pkl")
        self.scaler_espacial = self._load_joblib(self.darts_path / "scaler_espacial.pkl")
        
        # Pipelines estándar usados en el entrenamiento
        self.pipeline_log = Pipeline([InvertibleMapper(fn=np.log1p, inverse_fn=np.expm1)])
        self.pipeline_escala = Pipeline([Scaler(MinMaxScaler())])

        print("Cargando modelos TiDE desde Checkpoints...")
        # Modelos de Demanda (Especialistas)
        self.modelos_demanda = {
            "LOW": self._load_tide("demand/baja_demanda"),
            "MID": self._load_tide("demand/media_demanda"),
            "HIGH": self._load_tide("demand/alta_demanda")
        }
        
        # Modelos de Precio y Distancia
        self.modelo_precio = self._load_tide("amount/precio")
        self.modelo_distancia = self._load_tide("distance/distancia")
        
        print("✅ Todos los modelos cargados y listos.")

    # --- Métodos Auxiliares de Carga ---
    def _load_json(self, path):
        with open(path, 'r') as f:
            return json.load(f)
            
    def _load_joblib(self, path):
        return joblib.load(path)
            
    def _load_tide(self, subpath):
        """Carga un modelo TiDE desde un checkpoint de PyTorch Lightning."""
        # Ej: Si subpath es "demand/alta_demanda"
        partes = subpath.split("/")
        carpeta_padre = partes[0]  # "demand"
        nombre_modelo = partes[1]  # "alta_demanda"
        
        # El work_dir debe ser la ruta ABSOLUTA hasta la carpeta padre
        work_dir_absoluto = str(self.base_path / carpeta_padre)
        
        print(f"   -> Cargando: {nombre_modelo}")
        try:
            # Darts buscará automáticamente en: work_dir_absoluto / nombre_modelo / checkpoints / ...
            return TiDEModel.load_from_checkpoint(
                model_name=nombre_modelo, 
                work_dir=work_dir_absoluto, 
                best=True, # Carga el mejor modelo (best-model.ckpt) si existe
                weights_only=False
            )
        except Exception as e:
            print(f"⚠️ Error cargando modelo {nombre_modelo} en {work_dir_absoluto}: {e}")
            return None

    # --- Preprocesamiento Común ---
    def _preprocesar_input(self, df, value_col):
        """
        Aplica el mismo preprocesamiento que en entrenamiento: escalado espacial, 
        one-hot encoding y conversión a Darts TimeSeries.
        """
        df_clean = df.copy()
        
        # 1. Escalado Espacial
        cols_geo = ["Latitude", "Longitude"]
        if all(col in df_clean.columns for col in cols_geo):
            df_clean[cols_geo] = self.scaler_espacial.transform(df_clean[cols_geo])
            df_clean[cols_geo] = df_clean[cols_geo].astype("float32")
            
        # 2. One-Hot Encoding del VendorID
        if "VendorID" in df_clean.columns:
            dummies_array = self.encoder_vendor.transform(df_clean[['VendorID']])
            nombres_dummies = self.encoder_vendor.get_feature_names_out(['VendorID'])
            df_clean[nombres_dummies] = dummies_array
            
        # 3. Columnas Estáticas
        estaticas = list(self.encoder_vendor.get_feature_names_out(['VendorID'])) + cols_geo
        
        # 4. Convertir a TimeSeries de Darts
        ts_target = TimeSeries.from_dataframe(
            df_clean, time_col="timestamp", value_cols=value_col, static_cols=estaticas
        )
        
        return ts_target

    # ==========================================
    # FUNCIONES PÚBLICAS DE PREDICCIÓN
    # ==========================================

    def predecir_demanda(self, df_historia, df_covariables_futuras, horizonte=24):
        zona_id = str(int(df_historia['PULocationID'].iloc[0]))
        
        segmento = self.router.get(zona_id, "BASELINE")
        
        if segmento == "BASELINE":
            valores_ayer = df_historia['demand'].iloc[-24:].values
            tiling = int(np.ceil(horizonte / 24))
            pred_cruda = np.tile(valores_ayer, tiling)[:horizonte]
            return pd.Series(pred_cruda, name="pred_demand_baseline")
            
        modelo = self.modelos_demanda.get(segmento)
        if not modelo:
            raise ValueError(f"Modelo para segmento {segmento} no cargado.")

        ts_target = self._preprocesar_input(df_historia, "demand")
        ts_target_tf = self.pipeline_log.transform(ts_target)
        
        ts_past = TimeSeries.from_dataframe(df_historia, time_col="timestamp", value_cols=["avg_distance", "avg_amount"])
        ts_past_tf = self.pipeline_escala.transform(ts_past)
        
        ts_future = TimeSeries.from_dataframe(df_covariables_futuras, time_col="timestamp", value_cols=["hour", "dayofweek"])

        pred_tf = modelo.predict(
            n=horizonte,
            series=ts_target_tf,
            past_covariates=ts_past_tf,
            future_covariates=ts_future,
            num_samples=100 
        )
        
        pred_real = self.pipeline_log.inverse_transform(pred_tf)
        pred_p50 = pred_real.quantile(0.50).pd_series()
        pred_p50.name = f"pred_demand_{segmento}"
        
        return pred_p50

    def predecir_precio(self, df_historia, df_covariables_futuras, horizonte=24):
        if not self.modelo_precio:
            return None
            
        ts_target = self._preprocesar_input(df_historia, "avg_amount")
        ts_target_tf = self.pipeline_log.transform(ts_target)
        
        ts_future = TimeSeries.from_dataframe(df_covariables_futuras, time_col="timestamp", value_cols=["hour", "dayofweek"])

        pred_tf = self.modelo_precio.predict(n=horizonte, series=ts_target_tf, future_covariates=ts_future, num_samples=100)
        pred_real = self.pipeline_log.inverse_transform(pred_tf)
        return pred_real.quantile(0.50).pd_series(name="pred_amount")

    def predecir_distancia(self, df_historia, df_covariables_futuras, horizonte=24):
        if not self.modelo_distancia:
            return None
            
        ts_target = self._preprocesar_input(df_historia, "avg_distance")
        ts_target_tf = self.pipeline_log.transform(ts_target)
        
        ts_future = TimeSeries.from_dataframe(df_covariables_futuras, time_col="timestamp", value_cols=["hour", "dayofweek"])

        pred_tf = self.modelo_distancia.predict(n=horizonte, series=ts_target_tf, future_covariates=ts_future, num_samples=100)
        pred_real = self.pipeline_log.inverse_transform(pred_tf)
        return pred_real.quantile(0.50).pd_series(name="pred_distance")