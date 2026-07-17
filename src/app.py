import os
import pickle
import duckdb
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from contextlib import asynccontextmanager

# Rutas de base de datos y modelo
DB_PATH = r"D:\HAIC-LDP\data\logistics_warehouse.db"
MODEL_PATH = r"D:\HAIC-LDP\models\delivery_model.pkl"

# Variables globales para almacenar el modelo y metadatos en memoria
model = None
features = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    TODO: Lifespan Event Manager
    
    Instrucciones:
    1. En el arranque de la API (startup), verificar si el archivo del modelo `MODEL_PATH` existe.
    2. Cargar el archivo serializado `.pkl` usando `pickle.load()`.
    3. Poblar las variables globales `model` y `features` con el modelo y el orden de columnas correspondientes.
    4. Ceder el control a la aplicación (yield) para procesar solicitudes.
    """
    global model, features
    # Carga tu modelo aquí:
    yield

# Inicializar FastAPI
app = FastAPI(
    title="HAIC Logistics: Late Delivery Prediction API",
    description="Plantilla de API para predecir entregas tardías en base al dataset de logística de Olist.",
    version="1.0.0",
    lifespan=lifespan
)

# Esquemas de Datos con Pydantic
class BatchRequest(BaseModel):
    order_ids: Optional[List[str]] = None

class OrderPrediction(BaseModel):
    order_id: str
    probability_late: float
    prediction_late: int
    features: dict

class PredictionResponse(BaseModel):
    total_predicted: int
    late_count: int
    predictions: List[OrderPrediction]

@app.get("/")
def read_root():
    """
    TODO: Endpoint de estado del servidor
    Retornar un JSON simple indicando el estado del servidor, si el modelo está cargado y la ruta a los docs.
    """
    pass

@app.post("/predict/batch", response_model=PredictionResponse)
def predict_batch(request: Optional[BatchRequest] = None, limit: Optional[int] = 100):
    """
    TODO: Endpoint de Predicción Batch
    
    Instrucciones:
    1. Validar que el modelo esté cargado en memoria (`model is not None`).
    2. Conectarse a la base de datos DuckDB.
    3. Escribir y ejecutar una consulta SQL en `gold_orders`:
       - Si el cuerpo de la petición contiene `order_ids` (Lista no vacía), filtrar las órdenes por ese grupo de IDs.
       - Si no se provee cuerpo o la lista está vacía, hacer una inferencia sobre las órdenes activas en el pipeline
         (es decir, filtrar la tabla `gold_orders` donde el target `is_delayed IS NULL`).
    4. Cargar los datos filtrados en un DataFrame de Pandas.
    5. Utilizar las variables cargadas para la predicción de clases (`predict`) y cálculo de probabilidades (`predict_proba`).
    6. Calcular métricas resumen para la respuesta:
       - `total_predicted`: El conteo total de órdenes analizadas.
       - `late_count`: Cuántas de esas órdenes se predijo que llegarán tarde (`prediction = 1`).
    7. Retornar los detalles individuales en la lista `predictions`:
       - Para evitar que la interfaz de Swagger `/docs` se congele si la lista analizada tiene miles de registros,
         aplica el parámetro `limit` para recortar los objetos devueltos en la lista detailed (ej. `head(limit)`),
         pero calcula los resúmenes `total_predicted` y `late_count` sobre el total real calculado.
    """
    # Conexión, consulta filtrada, inferencia y mapeo a respuesta Pydantic aquí:
    pass

@app.post("/predict/new-data", response_model=PredictionResponse)
def predict_new_data(limit: Optional[int] = 100):
    """
    TODO: Endpoint de Predicción de Nuevos Datos (Live Drop)
    
    Instrucciones:
    1. Validar la carga del modelo.
    2. Conectarse a DuckDB.
    3. Formular una consulta SQL para extraer las órdenes agregadas en la tabla `gold_orders` procesadas
       recientemente.
       - Pista de negocio: Consulta registros donde la columna de auditoría de ingesta `processed_at` 
         sea mayor o igual al momento actual menos 5 minutos (`processed_at >= NOW() - INTERVAL '5 minutes'`).
         Esta lógica es la que simulará la carga incremental en tiempo real en la demostración en vivo.
    4. Realizar la predicción de retraso (`predict`) y cálculo de probabilidad (`predict_proba`).
    5. Calcular los valores totales (`total_predicted`, `late_count`).
    6. Mapear y retornar la lista de predicciones detalladas, recortando la cantidad de registros devueltos
       en el cuerpo JSON a la cantidad máxima definida en `limit` para optimizar la transferencia de red y
       el renderizado de Swagger.
    """
    # Consulta temporal de auditoría, inferencia y respuesta aquí:
    pass
