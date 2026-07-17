import os
import duckdb
import pandas as pd
import numpy as np
from prefect import task, flow
try:
    from src.utils import haversine_distance
except ModuleNotFoundError:
    from utils import haversine_distance

# Rutas globales de la bodega
DB_DIR = r"D:\HAIC-LDP\data"
DB_PATH = os.path.join(DB_DIR, "logistics_warehouse.db")

@task(name="Ingest Bronze Layer")
def ingest_bronze(data_dir: str, db_path: str, is_initial: bool = True):
    """
    TODO: Capa Bronze (Raw Ingestion)
    
    Instrucciones:
    1. Asegurar la creación del directorio base de la base de datos `db_path`.
    2. Conectarse a DuckDB utilizando `duckdb.connect(db_path)`.
    3. Leer y cargar los CSVs originales de la carpeta `data_dir` en tablas DuckDB:
       Tablas: bronze_orders, bronze_customers, bronze_order_items, bronze_order_payments,
               bronze_order_reviews, bronze_products, bronze_sellers, bronze_geolocation,
               product_category_name_translation.
       
    4. Manejo de Ingesta:
       - Si `is_initial` es True (Fase 1): Eliminar tablas existentes (si las hay) y crearlas desde cero
         cargando los CSVs mediante `read_csv_auto`.
       - Si `is_initial` es False (Fase 2): Ingestar de forma incremental. Añadir solo las nuevas filas
         a las tablas existentes sin generar duplicados (usando cláusulas SQL como `NOT IN` con las llaves primarias).
    """
    # Conexión a DuckDB y lógica de ingesta incremental en SQL aquí:
    pass

@task(name="Build Silver Layer")
def build_silver(db_path: str):
    """
    TODO: Capa Silver (Cleansed & Conformed en Pandas)
    
    Instrucciones:
    1. Conectarse a DuckDB.
    2. Extraer las tablas crudas `bronze_orders`, `bronze_customers` y `bronze_geolocation` 
       en DataFrames de Pandas (puedes usar `.to_df()` desde una consulta DuckDB).
    3. Cruzar la tabla de órdenes con la de clientes mediante `customer_id` usando Pandas (`.merge()`).
    4. Estandarizar tipos de datos y formatos:
       - Parsear todas las columnas de fechas a tipo Datetime usando `pd.to_datetime()`.
       - Columnas de fecha: order_purchase_timestamp, order_approved_at, order_delivered_carrier_date,
         order_delivered_customer_date, order_estimated_delivery_date.
    5. Calcular columnas de duración logística en días usando operaciones de fecha de Pandas:
       - `actual_delivery_days`: días reales transcurridos entre compra y entrega al cliente.
       - `estimated_delivery_days`: días estimados de entrega prometidos desde la compra.
       - Pista: Resta los timestamps, extrae los segundos totales usando `.dt.total_seconds()` y divide entre 86400.0.
    6. Agregar geolocalizaciones agrupando el dataframe de geolocalización por `geolocation_zip_code_prefix` 
       calculando el promedio de latitud y longitud. Renombrar las columnas a `lat` y `lng`.
    7. Guardar los DataFrames resultantes de vuelta en DuckDB (reemplazando las tablas `silver_orders` y `silver_geolocation_agg`).
       - Pista: Puedes ejecutar un comando como `CREATE OR REPLACE TABLE silver_orders AS SELECT * FROM df_silver`.
    """
    # Transformaciones de limpieza y cruces usando Pandas aquí:
    pass

@task(name="Build Gold Layer")
def build_gold(db_path: str):
    """
    TODO: Capa Gold (Feature Store & ML Ready en Pandas)
    
    Instrucciones:
    1. Conectarse a DuckDB y cargar las tablas `silver_orders`, `bronze_order_items`, `bronze_products`,
       `bronze_sellers` y `silver_geolocation_agg` en DataFrames de Pandas.
    2. Estandarizar tipos de datos para evitar errores de tipo al cruzar:
       - Asegurar que todas las columnas de código postal (`customer_zip_code_prefix`, `seller_zip_code_prefix` y
         `geolocation_zip_code_prefix` de la tabla de coordenadas) se conviertan a tipo `int64` (utilizando
         `pd.to_numeric` con `errors='coerce'` y convirtiendo con `.astype('int64')`).
    3. Definir la variable objetivo (Target) `is_delayed`:
       - `1` si el pedido llegó tarde (`order_delivered_customer_date` > `order_estimated_delivery_date`),
         `0` si llegó a tiempo, y `NaN` si aún no ha sido entregado (pedidos activos/tránsito).
       - Pista: Puedes usar `np.where` para evaluar la condición de fecha sobre filas con entrega no nula.
    4. Mapear artículos con características y geolocalización:
       - Cruzar artículos de orden con productos para obtener el peso (`product_weight_g`).
       - Cruzar artículos con vendedores para obtener su código postal de origen.
       - Cruzar artículos con clientes para obtener su código postal de destino.
       - Cruzar ambos códigos postales con la tabla `silver_geolocation_agg` para obtener coordenadas `lat/lng` 
         del cliente (`cust_lat`, `cust_lng`) y del vendedor (`sell_lat`, `sell_lng`).
    5. Calcular distancias geográficas en Python:
       - Aplicar la función `haversine_distance` importada desde `src.utils` pasándole las columnas de coordenadas.
       - Rellenar valores nulos de distancia (si no se encontraron coordenadas) con un valor mediano de 300.0 km.
    6. Agrupar las características a nivel de pedido (`order_id`):
       - `total_weight_g`: suma de pesos de los productos.
       - `total_items`: cantidad de artículos incluidos en la orden.
       - `max_distance_km`: distancia máxima registrada entre el cliente y los vendedores de sus artículos.
    7. Unir las características agrupadas de vuelta al DataFrame de `silver_orders`.
    8. Extraer características temporales en Python:
       - `purchase_day_of_week` (día de la semana de la compra: 0-6).
       - `purchase_hour` (hora de la compra: 0-23).
    9. Registrar columna de auditoría `processed_at` con la marca de tiempo de este momento (`pd.Timestamp.now()`),
       vital para que el endpoint de inferencia `/predict/new-data` detecte datos procesados en los últimos 5 minutos.
    10. Guardar el DataFrame final de vuelta en la tabla `gold_orders` de DuckDB.
    """
    # Lógica de cálculo de features a nivel de orden y guardado en gold_orders usando Pandas aquí:
    pass

@flow(name="logistics_etl_pipeline")
def logistics_etl_pipeline(data_dir: str, db_path: str = DB_PATH, is_initial: bool = True):
    """
    TODO: Orquestación del Flujo ETL
    
    Instrucciones:
    1. Ejecutar en secuencia ordenada las tareas:
       a) Ingest Bronze Layer
       b) Build Silver Layer
       c) Build Gold Layer
    2. Imprimir logs útiles sobre el estado y avance de cada etapa del pipeline.
    """
    # Orquestación de las tareas Prefect aquí:
    pass

if __name__ == "__main__":
    # Comando por defecto para inicializar la Fase 1
    logistics_etl_pipeline(data_dir=r"D:\HAIC-LDP\raw_data\fase_1", is_initial=True)
