import os
import duckdb
from prefect import task, flow
from datetime import datetime

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
    3. Leer y cargar los CSVs originales de la carpeta `data_dir`. Las tablas necesarias son:
       - bronze_orders
       - bronze_customers
       - bronze_order_items
       - bronze_order_payments
       - bronze_order_reviews
       - bronze_products
       - bronze_sellers
       - bronze_geolocation
       - product_category_name_translation
       
    4. Manejo de Ingesta:
       - Si `is_initial` es True (Fase 1): Eliminar tablas existentes (si las hay) y crearlas desde cero
         cargando los CSVs mediante `read_csv_auto`.
       - Si `is_initial` es False (Fase 2): Ingestar de forma incremental. Añadir solo las nuevas filas
         a las tablas existentes sin generar duplicados (usando cláusulas SQL como `NOT IN` con las llaves primarias).
    """
    # Conexión a DuckDB y lógica de ingesta incremental aquí:
    pass

@task(name="Build Silver Layer")
def build_silver(db_path: str):
    """
    TODO: Capa Silver (Cleansed & Conformed)
    
    Instrucciones:
    1. Conectarse a DuckDB.
    2. Crear o reemplazar la tabla `silver_orders` cruzando `bronze_orders` con `bronze_customers` (usando `customer_id`).
    3. Estandarizar tipos de datos y formatos:
       - Parsear todas las columnas de fechas a tipo `TIMESTAMP` (usando TRY_CAST en SQL).
       - Campos clave de fecha: order_purchase_timestamp, order_approved_at, order_delivered_carrier_date,
         order_delivered_customer_date, order_estimated_delivery_date.
    4. Calcular columnas de duración logística inicial en días:
       - actual_delivery_days: días reales transcurridos entre compra y entrega al cliente.
       - estimated_delivery_days: días estimados de entrega prometidos desde la compra.
       - Pista: En DuckDB puedes restar marcas de tiempo y usar la función `epoch` para convertirlas a segundos,
         dividiendo luego entre 86400.0 para obtener días flotantes.
    5. Opcional: Agregar una tabla consolidada o agregada de geolocalizaciones (`silver_geolocation_agg`) agrupando
       `bronze_geolocation` por `geolocation_zip_code_prefix` con el promedio de lat/lng para tener coordenadas únicas.
    """
    # Transformaciones SQL de limpieza y cruces aquí:
    pass

@task(name="Build Gold Layer")
def build_gold(db_path: str):
    """
    TODO: Capa Gold (Feature Store & ML Ready)
    
    Instrucciones:
    1. Conectarse a DuckDB.
    2. Reconstruir la tabla final `gold_orders` agrupada a nivel de pedido (`order_id`) que unifique
       la orden procesada en la capa Silver con los artículos, pesos y distancias.
    3. Campos a calcular/generar en la consulta SQL final:
       - `order_id` y metadatos básicos de la orden.
       - Target (`is_delayed`): `1` si el pedido llegó tarde (`order_delivered_customer_date` > `order_estimated_delivery_date`),
         `0` si llegó a tiempo, y `NULL` si aún no ha sido entregado (pedidos activos/tránsito).
       - Feature `total_weight_g`: Peso total consolidado sumando los pesos de los productos (`product_weight_g`).
       - Feature `total_items`: Cantidad de artículos incluidos en el pedido.
       - Feature `purchase_day_of_week`: Día de la semana en que se realizó la compra (0 a 6 o 1 a 7).
       - Feature `purchase_hour`: Hora del día de la compra (0 a 23).
       - Feature `estimated_delivery_days`: Promesa de entrega en días flotantes.
       - Feature `max_distance_km`: Distancia máxima entre el cliente y el vendedor del artículo.
         Pista: Puedes calcular la distancia geodésica directamente en DuckDB SQL uniendo las coordenadas 
         promedio de cliente y vendedor a partir del prefijo de código postal, y aplicando la fórmula de Haversine:
         
         distancia = 2 * 6371 * ASIN(SQRT(
             POWER(SIN(radians(s_lat - c_lat) / 2), 2) +
             COS(radians(c_lat)) * COS(radians(s_lat)) * POWER(SIN(radians(s_lng - c_lng) / 2), 2)
         ))
         
         Asegúrate de manejar nulos usando COALESCE con una distancia mediana razonable (ej. 300 km) si no se encuentran coordenadas.
         
       - Campo `processed_at`: Registrar el timestamp actual del procesamiento (`CURRENT_TIMESTAMP`).
         Este campo es indispensable para que la API de inferencia en vivo pueda identificar qué registros 
         se procesaron en el "live drop" de los últimos 5 minutos.
    """
    # Lógica de cálculo de features a nivel de orden y guardado en gold_orders aquí:
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
