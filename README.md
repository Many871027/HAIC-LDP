# HAIC Logistics: Late Delivery Prediction (LDP) 

![Logo de Hidalgo AI Community](HAIC%20LOGO.png)

¡Hola, comunidad de **Hidalgo AI Community (HAIC)**! 

Bienvenidos al repositorio base de la práctica **Late Delivery Prediction (LDP)**. En el mundo real, el e-commerce y la logística son dos de los sectores que mayor volumen de datos generan y donde la predicción de tiempos de entrega tiene un impacto directo en la satisfacción del cliente y en los costos operativos. 

Este proyecto está diseñado para simular un entorno de producción utilizando una arquitectura de datos **Medallion** sobre **DuckDB**, orquestada con **Prefect**, entrenamiento de modelos con **Scikit-learn** y servicio de inferencia en tiempo real a través de **FastAPI**.

En este template, ayuda a los miembros de **HAIC** a comprender e interactuar de forma manual con cada una de las partes y scripts que componen el ciclo de vida de los datos del proyecto.

---

##  El Desafío

El objetivo es construir un sistema capaz de predecir si un pedido llegará tarde (`order_delivered_customer_date` > `order_estimated_delivery_date`).

Para simular un entorno productivo real, hemos dividido los datos cronológicamente:
* **Fase 1 (70% más antiguo)**: Representa los datos históricos y está disponible en `raw_data/fase_1`. Se usará para entrenar el modelo.
* **Fase 2 (30% más reciente)**: Representa los "datos de hoy martes" y se simula su llegada en la sesión en vivo. **Nota:** Esta carpeta está ignorada en Git (`.gitignore`) para que la descarguen en la sesión del "Show & Tell".

---

##  Estructura de Carpetas y Requisitos de Código

Para completar esta práctica, deberás desarrollar la lógica de código en los siguientes scripts que se encuentran en estado de plantilla:

```text
├── raw_data/                # Datos CSV crudos completos
│   ├── fase_1/              # Fase 1: 70% de datos (incluido en el repositorio)
│   └── fase_2/              # Fase 2: 30% de datos (ignorado en git, live drop)
├── src/
│   ├── utils.py             # Funciones de cálculo geográfico
│   ├── pipeline.py          # Flujo de orquestación de datos Prefect + DuckDB (Medallion)
│   ├── train.py             # Script de entrenamiento del modelo de ML
│   └── app.py               # API de inferencia FastAPI
├── pyproject.toml           # Dependencias de Python gestionadas con uv
└── README.md                # Guía y manual de uso
```

A continuación, se detallan las actividades y requisitos de código que debes completar en cada script:

### 1.  `src/utils.py`
En este script debes implementar el cálculo geométrico de la distancia.
* **Función**: `haversine_distance(lat1, lon1, lat2, lon2)`
* **Objetivo**: Recibir las latitudes y longitudes del comprador y vendedor en grados decimales y devolver la distancia lineal en kilómetros (km).
* **Fórmula**: Implementar la ecuación matemática de Haversine utilizando `numpy` para vectorización:
  $$dlat = lat_2 - lat_1$$
  $$dlon = lon_2 - lon_1$$
  $$a = \sin^2(dlat/2) + \cos(lat_1) \cdot \cos(lat_2) \cdot \sin^2(dlon/2)$$
  $$c = 2 \cdot \arcsin(\sqrt{a})$$
  $$Distancia = c \cdot 6371\text{ km}$$
* **Nota**: Recuerda transformar los grados a radianes con `np.radians()` antes de aplicar funciones trigonométricas.

### 2.  `src/pipeline.py`
Este script contendrá las tres capas de la arquitectura Medallion utilizando tareas (`@task`) y flujos (`@flow`) de **Prefect** conectando **DuckDB** para persistencia y **Pandas** para las transformaciones:
* **Tarea `ingest_bronze`**:
  - Asegurar la creación del directorio para la base de datos `data/logistics_warehouse.db`.
  - Ingestar los archivos CSV de la ruta indicada (`data_dir`).
  - **Manejo de Ingesta**: Si `is_initial=True` (Fase 1), recrear todas las tablas cargándolas directamente. Si `is_initial=False` (Fase 2), realizar una **ingesta incremental** en SQL insertando solo las órdenes y detalles cuyos IDs no existan previamente en la base de datos (evitando duplicados).
* **Tarea `build_silver`**:
  - Cargar las tablas crudas `bronze_orders`, `bronze_customers` y `bronze_geolocation` desde DuckDB hacia DataFrames de Pandas (usando `.to_df()`).
  - Unir la tabla de órdenes con la de clientes mediante `customer_id` usando Pandas (`.merge()`).
  - Convertir todos los campos de fecha de texto a tipo datetime usando `pd.to_datetime()`.
  - Calcular la duración real y estimada de entrega en días flotantes (restando timestamps, extrayendo segundos totales con `.dt.total_seconds()` y dividiendo entre 86400.0).
  - Agrupar la geolocalización por `geolocation_zip_code_prefix` con el promedio de latitud/longitud.
  - Guardar los DataFrames procesados de vuelta en DuckDB (reemplazando las tablas `silver_orders` y `silver_geolocation_agg`).
* **Tarea `build_gold`**:
  - Cargar las tablas procesadas de la capa Silver y Bronze en DataFrames de Pandas.
  - Convertir las columnas de códigos postales (`customer_zip_code_prefix`, `seller_zip_code_prefix` y `geolocation_zip_code_prefix`) a tipo `int64` para evitar conflictos de tipo de datos al fusionar.
  - Definir la variable objetivo: `is_delayed` (1 si el pedido llegó tarde, 0 si llegó a tiempo, y NaN si está en tránsito) usando `np.where`.
  - Cruzar las coordenadas de clientes y vendedores correspondientes.
  - Calcular la distancia de envío en kilómetros llamando a la función **`haversine_distance` importada desde `src.utils`**. Rellenar nulos con una mediana de 300.0 km.
  - Agrupar características a nivel de pedido (`order_id`): suma de pesos, conteo de ítems y distancia máxima de envío.
  - Extraer variables temporales: día de la semana (0-6) y hora de la compra.
  - **Crítico para el Live Demo**: Registrar la columna `processed_at` con el timestamp de este momento (`pd.Timestamp.now()`) para auditar cuándo se procesaron los datos.
  - Guardar el DataFrame final de vuelta en la tabla `gold_orders` de DuckDB.
* **Flujo `logistics_etl_pipeline`**:
  - Orquestar la ejecución secuencial de las tres tareas anteriores.


### 3.  `src/train.py`
En este script implementarás el ciclo de entrenamiento y serialización del modelo:
* **Conexión**: Cargar los datos desde la tabla `gold_orders` en DuckDB hacia un DataFrame de Pandas.
* **Filtros**: Entrenar únicamente con pedidos entregados (donde `is_delayed` no sea nulo).
* **Partición**: Hacer un split de entrenamiento/prueba (Train/Test) estratificado.
* **Algoritmo**: Instanciar y entrenar un `RandomForestClassifier`.
* **Desbalance de Clases**: Debido a que solo ~9% de los pedidos llegan tarde, es obligatorio inicializar el modelo con **`class_weight="balanced"`** para asegurar que el modelo aprenda a predecir retrasos en lugar de sesgarse a decir que todo llegará a tiempo.
* **Evaluación**: Imprimir métricas de precisión, recall, F1-score y la puntuación de ROC-AUC.
* **Serialización**: Guardar en `models/delivery_model.joblib` un diccionario con el clasificador entrenado y la lista de nombres de features empleadas.

### 4.  `src/app.py`
En este script desarrollarás la API de inferencia en tiempo real usando **FastAPI**:
* **Lifespan Startup**: Cargar el modelo guardado en `models/delivery_model.joblib` en las variables globales al iniciar la aplicación.
* **Ruta `GET /`**: Retornar el estado del servicio.
* **Ruta `POST /predict/batch`**:
  - Aceptar un listado opcional de `order_ids`.
  - Si se proveen IDs, consultar la capa Gold y retornar sus predicciones.
  - Si la lista está vacía, predecir sobre todas las órdenes activas en tránsito (donde `is_delayed IS NULL`).
  - **Paginación/Slicing**: Implementar un parámetro de query `limit` (por defecto 100). Aunque el conteo total y de retrasos se evalúe sobre todo el DataFrame, la lista detallada de predicciones JSON en la respuesta debe limitarse a las primeras `limit` filas para evitar congelar el navegador en Swagger UI.
* **Ruta `POST /predict/new-data`**:
  - Realizar una consulta de DuckDB para filtrar y predecir sobre las órdenes procesadas por el ETL en los **últimos 5 minutos** (`processed_at >= NOW() - INTERVAL '5 minutes'`).
  - Implementar la misma lógica de recorte con `limit` en los resultados de respuesta detallados.

---

##  Requisitos de Configuración e Instalación

Para preparar tu entorno local, te recomendamos utilizar `uv` por su gran velocidad:

1. **Instalar dependencias y preparar el entorno virtual**:
   ```bash
   uv sync
   ```

2. **Ejecutar la ingesta de la Fase 1 (70% de datos)**:
   Puedes ejecutar directamente el script del pipeline (el cual por defecto corre la Fase 1 de forma inicial en su bloque `__main__`):
   ```bash
   uv run python src/pipeline.py
   ```

3. **Entrenar el Modelo**:
   Ejecuta directamente el script de entrenamiento para generar el archivo `.pkl`:
   ```bash
   uv run python src/train.py
   ```

4. **Levantar la API de FastAPI**:
   Usa Uvicorn desde la terminal para arrancar el servidor:
   ```bash
   uv run uvicorn src.app:app --host 127.0.0.1 --port 8000 --reload
   ```

5. **Simular Ingesta Incremental (Fase 2 - Live Drop)**:
   Dado que no hay CLI central, puedes importar el flujo e invocarlo directamente desde la terminal interactiva de Python o ejecutando una instrucción corta en línea:
   ```bash
   uv run python -c "from src.pipeline import logistics_etl_pipeline; logistics_etl_pipeline(data_dir='raw_data/fase_2', is_initial=False)"
   ```


---

*Desarrollado para la Hidalgo AI Community (HAIC).* 

