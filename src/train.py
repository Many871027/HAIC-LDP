import os
import duckdb
import pickle
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score

# Rutas de archivos del modelo
DB_PATH = r"D:\HAIC-LDP\data\logistics_warehouse.db"
MODEL_DIR = r"D:\HAIC-LDP\models"
MODEL_PATH = os.path.join(MODEL_DIR, "delivery_model.pkl")

def train_model():
    """
    TODO: Entrenamiento y Serialización del Modelo
    
    Instrucciones:
    1. Conectarse a DuckDB y extraer los datos de la tabla `gold_orders`.
    2. Cargar las features requeridas:
       - total_weight_g
       - max_distance_km
       - total_items
       - purchase_day_of_week
       - purchase_hour
       - estimated_delivery_days
       - is_delayed (como target)
    3. Filtrar los datos:
       - Entrenar exclusivamente en registros donde `is_delayed` y `estimated_delivery_days` no sean nulos 
         (es decir, pedidos que ya completaron su ciclo logístico).
    4. Separar los datos en conjunto de Entrenamiento (Train) y Prueba (Test) con `train_test_split`.
       - Sugerencia: Usar `stratify=y` dado el desbalanceo del target.
    5. Entrenar el clasificador RandomForestClassifier:
       - Nota de negocio crítica: Menos del 10% de los pedidos llegan tarde. Si entrenas el modelo sin balancear,
         el modelo tenderá a clasificar todo como "a tiempo" (0) dando métricas vacías para entregas tarde.
       - Solución: Configurar el clasificador con `class_weight="balanced"` para balancear los pesos de las clases
         y lograr capturar retrasos reales en los pedidos nuevos.
    6. Evaluar la calidad predictiva del modelo:
       - Calcular predicciones e imprimir el reporte de clasificación (`classification_report`).
       - Evaluar la probabilidad de retraso mediante el cálculo de la curva ROC-AUC (`roc_auc_score`).
    7. Guardar el modelo:
       - Crear el directorio `models` si no existe.
       - Empaquetar y serializar en un diccionario o tupla el modelo clasificador entrenado junto con la lista 
         de nombres de columnas de features en un archivo pickle (`delivery_model.pkl`).
    """
    # Conexión, carga de datos, entrenamiento, métricas y exportación del modelo en .pkl aquí:
    pass

if __name__ == "__main__":
    train_model()
