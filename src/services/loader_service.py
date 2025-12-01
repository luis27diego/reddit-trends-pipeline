# src/services/loader_service.py

import pandas as pd
from sqlalchemy import create_engine
import os
from prefect import get_run_logger

from src.services.io_service import leer_csv_optimizado
from src.services.spark_service import create_spark_session

# --- 1. Configuración de Conexión ---

# Conexión a PostgreSQL (usando variables de entorno del docker-compose)
DB_USER = os.getenv("POSTGRES_USER", "prefect")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "prefect")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres") # Nombre del servicio en docker-compose
DB_PORT = "5432"
DB_NAME = "reddit_analytics"
SPARK_NESTED_FOLDER = "the-reddit-climate-change-dataset-comments"
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)


def leer_resultado_spark_a_pandas(rutas_spark: dict, categoria: str, subcarpeta: str) -> pd.DataFrame:
    """
    Crea una sesión Spark (local), lee los CSV agregados desde MinIO/S3A, 
    y retorna el resultado como un Pandas DataFrame.
    """
    spark = create_spark_session()
    
    # Construir la ruta final de salida de Spark (S3A)
    base_path_s3a = rutas_spark[categoria]
    
    # La ruta completa que Spark necesita para leer los archivos
    full_path_s3a = f"{base_path_s3a}{subcarpeta}/{SPARK_NESTED_FOLDER}"
    
    print(f"Spark leyendo de: {full_path_s3a}")
    
    # Spark lee directamente la carpeta particionada
    df_spark = leer_csv_optimizado(spark, full_path_s3a)
    print(f"   → Datos leídos con Spark. Número de filas: {df_spark.count()}")
    
    # Conversión eficiente a Pandas (asumiendo que el resultado es pequeño después de la agregación)
    df_pandas = df_spark.toPandas()
    
    spark.stop() # Detener la sesión de Spark
    return df_pandas
    
# --- 2. Funciones de Carga ---

def limpiar_y_cargar(df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
    """Limpia columnas de Spark y carga a la tabla PostgreSQL."""
    logger = get_run_logger()
    
    # 1. Limpieza: Eliminar columnas de índice que Spark o Pandas pueden haber dejado.
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    
    # 2. Conversión de tipos (necesario para asegurar que las fechas de Spark sean leídas correctamente)
    if 'periodo' in df.columns:
        df['periodo'] = pd.to_datetime(df['periodo'])
    if 'created_utc' in df.columns:
        df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s', errors='coerce')
    
    logger.info(f"   -> Insertando {len(df)} filas en la tabla '{table_name}' con método '{if_exists}'...")

    # 3. Escritura a SQL
    df.to_sql(table_name, engine, if_exists=if_exists, index=False, method='multi')
    logger.info(f"   [OK] Tabla {table_name} actualizada exitosamente.")


def cargar_resultados_a_db(rutas_spark: dict):

    rutas_spark = {
    'temporal': 's3a://tendencias-reddit/analytics/temporal/',
    'sentiment': 's3a://tendencias-reddit/analytics/sentiment/',
    'engagement': 's3a://tendencias-reddit/analytics/engagement/',
    'text': 's3a://tendencias-reddit/analytics/text/',
    'reports': 's3a://tendencias-reddit/reports/'
}
    """
    Orquesta la lectura de los resultados de análisis de MinIO y los carga a la BD.
    
    :param rutas_spark: Diccionario devuelto por el flow de Spark con las rutas base de S3.
    """
    logger = get_run_logger()
    logger.info("=== INICIANDO CARGA DE RESULTADOS DE ANALÍTICA A POSTGRES ===")
    
    # Mapeo: (Clave del Diccionario Spark, Subcarpeta en S3, Nombre de la Tabla Postgres)
    # Usamos 'replace' para las tablas de resumen (e.g., tendencias) y 'append' para logs (e.g., anomalías)
    mapa_carga = [
        ("temporal", "tendencias_diarias", "tendencias_diarias", 'replace'),
        ("temporal", "patrones_horarios", "patrones_horarios", 'replace'),
        ("temporal", "anomalias", "anomalias", 'append'), # Queremos acumular anomalías
        ("sentiment", "distribucion", "distribucion_sentiment_score", 'replace'),
        ("sentiment", "extremos", "comentarios_extremos", 'replace'),
        # Nota: La comparativa de subreddits va directamente a 'reports'
        ("reports", "subreddit_comparativa", "comparativa_subreddits", 'replace'), 
    ]

    for categoria, subcarpeta, tabla, if_exists in mapa_carga:
        logger.info(f"\n→ Procesando tabla '{tabla}'. Leyendo con Spark...")
        
        try:
            # 🟢 CAMBIO CLAVE: Usamos la función que invoca a Spark para leer el S3A
            # Spark maneja la complejidad de S3A y la estructura de carpetas.
            df = leer_resultado_spark_a_pandas(rutas_spark, categoria, subcarpeta)
            print(df.head())
            # Una vez que tenemos el df en Pandas, usamos la función de guardado existente
            limpiar_y_cargar(df, tabla, if_exists)
            
        except Exception as e:
            # La excepción podría ser FileNotFoundError si Spark no encuentra el path S3A.
            logger.error(f"   [FALLO CRÍTICO] Error al leer con Spark o cargar la tabla {tabla}: {e}")
            pass