# src/services/loader_service.py

import pandas as pd
import s3fs
from sqlalchemy import create_engine
import os
from prefect import get_run_logger

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

# Conexión a MinIO (usando credenciales internas del docker-compose)
storage_options = {
    "key": os.getenv("MINIO_ROOT_USER", "minioadmin"),
    "secret": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    "client_kwargs": {
        "endpoint_url": "http://minio:9000" # URL interna de la red Docker
    }
}

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
        # Reemplazamos s3a:// por s3:// (pandas/s3fs solo soporta s3://)
        base_path = rutas_spark[categoria].replace("s3a://", "s3://")
        
        # Leemos todos los CSVs dentro de la carpeta particionada de Spark (*)
        full_path = f"{base_path}{subcarpeta}/{SPARK_NESTED_FOLDER}/*.csv"        
        logger.info(f"\n→ Procesando tabla '{tabla}'. Leyendo de: {full_path}")
        
        try:
            df = pd.read_csv(full_path, storage_options=storage_options)
            print(df)
            limpiar_y_cargar(df, tabla, if_exists)
            
        except FileNotFoundError:
            logger.warning(f"   [SKIP] Archivos no encontrados en {full_path}. El análisis de Spark pudo haber omitido generarlos.")
        except Exception as e:
            logger.error(f"   [FALLO CRÍTICO] Error general al cargar la tabla {tabla}: {e}")
            # Puedes usar 'raise' aquí si quieres que el flow falle, o 'pass' si prefieres la robustez.
            pass