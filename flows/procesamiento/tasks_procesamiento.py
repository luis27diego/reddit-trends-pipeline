# flows/procesamiento/tasks_procesamiento.py  (reemplaza TODO el archivo)
from prefect import task
from pyparsing import col
from src.services.spark_service import create_spark_session
from src.services.text_processing_service import procesar_texto_distribuido
from src.config.minio_config import PROCESSED_FOLDER
import os

@task(cache_policy=None, log_prints=True)
def procesar_archivo_grande(minio_key: str):
    """
    Procesa un archivo GIGANTE directamente desde MinIO usando Spark.
    Nunca carga todo en memoria.
    """
    spark = create_spark_session()
    
    print(f"Leyendo archivo grande desde MinIO: {minio_key}")
    
    path = f"s3a://tendencias-reddit/{minio_key}"
    
    print(f"Leyendo archivo grande desde MinIO: {minio_key}")
    
    # LEER COMO CSV CORRECTAMENTE
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiLine", "true") \
        .option("escape", "\"") \
        .csv(path)
    
    print(f"Total filas leídas: {df_raw.count():,}")
    
    # Filtrar solo selftext no nulo y no vacío
    df_text = df_raw.select("selftext") \
        .na.drop() \
        .filter(col("selftext") != "") \
        .filter(col("selftext") != "[deleted]") \
        .filter(col("selftext") != "[removed]")
    
    print(f"Filas con texto válido: {df_text.count():,}")
    
    # Procesamiento 100% distribuido
    df_counts = procesar_texto_distribuido(df_text)
    
    # Nombre de salida
    filename = os.path.basename(minio_key)
    output_folder = f"{PROCESSED_FOLDER}/{filename}_wordcount"
    
    print(f"Guardando resultados particionados en: {output_folder}")
    
    (df_counts
     .coalesce(12)  # 12 archivos de salida (fácil de leer después)
     .write
     .mode("overwrite")
     .option("header", "true")
     .csv(f"s3a://tendencias-reddit/{output_folder}"))
    
    print(f"Procesamiento completado: {output_folder}")
    return output_folder