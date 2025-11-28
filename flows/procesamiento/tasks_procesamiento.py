# flows/procesamiento/tasks_procesamiento.py  (reemplaza TODO el archivo)
from prefect import task
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
    
    # Leemos directamente desde MinIO como texto particionado
    df_raw = spark.read.text(f"s3a://tendencias-reddit/{minio_key}")
    df_raw = df_raw.select("selftext").na.drop()  # Elimina filas con valores nulos
    
    # Unir todo el texto de la columna 'selftext' en un solo string
    combined_text = df_raw.rdd.map(lambda row: row['selftext']).collect()
    combined_text = "\n".join(combined_text)
    
    # Convertir el texto a bytes
    df_raw = combined_text.encode('utf-8')
    
    print(f"Archivo leído. Filas aproximadas: {df_raw.count():,}")
    
    # Procesamiento 100% distribuido
    df_counts = procesar_texto_distribuido(df_raw)
    
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