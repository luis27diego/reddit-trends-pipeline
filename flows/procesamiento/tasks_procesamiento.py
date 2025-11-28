from prefect import task
from src.services.spark_service import create_spark_session
from src.services.io_service import leer_csv_optimizado, guardar_resultado
from src.services.analytics_service import (
    analizar_tendencia_temporal, 
    extraer_temas_lda, 
    validar_sentimiento
)

@task(log_prints=True)
def procesar_archivo_grande(minio_key: str):
    spark = create_spark_session()
    
    # 1. Lectura
    path_entrada = f"s3a://tendencias-reddit/{minio_key}"
    print(f"--- Iniciando lectura optimizada de {path_entrada} ---")
    df = leer_csv_optimizado(spark, path_entrada)
    
    # Persistimos en memoria si cabe, o en disco, porque usaremos el DF 3 veces
    df.persist() 
    print(f"Count inicial (rápido si schema es correcto): {df.count()}")

    base_output = f"s3a://tendencias-reddit/processed/{minio_key.split('/')[-1]}"

    # 2. Ejecutar Análisis 1: Tendencias
    print("--- Ejecutando Análisis de Tendencias ---")
    df_trends = analizar_tendencia_temporal(df)
    guardar_resultado(df_trends, f"{base_output}/trends", formato="csv") # CSV es ok para resultados pequeños

    # 3. Ejecutar Análisis 2: LDA
    print("--- Ejecutando Topic Modeling ---")
    df_topics = extraer_temas_lda(df)
    guardar_resultado(df_topics, f"{base_output}/topics", formato="json")

    # 4. Ejecutar Análisis 3: Validación
    print("--- Ejecutando Validación de Modelo ---")
    df_val = validar_sentimiento(df)
    guardar_resultado(df_val, f"{base_output}/validation", formato="csv")

    df.unpersist()
    print("--- Procesamiento completado ---")
    
    return base_output