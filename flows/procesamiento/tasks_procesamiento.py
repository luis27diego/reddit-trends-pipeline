from prefect import task
from src.services.spark_service import create_spark_session
from src.services.io_service import leer_csv_optimizado, guardar_resultado
from src.services.analytics_service import (
    analizar_tendencia_temporal,
    analizar_patrones_temporales,
    detectar_picos_actividad,
    palabras_por_sentimiento,
    analizar_controversia_por_subreddit,
    analizar_sentiment_vs_score,
    comparar_subreddits,
    encontrar_comentarios_extremos
)

@task(log_prints=True)
def procesar_archivo_grande(minio_key: str):
    spark = create_spark_session()
    
    # 1. Lectura
    path_entrada = f"s3a://tendencias-reddit/raw/{minio_key}"
    print(f"--- Iniciando lectura de {path_entrada} ---")
    df = leer_csv_optimizado(spark, path_entrada)
    df.persist()
    
    # Base para todas las salidas
    base_output = "s3a://tendencias-reddit/analytics"
    nombre_archivo = minio_key.split('/')[-1].replace('.csv', '')
    
    # ======================
    # ANÁLISIS TEMPORALES
    # ======================
    print("\n=== ANÁLISIS TEMPORALES ===")
    
    # Tendencias diarias
    print("→ Tendencias diarias...")
    df_trends = analizar_tendencia_temporal(df)
    guardar_resultado(
        df_trends, 
        f"{base_output}/temporal/tendencias_diarias/{nombre_archivo}",
        formato="csv"
    )
    
    # Patrones horarios
    print("→ Patrones horarios...")
    df_patrones = analizar_patrones_temporales(df)
    guardar_resultado(
        df_patrones,
        f"{base_output}/temporal/patrones_horarios/{nombre_archivo}",
        formato="csv"
    )
    
    # Detección de anomalías
    print("→ Detección de anomalías...")
    df_anomalias = detectar_picos_actividad(df)
    guardar_resultado(
        df_anomalias,
        f"{base_output}/temporal/anomalias/{nombre_archivo}",
        formato="csv"
    )
    
    # ======================
    # ANÁLISIS DE SENTIMIENTO
    # ======================
    print("\n=== ANÁLISIS DE SENTIMIENTO ===")
    
    # Distribución sentiment vs score
    print("→ Distribución sentiment vs score...")
    df_distribucion = analizar_sentiment_vs_score(df)
    guardar_resultado(
        df_distribucion,
        f"{base_output}/sentiment/distribucion/{nombre_archivo}",
        formato="csv"
    )
    
    # Comentarios extremos
    print("→ Comentarios extremos...")
    df_extremos = encontrar_comentarios_extremos(df, limite=500)
    guardar_resultado(
        df_extremos,
        f"{base_output}/sentiment/extremos/{nombre_archivo}",
        formato="csv"
    )
    
    # ======================
    # ANÁLISIS DE ENGAGEMENT
    # ======================
    print("\n=== ANÁLISIS DE ENGAGEMENT ===")
    
    # Controversia por subreddit
    print("→ Controversia por subreddit...")
    df_controversia = analizar_controversia_por_subreddit(df)
    guardar_resultado(
        df_controversia,
        f"{base_output}/engagement/controversia/{nombre_archivo}",
        formato="csv"
    )
    
    # ======================
    # ANÁLISIS DE TEXTO
    # ======================
    print("\n=== ANÁLISIS DE TEXTO ===")
    
    # Palabras clave por sentimiento
    print("→ Palabras clave por sentimiento...")
    df_palabras = palabras_por_sentimiento(df, top_n=50)
    guardar_resultado(
        df_palabras,
        f"{base_output}/text/palabras_clave/{nombre_archivo}",
        formato="csv"
    )
    
    # ======================
    # REPORTES CONSOLIDADOS
    # ======================
    print("\n=== REPORTES CONSOLIDADOS ===")
    
    # Comparativa de subreddits
    print("→ Comparativa de subreddits...")
    df_comparativa = comparar_subreddits(df)
    guardar_resultado(
        df_comparativa,
        f"s3a://tendencias-reddit/reports/subreddit_comparativa/{nombre_archivo}",
        formato="csv",
        coalesce_a_uno=True
    )
    
    df.unpersist()
    print("Procesamiento completado")
    
    return {
        "temporal": f"{base_output}/temporal/",
        "sentiment": f"{base_output}/sentiment/",
        "engagement": f"{base_output}/engagement/",
        "text": f"{base_output}/text/",
        "reports": "s3a://tendencias-reddit/reports/"
    }