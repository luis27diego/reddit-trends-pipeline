from src.infrastructure.spark.schemas import REDDIT_SCHEMA

def leer_csv_optimizado(spark, path):
    """
    Lee el CSV usando un schema predefinido para evitar escanear los 12GB 
    antes de empezar (evita inferSchema).
    """
    return (spark.read
            .schema(REDDIT_SCHEMA)
            .option("header", "true")
            .option("multiLine", "true")
            .option("escape", "\"")
            .csv(path))

def leer_csv_resultado(spark, path):
    """
    Lee archivos de resultado de Spark (que son más pequeños) 
    y permite inferir el esquema o usar el encabezado.
    """
    return (spark.read
            .option("header", "true") 
            .option("multiLine", "true")
            .option("escape", "\"")
            .csv(path))

def guardar_resultado(df, path_salida, formato="parquet", coalesce_a_uno=False):
    df_salida = df
    
    if coalesce_a_uno:
        df_salida = df.coalesce(1)
    
    df_salida.write.option("header", "true").mode("overwrite").csv(path_salida)
